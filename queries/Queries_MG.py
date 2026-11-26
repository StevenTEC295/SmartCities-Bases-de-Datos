import inspect
import json

from pyspark.sql import DataFrame, SparkSession, functions as F


class Queries_MG:
    """
    Consultas a MongoDB SOLO con Aggregation Framework.
    Cada consulta es un pipeline (lista de etapas) que se ejecuta dentro de Mongo
    mediante mongo-spark-connector 10.x, vía Spark Connect.
    Todos los métodos devuelven DataFrames de Spark.
    """

    # El dashboard es de solo lectura: se rechazan las etapas que escriben datos
    ETAPAS_PROHIBIDAS = {"$out", "$merge"}

    def __init__(self, spark: SparkSession, uri: str, base_datos: str):
        self.spark = spark
        self.uri = uri                # ej: mongodb://mongo:27017  (nombre del servicio, puerto interno)
        self.base_datos = base_datos

    # ---------- Núcleo ----------
    @classmethod
    def _validar(cls, pipeline: list) -> None:
        if not isinstance(pipeline, list) or not pipeline:
            raise ValueError("El pipeline debe ser una lista no vacía de etapas ($match, $group, ...)")
        for etapa in pipeline:
            if not isinstance(etapa, dict) or len(etapa) != 1:
                raise ValueError(f"Cada etapa debe ser un dict con un solo operador: {etapa}")
            operador = next(iter(etapa))
            if not operador.startswith("$"):
                raise ValueError(f"Operador de etapa inválido: {operador}")
            if operador in cls.ETAPAS_PROHIBIDAS:
                raise ValueError(f"La etapa {operador} no está permitida (solo lectura)")

    def _agregar(self, coleccion: str, pipeline: list) -> DataFrame:
        self._validar(pipeline)
        df = (
            self.spark.read.format("mongodb")
            .option("connection.uri", self.uri)
            .option("database", self.base_datos)
            .option("collection", coleccion)
            .option("aggregation.pipeline", json.dumps(pipeline))
            .load()
        )
        # _id llega como struct (ObjectId) y no se grafica bien: se descarta si quedó
        return df.drop("_id") if "_id" in df.columns else df

    def agregar(self, coleccion: str, pipeline: list) -> DataFrame:
        """Ejecuta un pipeline de agregación y devuelve el resultado como DataFrame de Spark."""
        return self._agregar(coleccion, pipeline)

    # ---------- Consultas de ejemplo (cambia colección y campos por los de tu base) ----------
    def conteo_por_campo(self, coleccion: str, campo: str) -> DataFrame:
        return self.agregar(coleccion, [
            {"$group": {"_id": f"${campo}", "total": {"$sum": 1}}},
            {"$project": {"_id": 0, campo: "$_id", "total": 1}},
            {"$sort": {"total": -1}},
        ])

    def top_n(self, coleccion: str, campo_grupo: str, campo_valor: str, n: int = 10) -> DataFrame:
        return self.agregar(coleccion, [
            {"$group": {"_id": f"${campo_grupo}", "total": {"$sum": f"${campo_valor}"}}},
            {"$sort": {"total": -1}},
            {"$limit": int(n)},
            {"$project": {"_id": 0, campo_grupo: "$_id", "total": 1}},
        ])

    def serie_diaria(self, coleccion: str, campo_fecha: str, campo_valor: str) -> DataFrame:
        # Requiere que campo_fecha sea tipo Date en Mongo (si es texto, usa {"$toDate": ...})
        return self.agregar(coleccion, [
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${campo_fecha}"}},
                "promedio": {"$avg": f"${campo_valor}"},
                "maximo": {"$max": f"${campo_valor}"},
            }},
            {"$project": {"_id": 0, "fecha": "$_id", "promedio": 1, "maximo": 1}},
            {"$sort": {"fecha": 1}},
        ])

    # =====================================================================
    # Consultas obligatorias de MongoDB (Aggregation Framework)
    # =====================================================================
    # Mongo guarda solo el id del sensor. El filtrado y la agregación se hacen en Mongo; el
    # resultado (ya agregado y pequeño) se une en Spark con `sensores`, el DataFrame de
    # Queries_PG.sensores_metadata() (fuente, sensor_id, nombre, zona, zona_simulada, parametro).
    # OJO: el conector parte las colecciones grandes en particiones y ejecuta el pipeline en cada una.
    # Por eso Mongo calcula resultados PARCIALES (sumas, conteos, mínimos, máximos) y Spark los
    # combina; así el resultado no depende del tamaño de la colección.

    # variable -> (colección, campo del sensor, campo de fecha, campo del valor)
    VARIABLES = {
        "temperature": ("clima_measurements", "dev_id", "time", "temperature"),
        "humidity": ("clima_measurements", "dev_id", "time", "humidity"),
        "battery": ("clima_measurements", "dev_id", "time", "battery"),
        "pm25": ("mediciones", "sensor_id", "datetime_utc", "value"),
        "pm10": ("mediciones", "sensor_id", "datetime_utc", "value"),
        "o3": ("mediciones", "sensor_id", "datetime_utc", "value"),
    }
    UNIDADES = {
        "temperature": "C", "humidity": "%", "battery": "V",
        "pm25": "µg/m³", "pm10": "µg/m³", "o3": "ppm",
    }
    COLECCION_ACCIDENTES_CLIMA = "weather"

    @classmethod
    def _config(cls, variable: str) -> tuple:
        if variable not in cls.VARIABLES:
            raise ValueError(f"Variable desconocida: {variable}. Use {sorted(cls.VARIABLES)}")
        return cls.VARIABLES[variable]

    @staticmethod
    def _iso(fecha: str, fin: bool = False) -> dict:
        """Fecha 'AAAA-MM-DD' como Extended JSON de MongoDB (inicio o final del día, UTC)."""
        return {"$date": f"{fecha}T{'23:59:59' if fin else '00:00:00'}Z"}

    def _ids(self, variable: str, sensores: DataFrame | None, sensor_id=None):
        """Ids de sensor a consultar: uno solo, todos los de la variable, o None (todos los dispositivos de clima)."""
        _, campo_sensor, _, _ = self._config(variable)
        if sensor_id is not None:
            return [int(sensor_id)] if campo_sensor == "sensor_id" else [str(sensor_id)]
        if campo_sensor == "dev_id":
            return None
        if sensores is None:
            raise ValueError("Falta el DataFrame de sensores (Queries_PG.sensores_metadata())")
        filas = (sensores.filter((F.col("fuente") == "contaminacion") & (F.col("parametro") == variable))
                 .select("sensor_id").collect())
        return [int(f["sensor_id"]) for f in filas]

    def _match(self, variable: str, ids=None, fecha_ini=None, fecha_fin=None, vmin=None, vmax=None) -> list:
        """Etapa $match con filtros opcionales de sensor, fechas (UTC) y rango de valor."""
        _, campo_sensor, campo_fecha, campo_valor = self._config(variable)
        filtro = {}
        if ids is not None:
            filtro[campo_sensor] = {"$in": ids}
        if fecha_ini or fecha_fin:
            rango = {}
            if fecha_ini:
                rango["$gte"] = self._iso(fecha_ini)
            if fecha_fin:
                rango["$lte"] = self._iso(fecha_fin, True)
            filtro[campo_fecha] = rango
        if vmin is not None or vmax is not None:
            rango = {}
            if vmin is not None:
                rango["$gte"] = float(vmin)
            if vmax is not None:
                rango["$lte"] = float(vmax)
            filtro[campo_valor] = rango
        return [{"$match": filtro}] if filtro else []

    @staticmethod
    def _con_metadata(df: DataFrame, sensores: DataFrame | None) -> DataFrame:
        """Une el resultado (columna `sensor`) con nombre, zona y parámetro, y crea `etiqueta`."""
        if sensores is None or "sensor" not in df.columns:
            return df
        meta = sensores.select(
            F.col("sensor_id").alias("_sid"), F.col("fuente"), F.col("nombre"),
            F.col("zona"), F.col("parametro"),
        )
        unido = df.join(meta, F.col("sensor").cast("string") == F.col("_sid"), "left").drop("_sid")
        return unido.withColumn(
            "etiqueta",
            F.concat_ws(" - ", F.col("nombre"), F.coalesce(F.col("parametro"), F.col("sensor").cast("string"))),
        )

    # ---- 1. Lecturas de un sensor específico ----
    def lecturas_sensor(self, variable: str, sensor_id, n: int = 20, sensores: DataFrame | None = None) -> DataFrame:
        """Las últimas n lecturas de un sensor."""
        coleccion, c_sensor, c_fecha, c_valor = self._config(variable)
        pipeline = self._match(variable, self._ids(variable, sensores, sensor_id)) + [
            {"$sort": {c_fecha: -1}},
            {"$limit": int(n)},
            {"$project": {"_id": 0, "sensor": f"${c_sensor}", "fecha": f"${c_fecha}", "valor": f"${c_valor}"}},
        ]
        df = self.agregar(coleccion, pipeline)
        return df.orderBy(F.desc("fecha")).limit(int(n)) if "fecha" in df.columns else df

    # ---- 2. Lecturas en un rango de fechas (y filtros múltiples: sensor, fechas y rango de valor) ----
    def lecturas_rango(self, variable: str, fecha_ini: str | None = None, fecha_fin: str | None = None,
                       sensor_id=None, valor_min=None, valor_max=None, n: int = 100,
                       sensores: DataFrame | None = None) -> DataFrame:
        coleccion, c_sensor, c_fecha, c_valor = self._config(variable)
        ids = self._ids(variable, sensores, sensor_id)
        pipeline = self._match(variable, ids, fecha_ini, fecha_fin, valor_min, valor_max) + [
            {"$sort": {c_fecha: 1}},
            {"$limit": int(n)},
            {"$project": {"_id": 0, "sensor": f"${c_sensor}", "fecha": f"${c_fecha}", "valor": f"${c_valor}"}},
        ]
        df = self.agregar(coleccion, pipeline)
        return df.orderBy(F.asc("fecha")).limit(int(n)) if "fecha" in df.columns else df

    # ---- 3. Valores máximos y mínimos (de un sensor, de cada sensor o de todos juntos) ----
    def extremos(self, variable: str, sensor_id=None, por_sensor: bool = False,
                 fecha_ini: str | None = None, fecha_fin: str | None = None,
                 sensores: DataFrame | None = None) -> DataFrame:
        coleccion, c_sensor, _, c_valor = self._config(variable)
        agrupar_por_sensor = por_sensor or sensor_id is not None
        ids = self._ids(variable, sensores, sensor_id)
        pipeline = self._match(variable, ids, fecha_ini, fecha_fin) + [
            {"$group": {
                "_id": f"${c_sensor}" if agrupar_por_sensor else None,
                "minimo": {"$min": f"${c_valor}"},
                "maximo": {"$max": f"${c_valor}"},
                "mediciones": {"$sum": 1},
            }},
            {"$project": {"_id": 0, "sensor": {"$ifNull": ["$_id", "todos"]},
                          "minimo": 1, "maximo": 1, "mediciones": 1}},
        ]
        resultado = self.agregar(coleccion, pipeline)
        if "sensor" not in resultado.columns:
            return resultado
        resultado = (resultado.groupBy("sensor")
                     .agg(F.min("minimo").alias("minimo"), F.max("maximo").alias("maximo"),
                          F.sum("mediciones").alias("mediciones"))
                     .orderBy("sensor"))
        if agrupar_por_sensor:
            resultado = self._con_metadata(resultado, sensores)
        return resultado

    # ---- 4. Sensores que superaron el límite (el límite lo define el usuario) ----
    def sensores_sobre_limite(self, variable: str, limite: float, superior: bool = True,
                              fecha_ini: str | None = None, fecha_fin: str | None = None,
                              sensores: DataFrame | None = None) -> DataFrame:
        """Por sensor: cuántas lecturas superan (o quedan bajo) el límite y el valor extremo."""
        coleccion, c_sensor, _, c_valor = self._config(variable)
        ids = self._ids(variable, sensores)
        filtro = self._match(variable, ids, fecha_ini, fecha_fin,
                             vmin=limite if superior else None, vmax=None if superior else limite)
        pipeline = filtro + [
            {"$group": {"_id": f"${c_sensor}", "eventos": {"$sum": 1},
                        "extremo": {"$max" if superior else "$min": f"${c_valor}"}}},
            {"$project": {"_id": 0, "sensor": "$_id", "eventos": 1, "extremo": 1}},
        ]
        parcial = self.agregar(coleccion, pipeline)
        if "sensor" in parcial.columns:
            parcial = (parcial.groupBy("sensor")
                       .agg(F.sum("eventos").alias("eventos"),
                            (F.max if superior else F.min)("extremo").alias("extremo"))
                       .orderBy(F.desc("eventos")))
        resultado = self._con_metadata(parcial, sensores)
        if "etiqueta" not in resultado.columns:
            return resultado
        return (resultado.select("etiqueta", "sensor", "nombre", "zona", "eventos", "extremo")
                .orderBy(F.desc("eventos")))

    # ---- 5. Alertas por zona (opción A: se calculan al vuelo con el límite; no se guarda nada) ----
    def alertas_por_zona(self, variable: str, limite: float, superior: bool = True,
                         fecha_ini: str | None = None, fecha_fin: str | None = None,
                         sensores: DataFrame | None = None) -> DataFrame:
        """Alertas que se emitirían con el límite dado, agrupadas por zona."""
        por_sensor = self.sensores_sobre_limite(variable, limite, superior, fecha_ini, fecha_fin, sensores)
        if "zona" not in por_sensor.columns:
            return por_sensor
        return (por_sensor.groupBy("zona")
                .agg(F.sum("eventos").alias("alertas"),
                     F.countDistinct("sensor").alias("sensores_en_alerta"))
                .orderBy(F.desc("alertas")))

    # ---- 6. Promedio de mediciones por zona (o por ubicación) ----
    def promedio_por_zona(self, variable: str, por: str = "zona", fecha_ini: str | None = None,
                          fecha_fin: str | None = None, sensores: DataFrame | None = None) -> DataFrame:
        """Promedio de una variable por zona. por='ubicacion' agrupa por nombre del sensor/estación."""
        if por not in ("zona", "ubicacion"):
            raise ValueError("`por` debe ser 'zona' o 'ubicacion'")
        coleccion, c_sensor, _, c_valor = self._config(variable)
        pipeline = self._match(variable, self._ids(variable, sensores), fecha_ini, fecha_fin) + [
            {"$group": {"_id": f"${c_sensor}", "suma": {"$sum": f"${c_valor}"}, "mediciones": {"$sum": 1}}},
            {"$project": {"_id": 0, "sensor": "$_id", "suma": 1, "mediciones": 1}},
        ]
        resultado = self._con_metadata(self.agregar(coleccion, pipeline), sensores)
        if "nombre" not in resultado.columns:
            return resultado
        columna = "zona" if por == "zona" else "nombre"
        return (resultado.groupBy(F.col(columna).alias("grupo"))
                .agg((F.sum("suma") / F.sum("mediciones")).alias("promedio"),
                     F.sum("mediciones").alias("mediciones"))
                .orderBy(F.desc("promedio")))

    # ---- 7. Promedio por tipo de sensor ----
    def promedio_por_tipo_sensor(self, fecha_ini: str | None = None, fecha_fin: str | None = None,
                                 sensores: DataFrame | None = None) -> DataFrame:
        """Promedio de cada tipo de sensor (pm25, pm10, o3, temperature, humidity, battery)."""
        partes = []
        for variable in self.VARIABLES:
            coleccion, _, _, c_valor = self._config(variable)
            pipeline = self._match(variable, self._ids(variable, sensores), fecha_ini, fecha_fin) + [
                {"$group": {"_id": None, "suma": {"$sum": f"${c_valor}"}, "mediciones": {"$sum": 1}}},
                {"$project": {"_id": 0, "suma": 1, "mediciones": 1}},
            ]
            df = self.agregar(coleccion, pipeline)
            if "suma" in df.columns:
                total = df.agg(F.sum("suma").alias("suma"), F.sum("mediciones").alias("mediciones"))
                partes.append(total.select(F.lit(variable).alias("tipo"),
                                           F.lit(self.UNIDADES[variable]).alias("unidad"),
                                           (F.col("suma") / F.col("mediciones")).cast("double").alias("promedio"),
                                           F.col("mediciones").cast("long").alias("mediciones")))
        if not partes:
            return self.spark.range(0)
        resultado = partes[0]
        for parte in partes[1:]:
            resultado = resultado.unionByName(parte)
        return resultado.orderBy("tipo")

    # ---- 8. Mediciones por sensor ----
    def mediciones_por_sensor(self, fecha_ini: str | None = None, fecha_fin: str | None = None,
                              sensores: DataFrame | None = None) -> DataFrame:
        """Cantidad de mediciones de cada sensor (contaminación y clima)."""
        partes = []
        for variable in ("pm25", "temperature"):   # una variable por colección
            coleccion, c_sensor, _, _ = self._config(variable)
            pipeline = self._match(variable, None, fecha_ini, fecha_fin) + [
                {"$group": {"_id": f"${c_sensor}", "mediciones": {"$sum": 1}}},
                {"$project": {"_id": 0, "sensor": "$_id", "mediciones": 1}},
            ]
            df = self.agregar(coleccion, pipeline)
            if "sensor" in df.columns:
                partes.append(df.select(F.col("sensor").cast("string").alias("sensor"),
                                        F.col("mediciones").cast("long").alias("mediciones")))
        partes = [p.groupBy("sensor").agg(F.sum("mediciones").alias("mediciones")) for p in partes]
        if not partes:
            return self.spark.range(0)
        resultado = partes[0]
        for parte in partes[1:]:
            resultado = resultado.unionByName(parte)
        resultado = self._con_metadata(resultado, sensores)
        columnas = [c for c in ("etiqueta", "sensor", "fuente", "nombre", "zona", "mediciones") if c in resultado.columns]
        return resultado.select(*columnas).orderBy(F.desc("mediciones"))

    # ---- 9. Incidentes (accidentes) por categoría: condición climática en el momento del accidente ----
    def accidentes_por_condicion(self, fecha_ini: str | None = None, fecha_fin: str | None = None,
                                 n: int = 15) -> DataFrame:
        filtro = {}
        if fecha_ini or fecha_fin:
            rango = {}
            if fecha_ini:
                rango["$gte"] = self._iso(fecha_ini)
            if fecha_fin:
                rango["$lte"] = self._iso(fecha_fin, True)
            filtro["weather_timestamp_utc"] = rango
        pipeline = ([{"$match": filtro}] if filtro else []) + [
            {"$group": {"_id": "$weather_condition", "accidentes": {"$sum": 1}}},
            {"$project": {"_id": 0, "condicion": {"$ifNull": ["$_id", "(sin dato)"]}, "accidentes": 1}},
        ]
        df = self.agregar(self.COLECCION_ACCIDENTES_CLIMA, pipeline)
        if "condicion" not in df.columns:
            return df
        return (df.groupBy("condicion").agg(F.sum("accidentes").alias("accidentes"))
                .orderBy(F.desc("accidentes")).limit(int(n)))

    # ---- 10. Evolución de una variable durante un periodo ----
    def evolucion_variable(self, variable: str, fecha_ini: str | None = None, fecha_fin: str | None = None,
                           sensor_id=None, granularidad: str = "dia",
                           sensores: DataFrame | None = None) -> DataFrame:
        formatos = {"dia": "%Y-%m-%d", "mes": "%Y-%m"}
        if granularidad not in formatos:
            raise ValueError(f"granularidad debe ser una de {sorted(formatos)}")
        coleccion, _, c_fecha, c_valor = self._config(variable)
        ids = self._ids(variable, sensores, sensor_id)
        pipeline = self._match(variable, ids, fecha_ini, fecha_fin) + [
            {"$group": {
                "_id": {"$dateToString": {"format": formatos[granularidad], "date": f"${c_fecha}"}},
                "suma": {"$sum": f"${c_valor}"},
                "minimo": {"$min": f"${c_valor}"},
                "maximo": {"$max": f"${c_valor}"},
                "mediciones": {"$sum": 1},
            }},
            {"$project": {"_id": 0, "fecha": "$_id", "suma": 1, "minimo": 1, "maximo": 1, "mediciones": 1}},
        ]
        df = self.agregar(coleccion, pipeline)
        if "fecha" not in df.columns:
            return df
        return (df.groupBy("fecha")
                .agg((F.sum("suma") / F.sum("mediciones")).alias("promedio"),
                     F.min("minimo").alias("minimo"), F.max("maximo").alias("maximo"),
                     F.sum("mediciones").alias("mediciones"))
                .orderBy("fecha"))

    # Registro de consultas obligatorias: número -> (descripción, método)
    CONSULTAS = {
        1: ("Lecturas de un sensor específico", "lecturas_sensor"),
        2: ("Lecturas en un rango de fechas (filtros múltiples)", "lecturas_rango"),
        3: ("Valores máximos y mínimos registrados", "extremos"),
        4: ("Sensores sobre el límite establecido (eventos críticos)", "sensores_sobre_limite"),
        5: ("Alertas por zona geográfica", "alertas_por_zona"),
        6: ("Promedio de mediciones por zona", "promedio_por_zona"),
        7: ("Promedio por tipo de sensor", "promedio_por_tipo_sensor"),
        8: ("Mediciones por sensor", "mediciones_por_sensor"),
        9: ("Accidentes por condición climática", "accidentes_por_condicion"),
        10: ("Evolución de una variable en un periodo", "evolucion_variable"),
    }

    def ejecutar(self, numero: int, sensores: DataFrame | None = None, **parametros) -> DataFrame:
        """Ejecuta una consulta obligatoria por su número (ver CONSULTAS)."""
        if numero not in self.CONSULTAS:
            raise ValueError(f"Consulta {numero} no disponible. Disponibles: {sorted(self.CONSULTAS)}")
        metodo = getattr(self, self.CONSULTAS[numero][1])
        if "sensores" in inspect.signature(metodo).parameters:
            parametros["sensores"] = sensores
        return metodo(**parametros)
