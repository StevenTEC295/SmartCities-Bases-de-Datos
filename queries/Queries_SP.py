from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


class Queries_SP:
    """
    Consultas con Spark sobre los archivos Parquet de HDFS (/data/processed).
    Todos los métodos devuelven DataFrames de Spark.

    Fuentes (todas en la capa processed, la misma que se carga a PostgreSQL y MongoDB):
      accidentes/accidents  -> accidentes (state, city, severity, start_time_utc, ...)
      contaminacion/{mediciones, sensores, parametros, locations}
      clima/{mediciones, sensores}
    Los métodos de periodo aceptan fecha_ini / fecha_fin ('AAAA-MM-DD', UTC); None = sin filtro.
    """

    # Divisiones del Censo de EE. UU. (misma definición que db/sensores_zonas.sql).
    ZONAS = {
        1: "New England", 2: "Mid-Atlantic", 3: "East North Central", 4: "West North Central",
        5: "South Atlantic", 6: "East South Central", 7: "West South Central", 8: "Mountain", 9: "Pacific",
    }
    ESTADOS_ZONA = {
        **dict.fromkeys(["CT", "ME", "MA", "NH", "RI", "VT"], 1),
        **dict.fromkeys(["NJ", "NY", "PA"], 2),
        **dict.fromkeys(["IL", "IN", "MI", "OH", "WI"], 3),
        **dict.fromkeys(["IA", "KS", "MN", "MO", "NE", "ND", "SD"], 4),
        **dict.fromkeys(["DE", "DC", "FL", "GA", "MD", "NC", "SC", "VA", "WV"], 5),
        **dict.fromkeys(["AL", "KY", "MS", "TN"], 6),
        **dict.fromkeys(["AR", "LA", "OK", "TX"], 7),
        **dict.fromkeys(["AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY"], 8),
        **dict.fromkeys(["AK", "CA", "HI", "OR", "WA"], 9),
    }
    # Estado y coordenadas de las 4 estaciones de contaminación (las mismas de limpieza.ipynb).
    # El Parquet de locations solo trae id y nombre.
    UBICACIONES_CONTAMINACION = [
        (448, "MA", 42.3295, -71.0826),
        (626, "NY", 40.816, -73.902),
        (739, "DC", 38.921847, -77.013178),
        (2139, "MD", 39.310833, -76.474444),
    ]

    def __init__(self, spark: SparkSession, ruta: str, ruta_accidentes: str | None = None):
        self.spark = spark
        self.ruta = ruta.rstrip("/")   # ej: hdfs://namenode:9000/data/processed
        self.ruta_accidentes = (ruta_accidentes or f"{self.ruta}/accidentes/accidents").rstrip("/")

    def probar(self) -> None:
        """Comprueba la conexión: imprime los números 0 al 4 (la salida sale en la consola)."""
        self.spark.range(5).show()

    # ---- Utilidades ----
    def _leer(self, subruta: str) -> DataFrame:
        return self.spark.read.parquet(f"{self.ruta}/{subruta}")

    @staticmethod
    def _periodo(df: DataFrame, columna: str, fecha_ini: str | None, fecha_fin: str | None) -> DataFrame:
        """Filtra por [fecha_ini, fecha_fin] (días completos). None = sin límite."""
        if fecha_ini:
            df = df.filter(F.col(columna) >= F.lit(fecha_ini).cast("timestamp"))
        if fecha_fin:
            df = df.filter(F.col(columna) < F.date_add(F.lit(fecha_fin).cast("date"), 1).cast("timestamp"))
        return df

    def _accidentes(self, fecha_ini=None, fecha_fin=None) -> DataFrame:
        """Accidentes procesados, filtrados por start_time_utc (igual que PostgreSQL)."""
        return self._periodo(self.spark.read.parquet(self.ruta_accidentes), "start_time_utc", fecha_ini, fecha_fin)

    def _mediciones_cont(self, fecha_ini=None, fecha_fin=None) -> DataFrame:
        return self._periodo(self._leer("contaminacion/mediciones"), "datetime_utc", fecha_ini, fecha_fin)

    def _mediciones_clima(self, fecha_ini=None, fecha_fin=None) -> DataFrame:
        return self._periodo(self._leer("clima/mediciones"), "time", fecha_ini, fecha_fin)

    def _ubicaciones_cont(self) -> DataFrame:
        """Estaciones de contaminación: location_id, location_name, estado, lat, lng."""
        coords = self.spark.createDataFrame(
            self.UBICACIONES_CONTAMINACION, "location_id INT, estado STRING, lat DOUBLE, lng DOUBLE"
        )
        return self._leer("contaminacion/locations").join(coords, "location_id", "left")

    def _estado_zona(self) -> DataFrame:
        """Tabla estado -> zona (zona_id, zona)."""
        filas = [(estado, zid, self.ZONAS[zid]) for estado, zid in self.ESTADOS_ZONA.items()]
        return self.spark.createDataFrame(filas, "estado STRING, zona_id INT, zona STRING")

    def sensoresTipo(self) -> DataFrame:
        """
        Cantidad de sensores por tipo de parámetro (pm10, pm25, o3...) con su unidad.
        Usa las tablas Parquet que genera limpieza.ipynb:
        contaminacion/sensores (sensor_id, location_id, parameter_id) y
        contaminacion/parametros (parameter_id, parameter, unit).
        """
        sensores = self._leer("contaminacion/sensores")
        parametros = self._leer("contaminacion/parametros")
        return (
            sensores.join(parametros, "parameter_id")
            .groupBy("parameter", "unit")
            .agg(F.count("sensor_id").alias("sensores"))
            .orderBy(F.desc("sensores"))
        )

    # 1. Sensores que generan mas informacion (clima + contaminacion)
    def sensores_mas_activos(self, n: int = 10, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        clima_sen = self._leer("clima/sensores").select("dev_id", "name")
        clima_activos = (
            self._mediciones_clima(fecha_ini, fecha_fin)
            .filter(F.col("dev_id").isNotNull())
            .groupBy("dev_id")
            .agg(F.count("*").alias("mediciones"))
            .join(clima_sen, on="dev_id", how="left")
            .select(
                F.lit("clima").alias("dataset"),
                F.col("dev_id").cast("string").alias("sensor_id"),
                F.coalesce(F.col("name").cast("string"), F.col("dev_id").cast("string")).alias("nombre"),
                F.col("mediciones"),
            )
        )

        cont_sen = (
            self._leer("contaminacion/sensores")
            .join(self._leer("contaminacion/locations"), "location_id", "left")
            .join(self._leer("contaminacion/parametros"), "parameter_id", "left")
            .select("sensor_id", "location_name", "parameter")
        )
        cont_activos = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .filter(F.col("sensor_id").isNotNull())
            .groupBy("sensor_id")
            .agg(F.count("*").alias("mediciones"))
            .join(cont_sen, on="sensor_id", how="left")
            .select(
                F.lit("contaminacion").alias("dataset"),
                F.col("sensor_id").cast("string").alias("sensor_id"),
                F.coalesce(
                    F.concat(F.col("location_name"), F.lit(" ("), F.col("parameter"), F.lit(")")),
                    F.col("sensor_id").cast("string"),
                ).alias("nombre"),
                F.col("mediciones"),
            )
        )

        return (
            clima_activos.unionByName(cont_activos)
            .orderBy(F.desc("mediciones"), F.asc("dataset"), F.asc("sensor_id"))
            .limit(int(n))
        )

    # 2. Distribucion geografica de eventos (accidentes + contaminacion)
    def distribucion_geografica_eventos(self, top_n: int = 15, fecha_ini: str | None = None,
                                        fecha_fin: str | None = None) -> DataFrame:
        """Eventos = accidentes por ciudad y mediciones de contaminación por estación."""
        acc_geo = (
            self._accidentes(fecha_ini, fecha_fin)
            .filter(F.col("state").isNotNull() & F.col("city").isNotNull())
            .groupBy("state", "city")
            .agg(
                F.count("*").alias("total_registros"),
                F.round(F.avg("start_lat"), 4).alias("lat_promedio"),
                F.round(F.avg("start_lng"), 4).alias("lng_promedio"),
            )
            .withColumn("dataset", F.lit("accidentes"))
            .withColumn("ubicacion", F.concat_ws(" - ", F.col("state"), F.col("city")))
            .select("dataset", "ubicacion", "total_registros", "lat_promedio", "lng_promedio")
            .orderBy(F.desc("total_registros"))
            .limit(int(top_n))
        )

        cont_geo = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .join(self._leer("contaminacion/sensores").select("sensor_id", "location_id"), "sensor_id")
            .groupBy("location_id")
            .agg(F.count("*").alias("total_registros"))
            .join(self._ubicaciones_cont(), "location_id", "left")
            .withColumn("dataset", F.lit("contaminacion"))
            .withColumn("ubicacion", F.concat_ws(" - ", F.col("estado"), F.col("location_name")))
            .withColumn("lat_promedio", F.col("lat"))
            .withColumn("lng_promedio", F.col("lng"))
            .select("dataset", "ubicacion", "total_registros", "lat_promedio", "lng_promedio")
            .orderBy(F.desc("total_registros"))
            .limit(int(top_n))
        )

        return acc_geo.unionByName(cont_geo)

    # 3. Evolucion temporal de indicadores urbanos (clima + contaminacion + accidentes)
    def evolucion_temporal_indicadores(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        clima_agg = (
            self._mediciones_clima(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("time"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.count("*").alias("total_registros"),
                F.round(F.avg(F.col("temperature").cast("double")), 2).alias("valor_promedio"),
            )
            .withColumn("dataset", F.lit("clima"))
            .withColumn("indicador", F.lit("temperatura_promedio"))
        )

        cont_agg = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("datetime_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.count("*").alias("total_registros"),
                F.round(F.avg(F.col("value").cast("double")), 2).alias("valor_promedio"),
            )
            .withColumn("dataset", F.lit("contaminacion"))
            .withColumn("indicador", F.lit("contaminacion_promedio"))
        )

        acc_agg = (
            self._accidentes(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("start_time_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.count("*").alias("total_registros"),
                F.round(F.avg(F.col("severity").cast("double")), 2).alias("valor_promedio"),
            )
            .withColumn("dataset", F.lit("accidentes"))
            .withColumn("indicador", F.lit("severidad_promedio"))
        )

        return (
            clima_agg.unionByName(cont_agg)
            .unionByName(acc_agg)
            .select("periodo", "dataset", "indicador", "total_registros", "valor_promedio")
            .orderBy(F.asc("periodo"), F.asc("dataset"))
        )

    # 4. Correlacion entre variables (accidentes + contaminacion por periodo)
    def correlacion_variables(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        """Series mensuales de accidentes y contaminación en los meses en común, más el coeficiente
        de correlación de Pearson GLOBAL (una sola cifra repetida en cada fila)."""
        acc_mes = (
            self._accidentes(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("start_time_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.count("*").cast("double").alias("total_accidentes"),
                F.round(F.avg(F.col("severity").cast("double")), 4).alias("severidad_prom_acc"),
            )
        )

        cont_mes = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("datetime_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.round(F.avg(F.col("value").cast("double")), 4).alias("contaminacion_prom"),
                F.count("*").cast("double").alias("total_mediciones_cont"),
            )
        )

        serie = acc_mes.join(cont_mes, on="periodo", how="inner")
        globales = serie.agg(
            F.round(F.corr("total_accidentes", "contaminacion_prom"), 4).alias("corr_acc_vs_cont"),
            F.round(F.corr("severidad_prom_acc", "contaminacion_prom"), 4).alias("corr_sev_vs_cont"),
        )
        return (
            serie.crossJoin(globales)
            .select(
                "periodo",
                "total_accidentes",
                "severidad_prom_acc",
                "contaminacion_prom",
                "total_mediciones_cont",
                "corr_acc_vs_cont",
                "corr_sev_vs_cont",
            )
            .orderBy(F.asc("periodo"))
        )

    # 5. Ranking de zonas con mayor actividad (accidentes + contaminacion)
    def ranking_zonas_actividad(self, top_n: int = 15, fecha_ini: str | None = None,
                                fecha_fin: str | None = None) -> DataFrame:
        """Ranking por zona del Censo (las mismas 9 zonas de PostgreSQL). Actividad = accidentes
        y mediciones de contaminación. Clima no entra: sus zonas son simuladas (Australia)."""
        estado_zona = self._estado_zona()

        acc_zonas = (
            self._accidentes(fecha_ini, fecha_fin)
            .join(estado_zona, F.col("state") == estado_zona["estado"])
            .groupBy("zona")
            .agg(F.count("*").alias("total_actividad"))
            .withColumn("dataset", F.lit("accidentes"))
        )

        sensor_zona = (
            self._leer("contaminacion/sensores").select("sensor_id", "location_id")
            .join(self._ubicaciones_cont().select("location_id", "estado"), "location_id")
            .join(estado_zona, "estado")
            .select("sensor_id", "zona")
        )
        cont_zonas = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .join(sensor_zona, "sensor_id")
            .groupBy("zona")
            .agg(F.count("*").alias("total_actividad"))
            .withColumn("dataset", F.lit("contaminacion"))
        )

        w = Window.partitionBy("dataset").orderBy(F.desc("total_actividad"), F.asc("zona"))
        return (
            acc_zonas.select("dataset", "zona", "total_actividad")
            .unionByName(cont_zonas.select("dataset", "zona", "total_actividad"))
            .withColumn("ranking", F.row_number().over(w))
            .filter(F.col("ranking") <= int(top_n))
            .select("dataset", "ranking", "zona", "total_actividad")
            .orderBy(F.asc("dataset"), F.asc("ranking"))
        )

    # 6. Tendencias por periodo (accidentes + contaminacion)
    def tendencias_por_periodo(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        acc_mes = (
            self._accidentes(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("start_time_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("accidentes"))
        )

        cont_mes = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("datetime_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("contaminacion"))
        )

        w = Window.partitionBy("dataset").orderBy("periodo")
        return (
            acc_mes.unionByName(cont_mes)
            .withColumn("registros_mes_anterior", F.lag("total_registros", 1).over(w))
            .withColumn("variacion_absoluta", F.col("total_registros") - F.col("registros_mes_anterior"))
            .withColumn(
                "variacion_pct",
                F.when(
                    F.col("registros_mes_anterior").isNotNull() & (F.col("registros_mes_anterior") != 0),
                    F.round(((F.col("total_registros") - F.col("registros_mes_anterior")) / F.col("registros_mes_anterior")) * 100.0, 2),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn(
                "tendencia",
                F.when(F.col("variacion_absoluta") > 0, F.lit("ALZA"))
                .when(F.col("variacion_absoluta") < 0, F.lit("BAJA"))
                .when(F.col("variacion_absoluta") == 0, F.lit("ESTABLE"))
                .otherwise(F.lit("INICIAL")),
            )
            .select(
                "dataset",
                "periodo",
                "total_registros",
                "registros_mes_anterior",
                "variacion_absoluta",
                "variacion_pct",
                "tendencia",
            )
            .orderBy(F.asc("periodo"), F.asc("dataset"))
        )

    # 7. Deteccion de anomalias (accidentes + contaminacion)
    def deteccion_anomalias(self, umbral_z: float = 1.5, fecha_ini: str | None = None,
                            fecha_fin: str | None = None) -> DataFrame:
        acc_mes = (
            self._accidentes(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("start_time_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(F.count("*").cast("double").alias("total_registros"))
            .withColumn("dataset", F.lit("accidentes"))
        )

        cont_mes = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .withColumn("periodo", F.date_format(F.col("datetime_utc"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(F.count("*").cast("double").alias("total_registros"))
            .withColumn("dataset", F.lit("contaminacion"))
        )

        w_ds = Window.partitionBy("dataset")
        return (
            acc_mes.unionByName(cont_mes)
            .withColumn("media_mensual", F.round(F.avg("total_registros").over(w_ds), 2))
            .withColumn("desv_estandar", F.round(F.stddev("total_registros").over(w_ds), 2))
            .withColumn(
                "z_score",
                F.when(F.col("desv_estandar").isNull() | (F.col("desv_estandar") == 0), F.lit(0.0))
                .otherwise(F.round((F.col("total_registros") - F.col("media_mensual")) / F.col("desv_estandar"), 2)),
            )
            .withColumn(
                "estado",
                F.when(F.abs(F.col("z_score")) >= F.lit(float(umbral_z)), F.lit("ANOMALIA")).otherwise(F.lit("NORMAL")),
            )
            .select("dataset", "periodo", "total_registros", "media_mensual", "desv_estandar", "z_score", "estado")
            .orderBy(F.desc(F.abs(F.col("z_score"))), F.asc("dataset"), F.asc("periodo"))
        )

    # 8. Agrupamiento por categorias (accidentes + contaminacion + clima)
    def agrupamiento_por_categorias(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        """Categorías: severidad (accidentes), sensor (contaminación) y dispositivo (clima).
        El porcentaje se calcula con una ventana sobre el total (sin un count() extra)."""
        w = Window.partitionBy()   # todo el conjunto: pocas filas ya agregadas

        def porcentaje():
            return F.round(F.col("total_registros") * 100.0 / F.sum("total_registros").over(w), 2).alias("porcentaje")

        acc_cat = (
            self._accidentes(fecha_ini, fecha_fin).filter(F.col("severity").isNotNull())
            .groupBy("severity").agg(F.count("*").alias("total_registros"))
            .select(
                F.lit("accidentes").alias("dataset"),
                F.col("severity").cast("string").alias("categoria"),
                F.col("total_registros"),
                porcentaje(),
                F.lit(None).cast("double").alias("valor_promedio"),
            )
        )

        cont_cat = (
            self._mediciones_cont(fecha_ini, fecha_fin).filter(F.col("sensor_id").isNotNull())
            .groupBy("sensor_id")
            .agg(F.count("*").alias("total_registros"), F.round(F.avg(F.col("value").cast("double")), 2).alias("valor_promedio"))
            .select(
                F.lit("contaminacion").alias("dataset"),
                F.col("sensor_id").cast("string").alias("categoria"),
                F.col("total_registros"),
                porcentaje(),
                F.col("valor_promedio"),
            )
        )

        clima_cat = (
            self._mediciones_clima(fecha_ini, fecha_fin).filter(F.col("dev_id").isNotNull())
            .groupBy("dev_id")
            .agg(F.count("*").alias("total_registros"), F.round(F.avg(F.col("temperature").cast("double")), 2).alias("valor_promedio"))
            .select(
                F.lit("clima").alias("dataset"),
                F.col("dev_id").cast("string").alias("categoria"),
                F.col("total_registros"),
                porcentaje(),
                F.col("valor_promedio"),
            )
        )

        return (
            acc_cat.unionByName(cont_cat)
            .unionByName(clima_cat)
            .select("dataset", "categoria", "total_registros", "porcentaje", "valor_promedio")
            .orderBy(F.asc("dataset"), F.desc("total_registros"))
        )

    # 9. Comparacion historica entre anios (accidentes + contaminacion + clima)
    def comparacion_historica_anios(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        acc_anual = (
            self._accidentes(fecha_ini, fecha_fin)
            .withColumn("anio", F.year(F.col("start_time_utc")).cast("int"))
            .filter(F.col("anio").isNotNull())
            .groupBy("anio")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("accidentes"))
        )

        cont_anual = (
            self._mediciones_cont(fecha_ini, fecha_fin)
            .withColumn("anio", F.year(F.col("datetime_utc")).cast("int"))
            .filter(F.col("anio").isNotNull())
            .groupBy("anio")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("contaminacion"))
        )

        clima_anual = (
            self._mediciones_clima(fecha_ini, fecha_fin)
            .withColumn("anio", F.year(F.col("time")).cast("int"))
            .filter(F.col("anio").isNotNull())
            .groupBy("anio")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("clima"))
        )

        w = Window.partitionBy("dataset").orderBy("anio")
        return (
            acc_anual.unionByName(cont_anual)
            .unionByName(clima_anual)
            .withColumn("registros_anio_anterior", F.lag("total_registros", 1).over(w))
            .withColumn("variacion_anual", F.col("total_registros") - F.col("registros_anio_anterior"))
            .withColumn(
                "variacion_anual_pct",
                F.when(
                    F.col("registros_anio_anterior").isNotNull() & (F.col("registros_anio_anterior") != 0),
                    F.round(((F.col("total_registros") - F.col("registros_anio_anterior")) / F.col("registros_anio_anterior")) * 100.0, 2),
                ).otherwise(F.lit(None).cast("double")),
            )
            .select("dataset", "anio", "total_registros", "registros_anio_anterior", "variacion_anual", "variacion_anual_pct")
            .orderBy(F.asc("anio"), F.asc("dataset"))
        )

    # 10. Dashboard analitico o reporte consolidado (accidentes + contaminacion + clima)
    def dashboard_reporte_consolidado(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        total_acc = self._accidentes(fecha_ini, fecha_fin).count()
        total_cont_med = self._mediciones_cont(fecha_ini, fecha_fin).count()
        total_cont_sen = self._leer("contaminacion/sensores").count()
        total_clima_med = self._mediciones_clima(fecha_ini, fecha_fin).count()
        total_clima_sen = self._leer("clima/sensores").count()

        filas = [
            ("accidentes", "eventos_totales", int(total_acc), self.ruta_accidentes),
            ("contaminacion", "mediciones_totales", int(total_cont_med), f"{self.ruta}/contaminacion/mediciones"),
            ("contaminacion", "sensores_registrados", int(total_cont_sen), f"{self.ruta}/contaminacion/sensores"),
            ("clima", "mediciones_totales", int(total_clima_med), f"{self.ruta}/clima/mediciones"),
            ("clima", "sensores_registrados", int(total_clima_sen), f"{self.ruta}/clima/sensores"),
        ]

        return self.spark.createDataFrame(filas, ["dataset", "metrica", "valor", "origen"])

    # Registro de consultas obligatorias: id -> (descripción, método)
    CONSULTAS = {
        1: ("Sensores que generan más información", "sensores_mas_activos"),
        2: ("Distribución geográfica de eventos", "distribucion_geografica_eventos"),
        3: ("Evolución temporal de indicadores urbanos", "evolucion_temporal_indicadores"),
        4: ("Correlación entre variables", "correlacion_variables"),
        5: ("Ranking de zonas con mayor actividad", "ranking_zonas_actividad"),
        6: ("Tendencias por periodo", "tendencias_por_periodo"),
        7: ("Detección de anomalías", "deteccion_anomalias"),
        8: ("Agrupamiento por categorías", "agrupamiento_por_categorias"),
        9: ("Comparación histórica entre años", "comparacion_historica_anios"),
        10: ("Reporte consolidado", "dashboard_reporte_consolidado"),
    }

    def ejecutar(self, numero: int, **parametros) -> DataFrame:
        """Ejecuta una consulta obligatoria por su número (ver CONSULTAS)."""
        if numero not in self.CONSULTAS:
            raise ValueError(f"Consulta {numero} no disponible. Disponibles: {sorted(self.CONSULTAS)}")
        return getattr(self, self.CONSULTAS[numero][1])(**parametros)
