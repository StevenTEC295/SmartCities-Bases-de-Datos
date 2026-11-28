
from datetime import date, datetime, time, timedelta
import json

import pandas as pd
import streamlit as st

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class Dashboard:
    """
    Dashboard de SmartCities.

    Cada vista conserva sus últimos resultados en st.session_state.
    Las consultas se ejecutan únicamente cuando se presiona el botón
    de actualización correspondiente.

    Categorías:
        accidentes
        temperature
        humidity
        o3
        pm10
        pm25

    Orígenes:
        Departamento de transporte EE. UU.
        Victorian Government, Australia
        Open AQ
    """

    LIMITE_FILAS = 500
    
    LIMITE_SERIE = 5000
    LIMITE_EVENTOS = 10

    COLECCION_CLIMA = "clima_measurements"
    COLECCION_CONTAMINACION = "mediciones"
    BASE_MONGO = "smartcities"

    CATEGORIAS = [
        "accidentes",
        "temperature",
        "humidity",
        "o3",
        "pm10",
        "pm25",
    ]

    ORIGENES = [
        "Departamento de transporte EE. UU.",
        "Victorian Government, Australia",
        "Open AQ",
    ]

    # Rango real de los datos: desde el accidente más antiguo (2016-01-14)
    # hasta la última medición de contaminación (2026-11-04).

    FECHA_MINIMA = date(2016, 1, 14)
    FECHA_MAXIMA = date(2026, 11, 4)

    # Zonas del dataset de contaminación. Las de accidentes son códigos de estado
    # de 2 letras (MA, NY...) y el resto son los dispositivos de clima.
    ZONAS_CONTAMINACION = (
        "Boston - Roxbury",
        "Bronx - IS52",
        "McMillan Reservoir",
        "Essex",
    )

    def __init__(
        self,
        queries,
        queries_pg,
        queries_mg,
        queries_hdfs,
        filtros,
        titulo="Dashboard de SmartCities",
        
    ):
        self.queries = queries
        self.queries_pg = queries_pg
        self.queries_mg = queries_mg
        self.queries_hdfs = queries_hdfs
        self.filtros = filtros

        self.titulo = titulo
        

    # ============================================================
    # INICIALIZACIÓN Y ESTADO
    # ============================================================

    def _inicializar_estado(self):
        """
        Inicializa los resultados de las vistas una sola vez
        durante la sesión del usuario.
        """

        # Conservar el objeto Filtros entre los reruns de Streamlit.
        if "dashboard_filtros" not in st.session_state:
            st.session_state.dashboard_filtros = self.filtros

        self.filtros = st.session_state.dashboard_filtros

        valores_iniciales = {
            "vista_1_datos": None,
            "vista_2_datos": None,
            "tendencia_1_datos": None,
            "tendencia_2_datos": None,
            "correlacion": None,
            "vista_5_datos": None,
            "vista_6_datos": None,
            "vista_6_total": None,
            "categoria_tendencia_2": "temperature",
            "pg_datos": None,
            "pg_numero": None,
            "pg_criterio": "sensores",
            "mg_datos": None,
            "mg_numero": None,
            "sp_datos": None,
            "sp_numero": None,
            "kpi_datos": None,
            "hd_datos": None,
            "hd_numero": None,
            "hd_op": None,
            "hd_bloques": None,
            "error_vista_1": None,
            "error_vista_2": None,
            "error_tendencia_1": None,
            "error_tendencia_2": None,
            "error_vista_5": None,
            "error_vista_6": None,
        }

        for clave, valor in valores_iniciales.items():
            if clave not in st.session_state:
                st.session_state[clave] = valor

    def _ejecutar_consulta(self, clave, funcion):
        """
        Ejecuta una consulta y guarda el resultado.

        Si ocurre un error, conserva el resultado anterior.
        """

        try:
            resultado = funcion()
            st.session_state[clave] = resultado
            return True

        except Exception as error:
            st.error(f"No fue posible actualizar la consulta: {error}")
            return False

    # ============================================================
    # UTILIDADES
    # ============================================================

    @staticmethod
    def _sql_texto(valor):
        """Escapa comillas simples para literales SQL."""

        return str(valor).replace("'", "''")

    @staticmethod
    def _fecha_datetime(fecha, final=False):
        """
        Convierte date/datetime a datetime.

        Si final=True y se recibe un date, utiliza el final del día.
        """

        if isinstance(fecha, datetime):
            return fecha

        if isinstance(fecha, date):
            if final:
                return datetime.combine(fecha, time(23, 59, 59))
            return datetime.combine(fecha, time.min)

        raise ValueError("Fecha inválida")
    @staticmethod
    def _tiene_fechas_validas(df, col_fecha="fecha"):
        if df is None or col_fecha not in df.columns:
            return False

        fila = (
            df.select(F.col(col_fecha).cast("timestamp").alias("_f"))
            .filter(F.col("_f").isNotNull())
            .agg(F.min("_f").alias("f_min"))
            .first()
        )
        return bool(fila and fila["f_min"] is not None)
    def _rango_fechas(self):
        """
        Obtiene el rango de fechas desde Filtros.

        Se espera:
            filtros.get_fecha() == (fecha_inicio, fecha_fin)
        """

        fechas = self.filtros.get_fecha()

        if not isinstance(fechas, tuple) or len(fechas) != 2:
            raise ValueError(
                "Filtros.get_fecha() debe devolver "
                "(fecha_inicio, fecha_fin)."
            )

        inicio, fin = fechas

        if inicio is None or fin is None:
            raise ValueError("Debe seleccionar un rango de fechas.")

        inicio = self._fecha_datetime(inicio)
        fin = self._fecha_datetime(fin, final=True)

        if inicio > fin:
            raise ValueError("La fecha inicial supera la fecha final.")

        return inicio, fin

    def _mongo_pipeline_fechas(self, campo):
        """
        Genera un $match de MongoDB para el rango seleccionado.

        Utiliza Extended JSON para representar fechas BSON.
        """

        inicio, fin = self._rango_fechas()

        inicio_iso = inicio.isoformat() + "Z"
        fin_iso = fin.isoformat() + "Z"

        return [
            {
                "$match": {
                    campo: {
                        "$gte": {"$date": inicio_iso},
                        "$lte": {"$date": fin_iso},
                    }
                }
            }
        ]

    def _a_pandas(self, spark_df, limite=None):
        """
        Convierte un Spark DataFrame a Pandas.

        Se utiliza después de agregar y limitar los resultados,
        no para descargar grandes colecciones completas.
        Sin `limite` se usa LIMITE_FILAS (para tablas); las series
        diarias pasan LIMITE_SERIE.
        """

        if limite is None:
            limite = self.LIMITE_FILAS

        return spark_df.limit(limite).toPandas()

    def _origen_dataset(self):
        origen = self.filtros.get_origen()

        equivalencias = {
            "Departamento de transporte EE. UU.": "accidentes",
            "Victorian Government, Australia": "clima",
            "Open AQ": "contaminacion",
        }

        if origen is None:
            return None

        return equivalencias.get(origen)

    def _dataset_categoria(self, categoria):
        if categoria == "accidentes":
            return "accidentes"

        if categoria in ("temperature", "humidity"):
            return "clima"

        if categoria in ("o3", "pm10", "pm25"):
            return "contaminacion"

        raise ValueError(f"Categoría desconocida: {categoria}")

    def _validar_categoria_origen(self, categoria):
        """
        Verifica que la categoría pertenezca al origen seleccionado.
        """

        dataset_categoria = self._dataset_categoria(categoria)
        dataset_origen = self._origen_dataset()

        if dataset_origen is not None and dataset_categoria != dataset_origen:
            raise ValueError(
                f"La categoría '{categoria}' no corresponde "
                "al origen seleccionado."
            )

    @classmethod
    def _dataset_zona(cls, zona):
        """Dataset al que pertenece una zona: accidentes (estado), contaminación o clima."""

        if zona in cls.ZONAS_CONTAMINACION:
            return "contaminacion"

        if len(zona) == 2 and zona.isupper():
            return "accidentes"

        return "clima"

    def _zonas_de_categoria(self, categoria):
        """Zonas válidas para la categoría: solo las de su mismo dataset."""

        dataset = self._dataset_categoria(categoria)

        return [
            zona
            for zona in self.filtros.zonas_validas
            if self._dataset_zona(zona) == dataset
        ]

    def _zona(self, dataset=None):
        """
        Zona seleccionada. Si se indica el dataset que se va a leer, devuelve
        la zona solo cuando pertenece a ese dataset; si no, None (sin filtro de
        zona). Así una zona de otro dataset no deja resultados vacíos.
        """

        zona = self.filtros.get_zona()

        if zona is None or dataset is None:
            return zona

        if self._dataset_zona(zona) != dataset:
            return None

        return zona

    # ============================================================
    # LECTURA DE LOS DATASETS
    # ============================================================

    def _leer_accidentes(self):
        """
        Lee accidentes desde PostgreSQL.

        Retorna:
            id, start_time, severity, city, state_code
        """

        inicio, fin = self._rango_fechas()

        inicio_sql = inicio.strftime("%Y-%m-%d %H:%M:%S")
        fin_sql = fin.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
            SELECT
                a.id,
                a.start_time,
                a.severity,
                c.city,
                c.state_code
            FROM accidents a
            INNER JOIN cities c
                ON a.city_id = c.city_id
            WHERE a.start_time >= '{inicio_sql}'
              AND a.start_time <= '{fin_sql}'
        """

        zona = self._zona("accidentes")

        if zona is not None:
            sql +=f" AND c.state_code = '{self._sql_texto(zona)}'"

        return self.queries_pg.consultar(sql)

    def _leer_clima(self):
        """
        Lee mediciones de clima desde MongoDB y las relaciona
        con la metadata de sensores almacenada en PostgreSQL.

        Retorna:
            dev_id, name, time, temperature, humidity
        """

        pipeline = self._mongo_pipeline_fechas("time")

        mediciones = self.queries_mg.agregar(
            self.COLECCION_CLIMA,
            pipeline,
        )

        if "dev_id" not in mediciones.columns:
            esquema_vacio = StructType([
                StructField("dev_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("time", TimestampType(), True),
                StructField("temperature", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
            ])
            return self.queries_mg.spark.createDataFrame([], esquema_vacio)

        sensores = self.queries_pg.consultar("""
            SELECT
                dev_id,
                name
            FROM sensors_table
        """)

        resultado = mediciones.join(
            sensores,
            on="dev_id",
            how="inner",
        )

        zona = self._zona("clima")

        if zona is not None:
            resultado = resultado.filter(
                F.col("name") == zona
            )

        return resultado
    def _leer_contaminacion(self):
        """
        Lee mediciones de contaminación desde MongoDB y las relaciona
        con sensores, ubicaciones y parámetros de PostgreSQL.

        Retorna:
            sensor_id, datetime_utc, value,
            location_name, parameter
        """

        pipeline = self._mongo_pipeline_fechas("datetime_utc")

        mediciones = self.queries_mg.agregar(
            self.COLECCION_CONTAMINACION,
            pipeline,
        )

        metadata = self.queries_pg.consultar("""
            SELECT
                s.sensor_id,
                l.location_name,
                p.parameter
            FROM sensores s
            INNER JOIN locations l
                ON s.location_id = l.location_id
            INNER JOIN parametros p
                ON s.parameter_id = p.parameter_id
        """)

        resultado = mediciones.join(
            metadata,
            on="sensor_id",
            how="inner",
        )

        zona = self._zona("contaminacion")

        if zona is not None:
            resultado = resultado.filter(
                F.col("location_name") == zona
            )

        return resultado

    def _leer_categoria(self, categoria):
        """
        Devuelve el Spark DataFrame correspondiente a una categoría.
        """

        self._validar_categoria_origen(categoria)

        if categoria == "accidentes":
            return self._leer_accidentes()

        if categoria in ("temperature", "humidity"):
            return self._leer_clima()

        return self._leer_contaminacion()

    # ============================================================
    # AGREGACIONES COMUNES
    # ============================================================

    def _columna_fecha(self, df, campo):
        """
        Normaliza un campo temporal a timestamp.
        """

        return df.withColumn(
            "fecha",
            F.to_timestamp(F.col(campo)),
        )

    def _serie_temporal(self, categoria):
        """
        Calcula una serie temporal diaria.

        Accidentes:
            cantidad de accidentes por día.

        Clima:
            promedio diario de temperatura o humedad.

        Contaminación:
            promedio diario del parámetro seleccionado.
        """

        df = self._leer_categoria(categoria)

        if categoria == "accidentes":
            df = self._columna_fecha(df, "start_time")

            if not self._tiene_fechas_validas(df, "fecha"):
                return pd.DataFrame(columns=["dia", "valor"])

            resultado = (
                df.withColumn("dia", F.to_date("fecha"))
                .groupBy("dia")
                .agg(F.count("*").alias("valor"))
            )

        elif categoria in ("temperature", "humidity"):
            df = self._columna_fecha(df, "time")

            if not self._tiene_fechas_validas(df, "fecha"):
                return pd.DataFrame(columns=["dia", "valor"])

            resultado = (
                df.withColumn("dia", F.to_date("fecha"))
                .groupBy("dia")
                .agg(
                    F.avg(categoria).alias("valor")
                )
            )

        else:
            df = df.filter(
                F.col("parameter") == categoria
            )

            df = self._columna_fecha(df, "datetime_utc")

            if not self._tiene_fechas_validas(df, "fecha"):
                return pd.DataFrame(columns=["dia", "valor"])

            resultado = (
                df.withColumn("dia", F.to_date("fecha"))
                .groupBy("dia")
                .agg(
                    F.avg("value").alias("valor")
                )
            )

        resultado = resultado.orderBy("dia")

        return self._a_pandas(resultado, self.LIMITE_SERIE)
    # ============================================================
    # VISTA 1: CANTIDAD DE SENSORES POR TIPO
    # ============================================================



    def consultar_vista_1(self):
        """
        Cuenta los sensores registrados en PostgreSQL por categoría.

        Clima:
            Sensors_table contiene los sensores de humidity y temperature.
            Cada sensor puede medir ambas categorías.

        Contaminación:
            sensores se relaciona con parametros para identificar
            los sensores de O3, PM10 y PM25.
        """

        registros = []

        origen = self._origen_dataset()

        # Sensores de clima
        if origen in (None, "clima"):
            sql_clima = """
                SELECT COUNT(*) AS cantidad
                FROM Sensors_table
            """

            df_clima = self.queries_pg.consultar(sql_clima)
            cantidad_clima = df_clima.collect()[0]["cantidad"]

            for categoria in ("humidity", "temperature"):
                registros.append({
                    "Tipo": categoria,
                    "Cantidad de sensores": cantidad_clima,
                })

        # Sensores de contaminación
        if origen in (None, "contaminacion"):
            sql_contaminacion = """
                SELECT
                    p.parameter AS tipo,
                    COUNT(s.sensor_id) AS cantidad
                FROM sensores s
                INNER JOIN parametros p
                    ON s.parameter_id = p.parameter_id
                WHERE p.parameter IN ('o3', 'pm10', 'pm25')
                GROUP BY p.parameter
            """

            df_contaminacion = self.queries_pg.consultar(sql_contaminacion)

            # Inicializar en cero para mostrar también categorías sin sensores.
            cantidades = {
                "o3": 0,
                "pm10": 0,
                "pm25": 0,
            }

            for fila in df_contaminacion.collect():
                cantidades[fila["tipo"]] = fila["cantidad"]

            for categoria in ("o3", "pm10", "pm25"):
                registros.append({
                    "Tipo": categoria,
                    "Cantidad de sensores": cantidades[categoria],
                })

        return pd.DataFrame(registros)



    def mostrar_vista_1(self):
        st.subheader("1. Cantidad de sensores por tipo")

        if st.button("Actualizar vista 1", key="btn_vista_1"):
            self._ejecutar_consulta(
                "vista_1_datos",
                self.consultar_vista_1,
            )

        datos = st.session_state.vista_1_datos

        if datos is not None:
            if datos.empty:
                st.info("No se encontraron sensores para los filtros seleccionados.")
            else:
                st.bar_chart(
                    datos.set_index("Tipo")
                )

    # ============================================================
    # VISTA 2: RANKING DE UBICACIONES
    # ============================================================

    def consultar_vista_2(self):
        categoria = self.filtros.get_categoria()

        if categoria is None:
            raise ValueError("Seleccione una categoría.")

        df = self._leer_categoria(categoria)

        if categoria == "accidentes":
            resultado = (
                df.groupBy("state_code")
                .agg(F.count("*").alias("valor"))
                .withColumnRenamed("state_code", "ubicacion")
            )

        elif categoria in ("temperature", "humidity"):
            resultado = (
                df.groupBy("name")
                .agg(F.avg(categoria).alias("valor"))
                .withColumnRenamed("name", "ubicacion")
            )

        else:
            df = df.filter(
                F.col("parameter") == categoria
            )

            resultado = (
                df.groupBy("location_name")
                .agg(F.avg("value").alias("valor"))
                .withColumnRenamed("location_name", "ubicacion")
            )

        usar_maximo = self.filtros.get_usar_maximo()

        if usar_maximo:
            resultado = resultado.orderBy(
                F.desc("valor")
            )
        else:
            resultado = resultado.orderBy(
                F.asc("valor")
            )

        cantidad = self.filtros.get_ranking_cantidad()

        if cantidad is None:
            cantidad = 10

        resultado = resultado.limit(cantidad)

        return self._a_pandas(resultado)

    def mostrar_vista_2(self):
        st.subheader("2. Ranking de ubicaciones")

        if st.button("Actualizar vista 2", key="btn_vista_2"):
            self._ejecutar_consulta(
                "vista_2_datos",
                self.consultar_vista_2,
            )

        datos = st.session_state.vista_2_datos

        if datos is not None:
            if datos.empty:
                st.info("No se encontraron resultados para el ranking.")
            else:
                st.dataframe(
                    datos,
                    use_container_width=True,
                    hide_index=True,
                )

    # ============================================================
    # VISTA 3: PRIMERA TENDENCIA TEMPORAL
    # ============================================================

    def consultar_tendencia_1(self):
        categoria = self.filtros.get_categoria()

        if categoria is None:
            raise ValueError("Seleccione una categoría.")

        return self._serie_temporal(categoria)

    def mostrar_tendencia_1(self):
        st.subheader("3. Primera tendencia temporal")

        if st.button(
            "Actualizar tendencia 1",
            key="btn_tendencia_1",
        ):
            actualizado = self._ejecutar_consulta(
                "tendencia_1_datos",
                self.consultar_tendencia_1,
            )

            if actualizado:
                self.actualizar_correlacion()

        datos = st.session_state.tendencia_1_datos

        if datos is not None:
            if datos.empty:
                st.info("No hay datos para mostrar.")
            else:
                datos = datos.copy()
                datos["promedio"] = datos["valor"].mean()

                grafico = datos.set_index("dia")[
                    ["valor", "promedio"]
                ]

                st.line_chart(grafico)

    # ============================================================
    # VISTA 4: SEGUNDA TENDENCIA Y CORRELACIÓN
    # ============================================================

    def consultar_tendencia_2(self):
        categoria = st.session_state.categoria_tendencia_2

        if categoria is None:
            raise ValueError("Seleccione la segunda categoría.")

        return self._serie_temporal(categoria)

    def actualizar_correlacion(self):
        """
        Calcula la correlación de Pearson entre las últimas
        dos series temporales almacenadas.

        Las series se relacionan por día.
        """

        serie_1 = st.session_state.tendencia_1_datos
        serie_2 = st.session_state.tendencia_2_datos

        if serie_1 is None or serie_2 is None:
            st.session_state.correlacion = None
            return

        if serie_1.empty or serie_2.empty:
            st.session_state.correlacion = None
            return

        datos_1 = serie_1[["dia", "valor"]].rename(
            columns={"valor": "valor_1"}
        )

        datos_2 = serie_2[["dia", "valor"]].rename(
            columns={"valor": "valor_2"}
        )

        comparacion = pd.merge(
            datos_1,
            datos_2,
            on="dia",
            how="inner",
        )

        comparacion = comparacion.dropna(
            subset=["valor_1", "valor_2"]
        )

        if len(comparacion) < 2:
            st.session_state.correlacion = None
            return

        correlacion = comparacion["valor_1"].corr(
            comparacion["valor_2"],
            method="pearson",
        )

        st.session_state.correlacion = {
            "valor": correlacion,
            "cantidad": len(comparacion),
        }

    def mostrar_tendencia_2(self):
        st.subheader("4. Segunda tendencia temporal")

        opciones = self.filtros.categorias_validas

        categoria_actual = st.session_state.categoria_tendencia_2

        if categoria_actual not in opciones:
            categoria_actual = opciones[0]

        categoria = st.selectbox(
            "Categoría de la segunda tendencia",
            options=opciones,
            index=opciones.index(categoria_actual),
            key="selector_categoria_tendencia_2",
        )

        st.session_state.categoria_tendencia_2 = categoria

        if st.button(
            "Actualizar tendencia 2",
            key="btn_tendencia_2",
        ):
            actualizado = self._ejecutar_consulta(
                "tendencia_2_datos",
                self.consultar_tendencia_2,
            )

            if actualizado:
                self.actualizar_correlacion()

        zona_filtro = self.filtros.get_zona()

        if (
            zona_filtro is not None
            and self._dataset_zona(zona_filtro) != self._dataset_categoria(categoria)
        ):
            st.caption(
                f"La zona '{zona_filtro}' no pertenece al dataset de esta "
                "categoría; esta tendencia usa todas sus zonas."
            )

        datos = st.session_state.tendencia_2_datos

        if datos is not None:
            if datos.empty:
                st.info("No hay datos para mostrar.")
            else:
                datos = datos.copy()
                datos["promedio"] = datos["valor"].mean()

                grafico = datos.set_index("dia")[
                    ["valor", "promedio"]
                ]

                st.line_chart(grafico)

        # La correlación se muestra dentro de esta vista.
        st.divider()
        st.markdown("**Correlación de Pearson entre ambas tendencias**")

        correlacion = st.session_state.correlacion

        if correlacion is None:
            st.info(
                "La correlación estará disponible cuando ambas "
                "tendencias tengan datos comparables."
            )
        else:
            valor = correlacion["valor"]
            cantidad = correlacion["cantidad"]

            if pd.isna(valor):
                st.info("No se pudo calcular la correlación.")
            else:
                st.metric(
                    "Coeficiente de Pearson (r)",
                    f"{valor:.4f}",
                )

                st.caption(
                    f"Calculado con {cantidad} días coincidentes."
                )

    # ============================================================
    # VISTA 5: CANTIDAD DE MEDICIONES POR SENSOR
    # ============================================================

    def consultar_vista_5(self):
        categoria = self.filtros.get_categoria()

        if categoria is None:
            raise ValueError("Seleccione una categoría.")

        df = self._leer_categoria(categoria)

        if categoria == "accidentes":
            cantidad = df.count()

            return pd.DataFrame({
                "Descripción": ["Accidentes registrados"],
                "Cantidad": [cantidad],
            })

        if categoria in ("temperature", "humidity"):
            resultado = (
                df.filter(F.col(categoria).isNotNull())
                .groupBy("dev_id", "name")
                .agg(
                    F.count("*").alias("cantidad")
                )
                .withColumnRenamed("dev_id", "sensor")
                .withColumnRenamed("name", "ubicacion")
                .orderBy(F.desc("cantidad"))
            )

        else:
            df = df.filter(
                (F.col("parameter") == categoria)
                & F.col("value").isNotNull()
            )

            resultado = (
                df.groupBy("sensor_id", "location_name")
                .agg(
                    F.count("*").alias("cantidad")
                )
                .withColumnRenamed("sensor_id", "sensor")
                .withColumnRenamed("location_name", "ubicacion")
                .orderBy(F.desc("cantidad"))
            )

        return self._a_pandas(resultado)

    def mostrar_vista_5(self):
        st.subheader("5. Cantidad de mediciones por sensor")

        if st.button("Actualizar vista 5", key="btn_vista_5"):
            self._ejecutar_consulta(
                "vista_5_datos",
                self.consultar_vista_5,
            )

        datos = st.session_state.vista_5_datos

        if datos is not None:
            if datos.empty:
                st.info("No se encontraron mediciones.")
            else:
                st.dataframe(
                    datos,
                    use_container_width=True,
                    hide_index=True,
                )

    # ============================================================
    # VISTA 6: EVENTOS CRÍTICOS
    # ============================================================

    def consultar_vista_6(self):
        categoria = self.filtros.get_categoria()

        if categoria is None:
            raise ValueError("Seleccione una categoría.")

        activar, umbral = self.filtros.get_umbral()

        df = self._leer_categoria(categoria)

        if categoria == "accidentes":
            df = df.select(
                F.lit("accidentes").alias("categoria"),
                F.col("severity").cast("double").alias("valor"),
                F.col("start_time").alias("fecha"),
                F.col("state_code").alias("zona"),
            )

        elif categoria in ("temperature", "humidity"):
            df = df.select(
                F.lit(categoria).alias("categoria"),
                F.col(categoria).cast("double").alias("valor"),
                F.col("time").alias("fecha"),
                F.col("name").alias("zona"),
            )

        else:
            df = df.filter(
                F.col("parameter") == categoria
            )

            df = df.select(
                F.col("parameter").alias("categoria"),
                F.col("value").cast("double").alias("valor"),
                F.col("datetime_utc").alias("fecha"),
                F.col("location_name").alias("zona"),
            )

        if activar:
            df = df.filter(F.col("valor") >= float(umbral))
        else:
            df = df.filter(F.col("valor") <= float(umbral))

        total = df.count()

        eventos = (
            df.orderBy(F.desc("valor"))
            .limit(self.LIMITE_EVENTOS)
        )

        return self._a_pandas(eventos), total

    def mostrar_vista_6(self):
        st.subheader("6. Eventos críticos")

        activar, umbral = self.filtros.get_umbral()

        if activar:
            st.caption(f"Eventos con valor ≥ {umbral}")
        else:
            st.caption(f"Eventos con valor ≤ {umbral}")

        if st.button("Actualizar vista 6", key="btn_vista_6"):
            try:
                datos, total = self.consultar_vista_6()

                st.session_state.vista_6_datos = datos
                st.session_state.vista_6_total = total

            except Exception as error:
                st.error(f"No fue posible actualizar los eventos: {error}")

        datos = st.session_state.vista_6_datos
        total = st.session_state.vista_6_total

        if datos is not None:
            st.metric(
                "Cantidad total de eventos que cumplen el umbral",
                total,
            )

            if datos.empty:
                st.info("No se encontraron eventos críticos.")
            else:
                st.dataframe(
                    datos,
                    use_container_width=True,
                    hide_index=True,
                )

    # ============================================================
    # KPIs (MÉTRICAS CLAVE): AMBIENTE E IoT
    # ============================================================

    KPI_SIN_DATOS = "Sin datos"

    @staticmethod
    def _kpi_seguro(funcion):
        """Ejecuta el cálculo de un KPI; si falla devuelve el mensaje (str) y las demás tarjetas siguen."""
        try:
            return funcion()
        except Exception as error:
            return f"{type(error).__name__}: {error}"[:300]

    def consultar_kpis(self):
        """
        Calcula los datos de todos los KPIs con el rango de fechas del panel.
        Cada fuente va por separado: si una falla, su valor es un texto de error y el resto se conserva.
        """

        inicio, fin = self._rango_fechas()
        fecha_ini = inicio.strftime("%Y-%m-%d")
        fecha_fin = fin.strftime("%Y-%m-%d")

        sensores = self.queries_pg.sensores_metadata()
        mg = self.queries_mg

        return {
            # Promedios por tipo de sensor (temperatura, humedad, PM2.5, O3) y mediciones recibidas
            "promedios": self._kpi_seguro(lambda: self._a_pandas(
                mg.promedio_por_tipo_sensor(fecha_ini, fecha_fin, sensores))),
            # Alertas ambientales = lecturas sobre el límite de referencia de cada contaminante
            "alertas": self._kpi_seguro(lambda: self._a_pandas(mg.kpi_alertas(fecha_ini, fecha_fin, sensores))),
            # Datos por día y por hora (sobre días/horas con datos)
            "ritmo": self._kpi_seguro(lambda: self._a_pandas(mg.kpi_ritmo_datos(fecha_ini, fecha_fin))),
            # % de accidentes con clima adverso
            "clima_adverso": self._kpi_seguro(lambda: self._a_pandas(mg.kpi_clima_adverso(fecha_ini, fecha_fin))),
            # Sensores totales y activos/inactivos (PostgreSQL)
            "sensores": self._kpi_seguro(lambda: self._a_pandas(
                self.queries_pg.sensores_activos_inactivos(fecha_ini, fecha_fin))),
        }

    @staticmethod
    def _kpi_tiene(datos, *columnas):
        """True si `datos` es un DataFrame con filas y todas las columnas pedidas (un resultado vacío de
        Spark llega sin columnas)."""
        return not isinstance(datos, str) and datos is not None and not datos.empty \
            and all(columna in datos.columns for columna in columnas)

    def _tarjeta_kpi(self, columna, titulo, valor, ayuda, nota=None):
        """Dibuja una tarjeta: valor grande, ayuda con la fórmula y una nota opcional debajo."""
        with columna:
            st.metric(titulo, valor, help=ayuda)

            if nota:
                st.caption(nota)

    def _kpi_promedio(self, datos, tipo, unidad, decimales):
        """(valor, nota) del promedio de un tipo de sensor, o el aviso de error / sin datos."""
        promedios = datos["promedios"]

        if isinstance(promedios, str):
            return "Error", promedios

        if not self._kpi_tiene(promedios, "tipo", "promedio", "mediciones"):
            return self.KPI_SIN_DATOS, "No hay mediciones en el periodo."

        fila = promedios[promedios["tipo"] == tipo]

        if fila.empty:
            return self.KPI_SIN_DATOS, "No hay mediciones en el periodo."

        fila = fila.iloc[0]
        return f"{fila['promedio']:.{decimales}f} {unidad}", f"{int(fila['mediciones']):,} mediciones"

    def mostrar_kpis(self):
        st.subheader("Indicadores clave (KPIs)")
        st.caption(
            "Todos usan el rango de fechas del panel. Se calculan solo al presionar «Actualizar KPIs» "
            "(consultan MongoDB y PostgreSQL mediante Spark)."
        )

        if st.button("Actualizar KPIs", key="btn_kpis"):
            with st.spinner("Calculando KPIs..."):
                self._ejecutar_consulta("kpi_datos", self.consultar_kpis)

        datos = st.session_state.kpi_datos

        if datos is None:
            st.info("Presione «Actualizar KPIs» para calcular los indicadores.")
            return

        # ---------------- Ambiente ----------------
        st.markdown("**Ambiente**")
        col_1, col_2, col_3 = st.columns(3)

        valor, nota = self._kpi_promedio(datos, "temperature", "°C", 2)
        self._tarjeta_kpi(col_1, "Temperatura global promedio", valor,
                          "Promedio de todas las lecturas de temperatura de los dispositivos de clima. "
                          "Fuente: MongoDB (promedio por tipo de sensor).", nota)

        valor, nota = self._kpi_promedio(datos, "pm25", "µg/m³", 2)
        self._tarjeta_kpi(col_2, "PM2.5 promedio", valor,
                          "Promedio de las lecturas de los sensores de PM2.5. Fuente: MongoDB.", nota)

        valor, nota = self._kpi_promedio(datos, "o3", "ppm", 3)
        self._tarjeta_kpi(col_3, "O₃ promedio", valor,
                          "Promedio de las lecturas de los sensores de ozono (O₃). El dataset no tiene CO₂, "
                          "por eso se usa O₃. Fuente: MongoDB.", nota)

        col_4, col_5, col_6 = st.columns(3)

        valor, nota = self._kpi_promedio(datos, "humidity", "%", 1)
        self._tarjeta_kpi(col_4, "Humedad promedio", valor,
                          "Promedio de las lecturas de humedad de los dispositivos de clima. Fuente: MongoDB.", nota)

        alertas = datos["alertas"]
        limites = ", ".join(f"{k} > {v}" for k, v in self.queries_mg.LIMITES_ALERTA.items())
        ayuda_alertas = (
            "Lecturas que superan el límite de referencia de cada contaminante "
            f"({limites}). Son valores indicativos: se comparan contra lecturas por hora, no contra "
            "promedios de 24 horas. Fuente: MongoDB."
        )

        if isinstance(alertas, str):
            valor_alertas, nota_alertas = "Error", alertas
        elif self._kpi_tiene(alertas, "tipo", "alertas"):
            valor_alertas = f"{int(alertas['alertas'].sum()):,}"
            nota_alertas = " · ".join(f"{f.tipo}: {int(f.alertas):,}" for f in alertas.itertuples())
        else:
            valor_alertas, nota_alertas = self.KPI_SIN_DATOS, "No hay mediciones en el periodo."

        self._tarjeta_kpi(col_5, "Alertas ambientales", valor_alertas, ayuda_alertas, nota_alertas)

        adverso = datos["clima_adverso"]
        ayuda_adverso = (
            "Accidentes cuya condición climática es adversa (lluvia, llovizna, nieve, hielo, tormenta, "
            "niebla, bruma, humo o polvo) entre todos los accidentes del periodo. No cuentan nublado, "
            "«Fair» ni solo viento. El periodo se filtra por la hora del clima. Fuente: MongoDB."
        )

        if isinstance(adverso, str):
            valor_adverso, nota_adverso = "Error", adverso
        elif self._kpi_tiene(adverso, "accidentes", "adversos", "porcentaje") and int(adverso.iloc[0]["accidentes"] or 0) > 0:
            fila = adverso.iloc[0]
            valor_adverso = f"{fila['porcentaje']:.2f} %"
            nota_adverso = f"{int(fila['adversos']):,} de {int(fila['accidentes']):,} accidentes"
        else:
            valor_adverso = self.KPI_SIN_DATOS
            nota_adverso = "No hay accidentes en el periodo (los datos llegan hasta marzo de 2023)."

        self._tarjeta_kpi(col_6, "% accidentes con clima adverso", valor_adverso, ayuda_adverso, nota_adverso)

        # ---------------- IoT ----------------
        st.markdown("**IoT**")
        col_1, col_2, col_3 = st.columns(3)

        sensores = datos["sensores"]

        if isinstance(sensores, str):
            valor_total = valor_estado = "Error"
            nota_total = nota_estado = sensores
        elif self._kpi_tiene(sensores, "fuente", "estado", "sensores"):
            total = int(sensores["sensores"].sum())
            activos = int(sensores.loc[sensores["estado"] == "activo", "sensores"].sum())
            valor_total = f"{total:,}"
            nota_total = " · ".join(
                f"{fuente}: {int(cantidad)}"
                for fuente, cantidad in sensores.groupby("fuente")["sensores"].sum().items()
            )
            valor_estado = f"{activos} / {total - activos}"
            nota_estado = "activos / inactivos en el periodo"
        else:
            valor_total = valor_estado = self.KPI_SIN_DATOS
            nota_total = nota_estado = None

        self._tarjeta_kpi(col_1, "Número de sensores", valor_total,
                          "Sensores físicos distintos (12 estaciones de contaminación y 10 dispositivos de clima). "
                          "Fuente: PostgreSQL (vista sensores_unificados).", nota_total)

        self._tarjeta_kpi(col_2, "Sensores activos / inactivos", valor_estado,
                          "Un sensor está activo si tiene mediciones dentro del periodo del panel. "
                          "Fuente: PostgreSQL (función estado_sensores).", nota_estado)

        promedios = datos["promedios"]

        if isinstance(promedios, str):
            valor_med, nota_med = "Error", promedios
        elif self._kpi_tiene(promedios, "tipo", "mediciones"):
            # Un documento de clima trae temperatura, humedad y batería: se cuenta una sola vez (temperature)
            cont = int(promedios.loc[promedios["tipo"].isin(["pm25", "pm10", "o3"]), "mediciones"].sum())
            clima = int(promedios.loc[promedios["tipo"] == "temperature", "mediciones"].sum())
            valor_med = f"{cont + clima:,}"
            nota_med = f"contaminación: {cont:,} · clima: {clima:,}"
        else:
            valor_med, nota_med = self.KPI_SIN_DATOS, "No hay mediciones en el periodo."

        self._tarjeta_kpi(col_3, "Mediciones recibidas", valor_med,
                          "Mediciones registradas en el periodo: pm25 + pm10 + o3 + lecturas de clima "
                          "(cada lectura de clima cuenta una vez aunque traiga temperatura, humedad y batería). "
                          "Fuente: MongoDB.", nota_med)

        col_4, col_5, col_6 = st.columns(3)

        self._tarjeta_kpi(col_4, "Alertas generadas", valor_alertas,
                          "Es la misma cifra de «Alertas ambientales»: lecturas sobre el límite de referencia. "
                          "Fuente: MongoDB.", nota_alertas)

        ritmo = datos["ritmo"]
        ayuda_ritmo = (
            "Mediciones recibidas divididas entre los {unidad} en que hubo datos (no entre todo el periodo, "
            "para no diluir el ritmo real). Fuente: MongoDB."
        )

        for columna, titulo, campo, unidad in (
            (col_5, "Datos por día", "por_dia", "días"),
            (col_6, "Datos por hora", "por_hora", "horas"),
        ):
            with columna:
                st.markdown(f"**{titulo}**", help=ayuda_ritmo.format(unidad=unidad))

                if isinstance(ritmo, str):
                    st.caption(f"Error: {ritmo}")
                    continue

                interior = st.columns(2)

                for sub, fuente in zip(interior, ("contaminacion", "clima")):
                    fila = ritmo[ritmo["fuente"] == fuente] if self._kpi_tiene(ritmo, "fuente", campo) else None

                    with sub:
                        if fila is None or fila.empty:
                            st.metric(fuente.capitalize(), self.KPI_SIN_DATOS)
                        else:
                            st.metric(fuente.capitalize(), f"{fila.iloc[0][campo]:,.1f}")

    # ============================================================
    # VISTA 7: CONSULTAS OBLIGATORIAS DE POSTGRESQL
    # ============================================================

    # Opciones del formulario de la consulta 9 (la vista sensores_unificados).
    PG_FUENTES = ["Todas", "clima", "contaminacion"]
    PG_PARAMETROS = ["Todos", "o3", "pm10", "pm25", "temperature", "humidity", "battery"]
    PG_ORGANISMOS = ["Todos", "OpenAQ", "Victorian Government, Australia"]

    # Columnas (etiqueta, valor) para graficar el resultado de cada consulta.
    PG_EJES = {
        1: ("parametro", "sensores"),
        2: ("ubicacion", "sensores"),
        3: ("estado", "sensores"),
        4: ("organismo", "sensores"),
        5: ("ubicacion", "sensores"),
        6: ("anio", "sensores"),
        8: ("zona", None),   # el valor es el criterio elegido (sensores o accidentes)
        10: ("severity", "accidentes"),
    }

    def consultar_postgres(self, numero, fuente=None, parametro=None, organismo=None,
                           criterio="sensores"):
        """
        Ejecuta una consulta obligatoria de PostgreSQL (Queries_PG.CONSULTAS).

        La 5 usa la cantidad del ranking del panel; la 3, 7, 8 y 10 usan el rango
        de fechas del panel; la 8 también usa el máximo/mínimo del panel; la 10
        usa la zona si es un estado; la 9 recibe sus filtros.
        """

        if numero == 5:
            resultado = self.queries_pg.ejecutar(
                5, n=self.filtros.get_ranking_cantidad() or 10
            )

        elif numero == 9:
            resultado = self.queries_pg.ejecutar(
                9, fuente=fuente, parametro=parametro, organismo=organismo
            )

        elif numero in (3, 7, 8, 10):
            inicio, fin = self._rango_fechas()
            fechas = {
                "fecha_ini": inicio.strftime("%Y-%m-%d"),
                "fecha_fin": fin.strftime("%Y-%m-%d"),
            }

            if numero == 8:
                fechas.update(
                    n=self.filtros.get_ranking_cantidad() or 10,
                    criterio=criterio,
                    ascendente=not self.filtros.get_usar_maximo(),
                )
            elif numero == 10:
                fechas["estado"] = self._zona("accidentes")

            resultado = self.queries_pg.ejecutar(numero, **fechas)

        else:
            resultado = self.queries_pg.ejecutar(numero)

        return self._a_pandas(resultado)

    def mostrar_consultas_postgres(self):
        st.subheader("7. Consultas obligatorias de PostgreSQL")
        st.caption("Tecnología: PostgreSQL, consultado mediante Spark (JDBC).")

        consultas = self.queries_pg.CONSULTAS

        numero = st.selectbox(
            "Consulta",
            options=sorted(consultas),
            format_func=lambda n: f"{n}. {consultas[n][0]}",
            key="pg_consulta",
        )

        fuente = parametro = organismo = None
        criterio = "sensores"

        if numero == 9:
            col_1, col_2, col_3 = st.columns(3)
            fuente = col_1.selectbox("Fuente", self.PG_FUENTES, key="pg_fuente")
            parametro = col_2.selectbox("Parámetro", self.PG_PARAMETROS, key="pg_parametro")
            organismo = col_3.selectbox("Organismo", self.PG_ORGANISMOS, key="pg_organismo")

            fuente = None if fuente == "Todas" else fuente
            parametro = None if parametro == "Todos" else parametro
            organismo = None if organismo == "Todos" else organismo

        elif numero == 5:
            st.caption(
                "Usa la cantidad de ubicaciones del panel: "
                f"{self.filtros.get_ranking_cantidad()}."
            )

        elif numero in (3, 7, 8, 10):
            inicio, fin = self._rango_fechas()
            texto = f"Usa el rango del panel ({inicio:%d/%m/%Y} a {fin:%d/%m/%Y})"

            if numero == 8:
                criterio = st.selectbox(
                    "Ordenar zonas por",
                    options=list(self.queries_pg.CRITERIOS_RANKING),
                    key="pg_criterio_sel",
                )
                texto += (
                    f", {self.filtros.get_ranking_cantidad()} zonas y "
                    f"{'los valores máximos' if self.filtros.get_usar_maximo() else 'los valores mínimos'}"
                )
                st.caption(
                    texto + ". Los sensores de clima (Australia) se asignan a zonas de forma simulada."
                )

            elif numero == 10:
                zona = self._zona("accidentes")
                st.caption(texto + f" y {'el estado ' + zona if zona else 'todos los estados'}.")

            else:
                st.caption(texto + ". Activo = tiene mediciones dentro del rango.")

        if st.button("Ejecutar consulta", key="btn_pg"):
            actualizado = self._ejecutar_consulta(
                "pg_datos",
                lambda: self.consultar_postgres(
                    numero, fuente, parametro, organismo, criterio
                ),
            )

            if actualizado:
                st.session_state.pg_numero = numero
                st.session_state.pg_criterio = criterio

        datos = st.session_state.pg_datos

        if datos is None:
            return

        mostrada = st.session_state.pg_numero
        st.markdown(f"**Resultado de la consulta {mostrada}: {consultas[mostrada][0]}**")

        if datos.empty:
            st.info("La consulta no devolvió resultados.")
            return

        if mostrada in self.PG_EJES:
            etiqueta, valor = self.PG_EJES[mostrada]

            if valor is None:
                valor = st.session_state.pg_criterio

            grafico = datos[[etiqueta, valor]].copy()
            grafico[etiqueta] = grafico[etiqueta].astype(str)
            st.bar_chart(grafico.groupby(etiqueta, sort=False)[valor].sum())

        st.dataframe(datos, use_container_width=True, hide_index=True)

    # ============================================================
    # VISTA 8: CONSULTAS OBLIGATORIAS DE MONGODB
    # ============================================================

    MG_VARIABLES = ["temperature", "humidity", "pm25", "pm10", "o3"]
    MG_VARIABLES_CLIMA = ("temperature", "humidity")

    # número -> (columna de etiqueta, columna de valor, tipo de gráfico). La 3 y la 7 solo llevan tabla.
    MG_EJES = {
        1: ("fecha", "valor", "line"),
        2: ("fecha", "valor", "line"),
        4: ("etiqueta", "eventos", "bar"),
        5: ("zona", "alertas", "bar"),
        6: ("grupo", "promedio", "bar"),
        8: ("etiqueta", "mediciones", "bar"),
        9: ("condicion", "accidentes", "bar"),
        10: ("fecha", "promedio", "line"),
    }

    def _sensores_mongo(self, variable):
        """Sensores de la variable elegida: {sensor_id: etiqueta}, para los selectores."""

        try:
            if "mg_sensores" not in st.session_state:
                st.session_state.mg_sensores = self._a_pandas(
                    self.queries_pg.sensores_metadata()
                )

            meta = st.session_state.mg_sensores

            if variable in self.MG_VARIABLES_CLIMA:
                filtro = meta["fuente"] == "clima"
            else:
                filtro = (meta["fuente"] == "contaminacion") & (meta["parametro"] == variable)

            return {
                fila.sensor_id: f"{fila.nombre} ({fila.sensor_id})"
                for fila in meta[filtro].itertuples()
            }

        except Exception as error:
            st.error(f"No fue posible leer la lista de sensores: {error}")
            return {}

    def consultar_mongodb(self, numero, parametros):
        """
        Ejecuta una consulta obligatoria de MongoDB (Queries_MG.CONSULTAS).
        `parametros` trae lo elegido en la sección; el rango de fechas (y, para
        la 4 y la 5, el límite) se toman del panel en el momento de ejecutar.
        """

        parametros = dict(parametros)

        if numero != 1:
            inicio, fin = self._rango_fechas()
            parametros["fecha_ini"] = inicio.strftime("%Y-%m-%d")
            parametros["fecha_fin"] = fin.strftime("%Y-%m-%d")

        if numero in (4, 5):
            superior, limite = self.filtros.get_umbral()
            parametros["limite"] = limite
            parametros["superior"] = superior

        resultado = self.queries_mg.ejecutar(
            numero,
            sensores=self.queries_pg.sensores_metadata(),
            **parametros,
        )

        limite_filas = self.LIMITE_SERIE if numero == 10 else None

        return self._a_pandas(resultado, limite_filas)

    def mostrar_consultas_mongodb(self):
        st.subheader("8. Consultas obligatorias de MongoDB")
        st.caption("Tecnología: MongoDB (Aggregation Framework), consultado mediante Spark.")

        consultas = self.queries_mg.CONSULTAS

        numero = st.selectbox(
            "Consulta",
            options=sorted(consultas),
            format_func=lambda n: f"{n}. {consultas[n][0]}",
            key="mg_consulta",
        )

        parametros = {}

        if numero in (1, 2, 3, 4, 5, 6, 10):
            categoria = self.filtros.get_categoria()
            indice = self.MG_VARIABLES.index(categoria) if categoria in self.MG_VARIABLES else 0

            variable = st.selectbox(
                "Variable",
                options=self.MG_VARIABLES,
                index=indice,
                key="mg_variable_sel",
            )
            parametros["variable"] = variable

            if variable in self.MG_VARIABLES_CLIMA and numero in (5, 6):
                st.caption("En clima, la zona es una asignación simulada de los dispositivos de Australia.")

            sensores = self._sensores_mongo(variable)

            # Sensor: obligatorio en la 1, opcional en la 2 y la 10, y según el alcance en la 3.
            if numero == 3:
                alcance = st.selectbox(
                    "Alcance",
                    options=["Cada sensor", "Todos juntos", "Un sensor"],
                    key="mg_alcance",
                )
                parametros["por_sensor"] = alcance == "Cada sensor"

            if numero == 1 or (numero == 3 and alcance == "Un sensor"):
                if sensores:
                    parametros["sensor_id"] = st.selectbox(
                        "Sensor",
                        options=list(sensores),
                        format_func=sensores.get,
                        key=f"mg_sensor_{variable}",
                    )
                else:
                    st.info("No hay sensores para esta variable.")

            elif numero in (2, 10):
                eleccion = st.selectbox(
                    "Sensor",
                    options=["Todos"] + list(sensores),
                    format_func=lambda s: "Todos" if s == "Todos" else sensores[s],
                    key=f"mg_sensor_opcional_{variable}",
                )
                if eleccion != "Todos":
                    parametros["sensor_id"] = eleccion

        if numero in (1, 2, 9):
            parametros["n"] = int(
                st.number_input(
                    "Cantidad de filas",
                    min_value=1,
                    max_value=500,
                    value=15 if numero == 9 else 20,
                    step=1,
                    key=f"mg_n_{numero}",
                )
            )

        if numero == 2 and st.checkbox("Filtrar por rango de valor", key="mg_filtrar_valor"):
            col_1, col_2 = st.columns(2)
            parametros["valor_min"] = col_1.number_input("Valor mínimo", value=0.0, key="mg_valor_min")
            parametros["valor_max"] = col_2.number_input("Valor máximo", value=100.0, key="mg_valor_max")

        if numero == 6:
            parametros["por"] = st.selectbox(
                "Agrupar por",
                options=["zona", "ubicacion"],
                key="mg_por",
            )

        if numero == 10:
            parametros["granularidad"] = st.selectbox(
                "Granularidad",
                options=["dia", "mes"],
                key="mg_granularidad",
            )

        # Qué toma la consulta del panel
        if numero != 1:
            inicio, fin = self._rango_fechas()
            texto = f"Usa el rango del panel ({inicio:%d/%m/%Y} a {fin:%d/%m/%Y})"

            if numero in (4, 5):
                superior, limite = self.filtros.get_umbral()
                texto += f" y el límite del panel ({'≥' if superior else '≤'} {limite})"

            if numero == 5:
                texto += ". Las alertas se calculan al vuelo con ese límite; no se guardan"

            st.caption(texto + ".")

        if st.button("Ejecutar consulta", key="btn_mg"):
            actualizado = self._ejecutar_consulta(
                "mg_datos",
                lambda: self.consultar_mongodb(numero, parametros),
            )

            if actualizado:
                st.session_state.mg_numero = numero

        datos = st.session_state.mg_datos

        if datos is None:
            return

        mostrada = st.session_state.mg_numero
        st.markdown(f"**Resultado de la consulta {mostrada}: {consultas[mostrada][0]}**")

        if datos.empty:
            st.info("La consulta no devolvió resultados.")
            return

        if mostrada in self.MG_EJES:
            etiqueta, valor, tipo = self.MG_EJES[mostrada]

            if etiqueta in datos.columns and valor in datos.columns:
                grafico = datos[[etiqueta, valor]].copy()
                grafico[etiqueta] = grafico[etiqueta].astype(str)
                grafico = grafico.set_index(etiqueta)

                if tipo == "line":
                    st.line_chart(grafico)
                else:
                    st.bar_chart(grafico)

        st.dataframe(datos, use_container_width=True, hide_index=True)

    # ============================================================
    # VISTA 9: CONSULTAS OBLIGATORIAS DE SPARK
    # ============================================================

    # número -> (columna X, columna Y, tipo de gráfico) de cada gráfico por dataset.
    # La 3 y la 10 tienen un tratamiento propio (ver mostrar_consultas_spark).
    SP_EJES = {
        1: ("nombre", "mediciones", "bar"),
        2: ("ubicacion", "total_registros", "bar"),
        5: ("zona", "total_actividad", "bar"),
        6: ("periodo", "total_registros", "line"),
        7: ("periodo", "total_registros", "line"),
        8: ("categoria", "total_registros", "bar"),
        9: ("anio", "total_registros", "line"),
    }

    def consultar_spark(self, numero, parametros):
        """
        Ejecuta una consulta obligatoria de Spark (Queries_SP.CONSULTAS).
        `parametros` trae lo elegido en la sección (top n, umbral z); el rango
        de fechas se toma del panel en el momento de ejecutar.
        """

        parametros = dict(parametros)
        inicio, fin = self._rango_fechas()
        parametros["fecha_ini"] = inicio.strftime("%Y-%m-%d")
        parametros["fecha_fin"] = fin.strftime("%Y-%m-%d")

        resultado = self.queries.ejecutar(numero, **parametros)

        return self._a_pandas(resultado, self.LIMITE_SERIE)

    def _grafico_por_dataset(self, datos, eje_x, eje_y, tipo):
        """
        Un gráfico por dataset (accidentes, contaminación, clima), en columnas.
        Se separan porque sus magnitudes son muy distintas (millones frente a miles)
        y en un solo gráfico unos aplastarían a los otros.
        """

        datasets = list(datos["dataset"].dropna().unique())

        if not datasets:
            return

        columnas = st.columns(len(datasets))

        for columna, dataset in zip(columnas, datasets):
            parte = datos[datos["dataset"] == dataset][[eje_x, eje_y]].copy()
            parte[eje_x] = parte[eje_x].astype(str)
            parte = parte.set_index(eje_x)

            with columna:
                st.caption(dataset)

                if tipo == "line":
                    st.line_chart(parte)
                else:
                    st.bar_chart(parte)

    def mostrar_consultas_spark(self):
        st.subheader("9. Consultas obligatorias de Spark")
        st.caption("Tecnología: Apache Spark sobre archivos Parquet en HDFS (capa processed).")

        consultas = self.queries.CONSULTAS

        numero = st.selectbox(
            "Consulta",
            options=sorted(consultas),
            format_func=lambda n: f"{n}. {consultas[n][0]}",
            key="sp_consulta",
        )

        parametros = {}

        if numero == 1:
            parametros["n"] = st.number_input(
                "Cantidad de sensores (top)", min_value=1, max_value=50, value=10, key="sp_n_1"
            )
        elif numero in (2, 5):
            parametros["top_n"] = st.number_input(
                "Cantidad de resultados (top)", min_value=1, max_value=50, value=15, key=f"sp_n_{numero}"
            )
        elif numero == 7:
            parametros["umbral_z"] = st.number_input(
                "Umbral del z-score (|z| mayor o igual se marca como anomalía)",
                min_value=0.5, max_value=5.0, value=1.5, step=0.25, key="sp_umbral_z",
            )

        try:
            inicio, fin = self._rango_fechas()
            st.caption(f"Usa el rango del panel ({inicio:%d/%m/%Y} a {fin:%d/%m/%Y}).")
        except ValueError as error:
            st.warning(str(error))

        if st.button("Ejecutar consulta", key="btn_sp"):
            actualizado = self._ejecutar_consulta(
                "sp_datos",
                lambda: self.consultar_spark(numero, parametros),
            )

            if actualizado:
                st.session_state.sp_numero = numero

        datos = st.session_state.sp_datos

        if datos is None:
            return

        mostrada = st.session_state.sp_numero
        st.markdown(f"**Resultado de la consulta {mostrada}: {consultas[mostrada][0]}**")

        if datos.empty:
            st.info("La consulta no devolvió resultados.")
            return

        if mostrada == 3:
            # Un indicador por dataset: temperatura (°C), contaminación (valor medio) y severidad.
            datos = datos.copy()
            datos["serie"] = datos["dataset"] + " - " + datos["indicador"]
            serie = datos.pivot_table(index="periodo", columns="serie", values="valor_promedio")
            st.line_chart(serie)

        elif mostrada == 4:
            primera = datos.iloc[0]
            col_1, col_2 = st.columns(2)
            col_1.metric("Correlación accidentes vs. contaminación", f"{primera['corr_acc_vs_cont']}")
            col_2.metric("Correlación severidad vs. contaminación", f"{primera['corr_sev_vs_cont']}")
            st.caption(
                "Coeficiente de Pearson sobre los meses en común (cerca de 0 = sin relación lineal; "
                "±1 = relación perfecta). Una correlación no implica causalidad."
            )
            col_1, col_2 = st.columns(2)
            col_1.caption("Accidentes por mes")
            col_1.line_chart(datos.set_index("periodo")["total_accidentes"])
            col_2.caption("Contaminación promedio por mes")
            col_2.line_chart(datos.set_index("periodo")["contaminacion_prom"])

        elif mostrada == 7:
            anomalias = int((datos["estado"] == "ANOMALIA").sum())
            st.metric("Meses marcados como anomalía", anomalias)

            if st.checkbox("Mostrar solo anomalías", key="sp_solo_anomalias"):
                datos = datos[datos["estado"] == "ANOMALIA"]

            self._grafico_por_dataset(datos, "periodo", "total_registros", "line")

        elif mostrada == 10:
            columnas = st.columns(len(datos))

            for columna, fila in zip(columnas, datos.itertuples()):
                columna.metric(f"{fila.dataset}: {fila.metrica}".replace("_", " "), f"{fila.valor:,}")

        elif mostrada in self.SP_EJES:
            eje_x, eje_y, tipo = self.SP_EJES[mostrada]
            self._grafico_por_dataset(datos, eje_x, eje_y, tipo)

            if mostrada == 2:
                mapa = datos.dropna(subset=["lat_promedio", "lng_promedio"]).rename(
                    columns={"lat_promedio": "lat", "lng_promedio": "lon"}
                )
                st.map(mapa[["lat", "lon"]])

        st.dataframe(datos, use_container_width=True, hide_index=True)

    # ============================================================
    # VISTA 10: PREGUNTAS DE HADOOP (MAPREDUCE) Y OPERACIONES DE HDFS
    # ============================================================

    HD_CARPETA_DEMO = "/tmp/demo_hdfs"
    HD_ARCHIVO_DEMO = "/datasets/rising-hf.csv"   # ruta vista desde el contenedor namenode
    HD_COPIA_LOCAL = "/tmp/rising_hf_copia.csv"
    HD_OPERACIONES = ["-mkdir", "-put", "-ls", "-cat", "-get", "-rm"]

    def consultar_hadoop(self, numero, parametros):
        """
        Ejecuta una pregunta de Hadoop (Queries_HDFS.CONSULTAS) con MapReduce.
        El rango de fechas se toma del panel en el momento de ejecutar.
        """

        parametros = dict(parametros)
        inicio, fin = self._rango_fechas()
        parametros["fecha_ini"] = inicio.strftime("%Y-%m-%d")
        parametros["fecha_fin"] = fin.strftime("%Y-%m-%d")

        return self._a_pandas(self.queries_hdfs.ejecutar(numero, **parametros))

    def _operacion_hdfs(self, operacion):
        """Ejecuta una operación de HDFS sobre una carpeta de prueba y devuelve (comando, resultado)."""

        h = self.queries_hdfs
        demo = self.HD_CARPETA_DEMO

        if operacion == "-mkdir":
            h.crear_carpeta(demo)
            return f"hdfs dfs -mkdir -p {demo}", f"Carpeta {demo} creada."

        if operacion == "-put":
            h.subir(self.HD_ARCHIVO_DEMO, f"{demo}/")
            return f"hdfs dfs -put -f {self.HD_ARCHIVO_DEMO} {demo}/", "Archivo subido a HDFS."

        if operacion == "-ls":
            return f"hdfs dfs -ls {demo}", self._a_pandas(h.listar(demo))

        if operacion == "-cat":
            archivo = f"{demo}/rising-hf.csv"
            return f"hdfs dfs -cat {archivo}  (primeras 5 líneas)", self._a_pandas(h.leer_archivo(archivo, 5))

        if operacion == "-get":
            h.descargar(f"{demo}/rising-hf.csv", self.HD_COPIA_LOCAL)
            return (
                f"hdfs dfs -get {demo}/rising-hf.csv {self.HD_COPIA_LOCAL}",
                f"Copia descargada en {self.HD_COPIA_LOCAL} (dentro del contenedor namenode).",
            )

        h.eliminar(demo, recursivo=True)
        return f"hdfs dfs -rm -r {demo}", "Carpeta de prueba eliminada."

    def mostrar_consultas_hadoop(self):
        st.subheader("10. Preguntas de Hadoop (MapReduce) y operaciones de HDFS")
        st.caption(
            "Tecnología: Hadoop (HDFS + MapReduce con Hadoop Streaming). Los jobs leen texto limpio "
            "generado desde los Parquet de la capa processed, los mismos datos que usan PostgreSQL y MongoDB."
        )

        h = self.queries_hdfs

        try:
            listo = h.texto_listo()
        except Exception as error:
            st.error(f"No fue posible consultar HDFS: {error}")
            return

        if not listo:
            st.warning("Falta el texto limpio que lee MapReduce (/data/mapreduce).")

            if st.button("Preparar texto limpio en HDFS", key="btn_hd_preparar"):
                with st.spinner("Exportando desde los Parquet..."):
                    try:
                        h.preparar_texto()
                        st.rerun()
                    except Exception as error:
                        st.error(f"No fue posible preparar el texto: {error}")
            return

        consultas = h.CONSULTAS

        numero = st.selectbox(
            "Pregunta",
            options=sorted(consultas),
            format_func=lambda n: f"{n}. {consultas[n][0]}",
            key="hd_consulta",
        )

        parametros = {}

        if numero == 2:
            parametros["n"] = st.number_input(
                "Cantidad de sensores (top)", min_value=1, max_value=30, value=5, key="hd_n_2"
            )
        elif numero in (1, 5):
            fuente = st.selectbox(
                "Fuente",
                options=["Todas", "contaminacion", "clima"],
                key=f"hd_fuente_{numero}",
            )

            if fuente != "Todas":
                parametros["fuente"] = fuente

        try:
            inicio, fin = self._rango_fechas()
            st.caption(
                f"Usa el rango del panel ({inicio:%d/%m/%Y} a {fin:%d/%m/%Y}). "
                "Cada pregunta lanza un job MapReduce y puede tardar de 10 a 30 segundos."
            )
        except ValueError as error:
            st.warning(str(error))

        if st.button("Ejecutar pregunta", key="btn_hd"):
            with st.spinner("Ejecutando job MapReduce..."):
                actualizado = self._ejecutar_consulta(
                    "hd_datos",
                    lambda: self.consultar_hadoop(numero, parametros),
                )

            if actualizado:
                st.session_state.hd_numero = numero

        datos = st.session_state.hd_datos

        if datos is not None:
            mostrada = st.session_state.hd_numero
            st.markdown(f"**Resultado de la pregunta {mostrada}: {consultas[mostrada][0]}**")

            if datos.empty:
                st.info("La consulta no devolvió resultados.")
            else:
                if mostrada in (1, 2, 5):
                    grafico = datos.head(15)[["sensor_id", "mediciones"]].copy()
                    grafico["sensor_id"] = grafico["sensor_id"].astype(str)
                    st.bar_chart(grafico.set_index("sensor_id"))

                elif mostrada == 3:
                    total = datos[datos["sensor_id"] == "TODOS"]

                    if not total.empty:
                        st.metric("Temperatura promedio histórica (°C)", f"{total.iloc[0]['promedio_c']}")

                    por_sensor = datos[datos["sensor_id"] != "TODOS"][["sensor_id", "promedio_c"]]
                    st.bar_chart(por_sensor.set_index("sensor_id"))

                elif mostrada == 4:
                    self._grafico_por_dataset(datos, "zona", "eventos", "bar")

                st.dataframe(datos, use_container_width=True, hide_index=True)

        st.markdown("**Operaciones de HDFS** (sobre una carpeta de prueba, no tocan los datos del proyecto)")
        st.caption(
            f"Orden sugerido: -mkdir, -put, -ls, -cat, -get, -rm. El archivo de prueba es {self.HD_ARCHIVO_DEMO}."
        )

        operacion = st.selectbox("Operación", options=self.HD_OPERACIONES, key="hd_operacion")

        if st.button("Ejecutar operación", key="btn_hd_op"):
            try:
                st.session_state.hd_op = self._operacion_hdfs(operacion)
            except Exception as error:
                st.error(f"La operación falló: {error}")

        if st.session_state.hd_op is not None:
            comando, resultado = st.session_state.hd_op
            st.code(comando, language="bash")

            if isinstance(resultado, str):
                st.success(resultado)
            else:
                st.dataframe(resultado, use_container_width=True, hide_index=True)

        st.markdown("**Distribución en bloques y datanodes**")
        ruta = st.text_input(
            "Archivo o carpeta de HDFS",
            value="/data/raw/accidentes/US_Accidents_March23.csv",
            key="hd_ruta_bloques",
        )

        if st.button("Ver bloques y estado del clúster", key="btn_hd_bloques"):
            try:
                st.session_state.hd_bloques = (
                    self._a_pandas(h.bloques(ruta)),
                    self._a_pandas(h.resumen_cluster()),
                )
            except Exception as error:
                st.error(f"No fue posible leer los bloques: {error}")

        if st.session_state.hd_bloques is not None:
            bloques, cluster = st.session_state.hd_bloques
            st.dataframe(cluster, use_container_width=True, hide_index=True)
            st.write(f"Bloques del archivo: {len(bloques)}")
            st.dataframe(bloques, use_container_width=True, hide_index=True)
            st.caption(
                "HDFS divide cada archivo en bloques de 128 MB y guarda cada bloque en un datanode; con más "
                "datanodes y replicación mayor a 1, cada bloque tendría copias en nodos distintos y el archivo "
                "seguiría disponible si uno falla. Este clúster tiene un solo datanode y replicación 1, "
                "por lo que todos los bloques aparecen en el mismo nodo."
            )

    # ============================================================
    # ACTUALIZAR TODAS LAS VISTAS
    # ============================================================

    def actualizar_todas(self):
        """
        Actualiza las vistas 1, 2, 5 y 6.

        La tendencia 1 y la tendencia 2 se excluyen expresamente.
        """

        st.session_state.error_vista_1 = None
        st.session_state.error_vista_2 = None
        st.session_state.error_vista_5 = None
        st.session_state.error_vista_6 = None

        consultas = [
            ("vista_1_datos", self.consultar_vista_1),
            ("vista_2_datos", self.consultar_vista_2),
            ("vista_5_datos", self.consultar_vista_5),
        ]

        for clave, funcion in consultas:
            self._ejecutar_consulta(clave, funcion)

        try:
            datos, total = self.consultar_vista_6()
            st.session_state.vista_6_datos = datos
            st.session_state.vista_6_total = total
        except Exception as error:
            st.error(f"Error al actualizar eventos críticos: {error}")

    # ============================================================
    # PANEL DE FILTROS
    # ============================================================

    def mostrar_filtros(self):
        st.sidebar.header("Filtros globales")

        # Zona: solo se ofrecen las zonas del dataset de la categoría elegida
        
        categoria_zonas = st.session_state.get(
            "filtro_categoria",
            self.filtros.categorias_validas[0],
        )

        zonas = self._zonas_de_categoria(categoria_zonas)

        zona_actual = self.filtros.get_zona()

        opciones_zona = ["Todas"] + list(zonas)

        # Si la zona guardada ya no es válida para la categoría, vuelve a "Todas".
        if st.session_state.get("filtro_zona", "Todas") not in opciones_zona:
            st.session_state["filtro_zona"] = "Todas"

        if zona_actual not in zonas:
            zona_actual = "Todas"

        zona = st.sidebar.selectbox(
            "Zona",
            options=opciones_zona,
            index=opciones_zona.index(
                zona_actual if zona_actual is not None else "Todas"
            ),
            key="filtro_zona",
        )

        self.filtros.set_zona(
            None if zona == "Todas" else zona
        )



        # Fecha
        fecha_actual = self.filtros.get_fecha()

        # Normalizar fecha mínima
        fecha_minima = self.FECHA_MINIMA

        if isinstance(fecha_minima, datetime):
            fecha_minima = fecha_minima.date()

        # Fecha máxima
        fecha_maxima = self.FECHA_MAXIMA

        # Normalizar fechas actuales
        if (
            isinstance(fecha_actual, tuple)
            and len(fecha_actual) == 2
            and fecha_actual[0] is not None
            and fecha_actual[1] is not None
        ):
            fecha_inicio, fecha_fin = fecha_actual

            if isinstance(fecha_inicio, datetime):
                fecha_inicio = fecha_inicio.date()

            if isinstance(fecha_fin, datetime):
                fecha_fin = fecha_fin.date()

            rango_actual = (fecha_inicio, fecha_fin)

        else:
            rango_actual = (fecha_minima, fecha_maxima)

        # Asegurar que el rango esté dentro de los límites
        rango_actual = (
            max(fecha_minima, rango_actual[0]),
            min(fecha_maxima, rango_actual[1]),
        )

        rango = st.sidebar.slider(
            "Rango de fechas",
            min_value=fecha_minima,
            max_value=fecha_maxima,
            value=rango_actual,
            format="DD/MM/YYYY",
            key="filtro_fechas",
        )

        self.filtros.set_fecha(rango)



        # Origen
        origenes = self.filtros.origenes_validos

        origen_actual = self.filtros.get_origen()

        opciones_origen = ["Todos"] + list(origenes)

        if origen_actual not in origenes:
            origen_actual = "Todos"

        origen = st.sidebar.selectbox(
            "Origen de datos",
            options=opciones_origen,
            index=opciones_origen.index(
                origen_actual if origen_actual is not None else "Todos"
            ),
            key="filtro_origen",
        )

        self.filtros.set_origen(
            None if origen == "Todos" else origen
        )

        # Categoría
        categorias = self.filtros.categorias_validas

        categoria_actual = self.filtros.get_categoria()

        if categoria_actual not in categorias:
            categoria_actual = categorias[0]

        categoria = st.sidebar.selectbox(
            "Categoría",
            options=categorias,
            index=categorias.index(categoria_actual),
            key="filtro_categoria",
        )

        self.filtros.set_categoria(categoria)

        # Cantidad del ranking
        ranking_actual = self.filtros.get_ranking_cantidad()

        if ranking_actual is None:
            ranking_actual = 10

        ranking = st.sidebar.number_input(
            "Cantidad de ubicaciones en el ranking",
            min_value=1,
            max_value=max(1, self.filtros.max_ranking - 1),
            value=min(
                ranking_actual,
                self.filtros.max_ranking - 1,
            ),
            step=1,
            key="filtro_ranking",
        )

        self.filtros.set_ranking_cantidad(int(ranking))

        # Máximo o mínimo
        usar_maximo = st.sidebar.checkbox(
            "Mostrar valores máximos",
            value=self.filtros.get_usar_maximo(),
            key="filtro_maximo",
        )

        self.filtros.set_usar_maximo(usar_maximo)

        # Umbral
        st.sidebar.subheader("Umbral de eventos críticos")

        activar = st.sidebar.checkbox(
            "Buscar valores superiores o iguales al umbral",
            value=self.filtros.get_umbral()[0],
            key="filtro_umbral_direccion",
        )

        numero = st.sidebar.number_input(
            "Valor del umbral",
            value=float(self.filtros.get_umbral()[1]),
            key="filtro_umbral_valor",
        )

        self.filtros.set_umbral((activar, numero))


    # ============================================================
    # EJECUCIÓN DEL DASHBOARD
    # ============================================================

    def run(self):
        st.set_page_config(
            page_title=self.titulo,
            
            layout="wide",
        )

        self._inicializar_estado()

        st.title(f"{self.titulo}")

        self.mostrar_filtros()

        if st.button(
            "Actualizar todas las vistas",
            type="primary",
            key="btn_actualizar_todas",
        ):
            self.actualizar_todas()

        st.divider()

        self.mostrar_kpis()

        st.divider()

        self.mostrar_vista_1()

        st.divider()

        self.mostrar_vista_2()

        st.divider()

        self.mostrar_tendencia_1()

        st.divider()

        self.mostrar_tendencia_2()

        st.divider()

        self.mostrar_vista_5()

        st.divider()

        self.mostrar_vista_6()

        st.divider()

        self.mostrar_consultas_postgres()

        st.divider()

        self.mostrar_consultas_mongodb()

        st.divider()

        self.mostrar_consultas_spark()

        st.divider()

        self.mostrar_consultas_hadoop()
