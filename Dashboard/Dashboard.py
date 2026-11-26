
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
        titulo="Dashboard de SmartCities (pruebas)",
        icono="🧪",
    ):
        self.queries = queries
        self.queries_pg = queries_pg
        self.queries_mg = queries_mg
        self.queries_hdfs = queries_hdfs
        self.filtros = filtros

        self.titulo = titulo
        self.icono = icono

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
            page_icon=self.icono,
            layout="wide",
        )

        self._inicializar_estado()

        st.title(f"{self.icono} {self.titulo}")

        self.mostrar_filtros()

        if st.button(
            "Actualizar todas las vistas",
            type="primary",
            key="btn_actualizar_todas",
        ):
            self.actualizar_todas()

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
