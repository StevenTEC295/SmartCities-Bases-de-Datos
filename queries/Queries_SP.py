from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


class Queries_SP:
    """
    Consultas con Spark sobre los archivos Parquet de HDFS (/data/processed).
    Todos los métodos devuelven DataFrames de Spark.
    """

    def __init__(self, spark: SparkSession, ruta: str):
        self.spark = spark
        self.ruta = ruta.rstrip("/")   # ej: hdfs://namenode:9000/data/processed

    def probar(self) -> None:
        """Comprueba la conexión: imprime los números 0 al 4 (la salida sale en la consola)."""
        self.spark.range(5).show()

    def _leer(self, subruta: str) -> DataFrame:
        return self.spark.read.parquet(f"{self.ruta}/{subruta}")

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
    def sensores_mas_activos(self, n: int = 10) -> DataFrame:
        clima_med = self._leer("clima/mediciones")
        clima_sen = self._leer("clima/sensores").select("dev_id", "name")

        clima_activos = (
            clima_med.filter(F.col("dev_id").isNotNull())
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

        cont_med = self._leer("contaminacion/mediciones")
        cont_sen = self._leer("contaminacion/sensores").select("sensor_id", "location_id")

        cont_activos = (
            cont_med.filter(F.col("sensor_id").isNotNull())
            .groupBy("sensor_id")
            .agg(F.count("*").alias("mediciones"))
            .join(cont_sen, on="sensor_id", how="left")
            .select(
                F.lit("contaminacion").alias("dataset"),
                F.col("sensor_id").cast("string").alias("sensor_id"),
                F.coalesce(
                    F.concat(F.lit("Location "), F.col("location_id").cast("string")),
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
    def distribucion_geografica_eventos(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
        top_n: int = 15,
    ) -> DataFrame:
        df_acc = self.spark.read.parquet(ruta_accidentes)
        acc_geo = (
            df_acc.filter(F.col("State").isNotNull() & F.col("City").isNotNull())
            .groupBy("State", "City")
            .agg(
                F.count("*").alias("total_registros"),
                F.round(F.avg(F.col("Start_Lat").cast("double")), 4).alias("lat_promedio"),
                F.round(F.avg(F.col("Start_Lng").cast("double")), 4).alias("lng_promedio"),
            )
            .withColumn("dataset", F.lit("accidentes"))
            .withColumn("ubicacion", F.concat_ws(" - ", F.col("State"), F.col("City")))
            .select("dataset", "ubicacion", "total_registros", "lat_promedio", "lng_promedio")
            .orderBy(F.desc("total_registros"))
            .limit(int(top_n))
        )

        cont_sen = self._leer("contaminacion/sensores")
        cont_geo = (
            cont_sen.filter(F.col("location_id").isNotNull())
            .groupBy("location_id")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("contaminacion"))
            .withColumn("ubicacion", F.concat(F.lit("Location "), F.col("location_id").cast("string")))
            .withColumn("lat_promedio", F.lit(None).cast("double"))
            .withColumn("lng_promedio", F.lit(None).cast("double"))
            .select("dataset", "ubicacion", "total_registros", "lat_promedio", "lng_promedio")
            .orderBy(F.desc("total_registros"))
            .limit(int(top_n))
        )

        return acc_geo.unionByName(cont_geo)

    # 3. Evolucion temporal de indicadores urbanos (clima + contaminacion + accidentes)
    def evolucion_temporal_indicadores(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
    ) -> DataFrame:
        clima_agg = (
            self._leer("clima/mediciones")
            .withColumn("periodo", F.date_format(F.col("time").cast("timestamp"), "yyyy-MM"))
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
            self._leer("contaminacion/mediciones")
            .withColumn("periodo", F.date_format(F.col("datetime_utc").cast("timestamp"), "yyyy-MM"))
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
            self.spark.read.parquet(ruta_accidentes)
            .withColumn("periodo", F.date_format(F.col("Start_Time").cast("timestamp"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.count("*").alias("total_registros"),
                F.round(F.avg(F.col("Severity").cast("double")), 2).alias("severidad_promedio"),
            )
            .withColumnRenamed("severidad_promedio", "valor_promedio")
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
    def correlacion_variables(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
    ) -> DataFrame:
        acc_mes = (
            self.spark.read.parquet(ruta_accidentes)
            .withColumn("periodo", F.date_format(F.col("Start_Time").cast("timestamp"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.count("*").cast("double").alias("total_accidentes"),
                F.round(F.avg(F.col("Severity").cast("double")), 4).alias("severidad_prom_acc"),
            )
        )

        cont_mes = (
            self._leer("contaminacion/mediciones")
            .withColumn("periodo", F.date_format(F.col("datetime_utc").cast("timestamp"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(
                F.round(F.avg(F.col("value").cast("double")), 4).alias("contaminacion_prom"),
                F.count("*").cast("double").alias("total_mediciones_cont"),
            )
        )

        w = Window.orderBy("periodo")
        return (
            acc_mes.join(cont_mes, on="periodo", how="inner")
            .withColumn("corr_acc_vs_cont", F.round(F.corr("total_accidentes", "contaminacion_prom").over(w), 4))
            .withColumn("corr_sev_vs_cont", F.round(F.corr("severidad_prom_acc", "contaminacion_prom").over(w), 4))
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
    def ranking_zonas_actividad(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
        top_n: int = 15,
    ) -> DataFrame:
        acc_zonas = (
            self.spark.read.parquet(ruta_accidentes)
            .filter(F.col("State").isNotNull() & F.col("City").isNotNull())
            .groupBy("State", "City")
            .agg(F.count("*").alias("total_actividad"))
            .withColumn("dataset", F.lit("accidentes"))
            .withColumn("zona", F.concat_ws(" - ", F.col("State"), F.col("City")))
            .select("dataset", "zona", "total_actividad")
        )

        cont_med = self._leer("contaminacion/mediciones")
        cont_sen = self._leer("contaminacion/sensores").select("sensor_id", "location_id")
        cont_zonas = (
            cont_med.filter(F.col("sensor_id").isNotNull())
            .join(cont_sen.filter(F.col("location_id").isNotNull()), on="sensor_id", how="inner")
            .groupBy("location_id")
            .agg(F.count("*").alias("total_actividad"))
            .withColumn("dataset", F.lit("contaminacion"))
            .withColumn("zona", F.concat(F.lit("Location "), F.col("location_id").cast("string")))
            .select("dataset", "zona", "total_actividad")
        )

        w = Window.partitionBy("dataset").orderBy(F.desc("total_actividad"), F.asc("zona"))
        return (
            acc_zonas.unionByName(cont_zonas)
            .withColumn("ranking", F.row_number().over(w))
            .filter(F.col("ranking") <= int(top_n))
            .select("dataset", "ranking", "zona", "total_actividad")
            .orderBy(F.asc("dataset"), F.asc("ranking"))
        )

    # 6. Tendencias por periodo (accidentes + contaminacion)
    def tendencias_por_periodo(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
    ) -> DataFrame:
        acc_mes = (
            self.spark.read.parquet(ruta_accidentes)
            .withColumn("periodo", F.date_format(F.col("Start_Time").cast("timestamp"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("accidentes"))
        )

        cont_mes = (
            self._leer("contaminacion/mediciones")
            .withColumn("periodo", F.date_format(F.col("datetime_utc").cast("timestamp"), "yyyy-MM"))
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
    def deteccion_anomalias(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
        umbral_z: float = 1.5,
    ) -> DataFrame:
        acc_mes = (
            self.spark.read.parquet(ruta_accidentes)
            .withColumn("periodo", F.date_format(F.col("Start_Time").cast("timestamp"), "yyyy-MM"))
            .filter(F.col("periodo").isNotNull())
            .groupBy("periodo")
            .agg(F.count("*").cast("double").alias("total_registros"))
            .withColumn("dataset", F.lit("accidentes"))
        )

        cont_mes = (
            self._leer("contaminacion/mediciones")
            .withColumn("periodo", F.date_format(F.col("datetime_utc").cast("timestamp"), "yyyy-MM"))
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
    def agrupamiento_por_categorias(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
    ) -> DataFrame:
        df_acc = self.spark.read.parquet(ruta_accidentes).filter(F.col("Severity").isNotNull())
        total_acc = df_acc.count()
        acc_cat = (
            df_acc.groupBy("Severity")
            .agg(
                F.lit("accidentes").alias("dataset"),
                F.count("*").alias("total_registros"),
                F.round((F.count("*").cast("double") / F.lit(float(total_acc))) * 100.0, 2).alias("porcentaje"),
                F.lit(None).cast("double").alias("valor_promedio"),
            )
            .withColumnRenamed("Severity", "categoria")
            .withColumn("categoria", F.col("categoria").cast("string"))
        )

        df_cont = self._leer("contaminacion/mediciones").filter(F.col("sensor_id").isNotNull())
        total_cont = df_cont.count()
        cont_cat = (
            df_cont.groupBy("sensor_id")
            .agg(
                F.lit("contaminacion").alias("dataset"),
                F.count("*").alias("total_registros"),
                F.round((F.count("*").cast("double") / F.lit(float(total_cont))) * 100.0, 2).alias("porcentaje"),
                F.round(F.avg(F.col("value").cast("double")), 2).alias("valor_promedio"),
            )
            .withColumnRenamed("sensor_id", "categoria")
            .withColumn("categoria", F.col("categoria").cast("string"))
        )

        df_clima = self._leer("clima/mediciones").filter(F.col("dev_id").isNotNull())
        total_clima = df_clima.count()
        clima_cat = (
            df_clima.groupBy("dev_id")
            .agg(
                F.lit("clima").alias("dataset"),
                F.count("*").alias("total_registros"),
                F.round((F.count("*").cast("double") / F.lit(float(total_clima))) * 100.0, 2).alias("porcentaje"),
                F.round(F.avg(F.col("temperature").cast("double")), 2).alias("valor_promedio"),
            )
            .withColumnRenamed("dev_id", "categoria")
            .withColumn("categoria", F.col("categoria").cast("string"))
        )

        return (
            acc_cat.unionByName(cont_cat)
            .unionByName(clima_cat)
            .select("dataset", "categoria", "total_registros", "porcentaje", "valor_promedio")
            .orderBy(F.asc("dataset"), F.desc("total_registros"))
        )

    # 9. Comparacion historica entre anios (accidentes + contaminacion + clima)
    def comparacion_historica_anios(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
    ) -> DataFrame:
        acc_anual = (
            self.spark.read.parquet(ruta_accidentes)
            .withColumn("anio", F.year(F.col("Start_Time").cast("timestamp")).cast("int"))
            .filter(F.col("anio").isNotNull())
            .groupBy("anio")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("accidentes"))
        )

        cont_anual = (
            self._leer("contaminacion/mediciones")
            .withColumn("anio", F.year(F.col("datetime_utc").cast("timestamp")).cast("int"))
            .filter(F.col("anio").isNotNull())
            .groupBy("anio")
            .agg(F.count("*").alias("total_registros"))
            .withColumn("dataset", F.lit("contaminacion"))
        )

        clima_anual = (
            self._leer("clima/mediciones")
            .withColumn("anio", F.year(F.col("time").cast("timestamp")).cast("int"))
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
    def dashboard_reporte_consolidado(
        self,
        ruta_accidentes: str = "hdfs://namenode:9000/data/staging/accidentes",
    ) -> DataFrame:
        total_acc = self.spark.read.parquet(ruta_accidentes).count()
        total_cont_med = self._leer("contaminacion/mediciones").count()
        total_cont_sen = self._leer("contaminacion/sensores").count()
        total_clima_med = self._leer("clima/mediciones").count()
        total_clima_sen = self._leer("clima/sensores").count()

        filas = [
            ("accidentes", "eventos_totales", int(total_acc), ruta_accidentes),
            ("contaminacion", "mediciones_totales", int(total_cont_med), f"{self.ruta}/contaminacion/mediciones"),
            ("contaminacion", "sensores_registrados", int(total_cont_sen), f"{self.ruta}/contaminacion/sensores"),
            ("clima", "mediciones_totales", int(total_clima_med), f"{self.ruta}/clima/mediciones"),
            ("clima", "sensores_registrados", int(total_clima_sen), f"{self.ruta}/clima/sensores"),
        ]

        return self.spark.createDataFrame(filas, ["dataset", "metrica", "valor", "origen"])