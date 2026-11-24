from pyspark.sql import DataFrame, SparkSession, functions as F


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