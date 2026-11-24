from pyspark.sql import DataFrame, SparkSession


class Queries_PG:
    """
    Consultas a PostgreSQL usando Spark + JDBC (vía Spark Connect).
    Todos los métodos devuelven DataFrames de Spark.
    """

    def __init__(self, spark: SparkSession, url: str, user, password, driver):
        self.spark = spark
        self.url = url
        self.user = user
        self.password = password
        self.driver = driver

    def _leer(self, sql: str) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.url)
            .option("query", sql)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", self.driver)
            .load()
        )

    def consultar(self, sql: str) -> DataFrame:
        """Ejecuta un SELECT en PostgreSQL (el trabajo lo hace la base) y devuelve un DataFrame de Spark."""
        sql = sql.strip().rstrip(";")   # la opción JDBC `query` no admite punto y coma final
        if not sql:
            raise ValueError("El SQL está vacío")
        return self._leer(sql)

    # ---- Consultas de ejemplo sobre el esquema de db/init.sql y db/accidents.sql ----
    def top_ubicaciones(self, n: int = 10) -> DataFrame:
        """Ubicaciones con más sensores."""
        n = int(n)
        return self.consultar(
            f"""
            SELECT l.location_name, COUNT(s.sensor_id) AS sensores
            FROM locations l
            JOIN sensores s ON s.location_id = l.location_id
            GROUP BY l.location_name
            ORDER BY sensores DESC
            LIMIT {n}
            """
        )

    def accidentes_por_severidad(self) -> DataFrame:
        """Número de accidentes por nivel de severidad."""
        return self.consultar(
            """
            SELECT severity, COUNT(*) AS accidentes
            FROM accidents
            GROUP BY severity
            ORDER BY severity
            """
        )