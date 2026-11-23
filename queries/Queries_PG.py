import streamlit as st
from pyspark.sql import DataFrame, SparkSession


class Queries_PG:
    """Consultas a Postgres usando Spark + JDBC (vía Spark Connect)."""

    

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

    @st.cache_data(show_spinner="Consultando Postgres...")
    def consultar(_self, sql: str) -> DataFrame:
        """Ejecuta cualquier SELECT en Postgres y devuelve un DataFrame de pandas."""
        return _self._leer(sql).toDF()

    # ---- Consultas de ejemplo: cambia tabla y columnas por las de tu esquema ----
    def top_productos(self, n: int = 10) -> DataFrame:
        n = int(n)
        return self.consultar(
            f"""
            SELECT producto, SUM(ingresos) AS ingresos
            FROM ventas
            GROUP BY producto
            ORDER BY ingresos DESC
            LIMIT {n}
            """
        )