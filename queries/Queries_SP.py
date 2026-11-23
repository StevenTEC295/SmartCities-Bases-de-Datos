import streamlit as st
from pyspark.sql import SparkSession,DataFrame, functions as F


class Queries_SP:
    def __init__(self, spark: SparkSession, ruta):
        self.spark = spark
        self.rutas = ruta

    def probar(self):
        self.spark.range(5).show()
    

    @st.cache_data(show_spinner="Consultando Spark...")
    def sensoresTipo(self) -> DataFrame:
        return (
            self.spark.read.parquet(self.ruta)
            .groupBy("fecha", "region", "producto")
            .agg(
                F.sum("unidades").alias("unidades"),
                F.sum("ingresos").alias("ingresos"),
            )
            .toDF()
        )

   