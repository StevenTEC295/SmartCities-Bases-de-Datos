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

    # ---- Consultas obligatorias de PostgreSQL (se leen de la vista sensores_unificados) ----
    # En clima cada dispositivo aparece 3 veces en la vista (temperatura, humedad, batería);
    # por eso los conteos de sensores usan COUNT(DISTINCT (fuente, sensor_id)).

    @staticmethod
    def _txt(valor: str) -> str:
        """Literal SQL de texto, con las comillas simples escapadas."""
        return "'" + str(valor).replace("'", "''") + "'"

    def sensores_por_tipo(self) -> DataFrame:
        """1. Sensores por tipo de parámetro (pm10, pm25, o3, temperature, humidity, battery)."""
        return self.consultar(
            """
            SELECT fuente, parametro, unidad,
                   COUNT(DISTINCT (fuente, sensor_id)) AS sensores
            FROM sensores_unificados
            GROUP BY fuente, parametro, unidad
            ORDER BY sensores DESC, parametro
            """
        )

    def sensores_por_ubicacion(self) -> DataFrame:
        """2. Sensores por ubicación (estación de contaminación o dispositivo de clima)."""
        return self.consultar(
            """
            SELECT fuente, nombre AS ubicacion,
                   COUNT(DISTINCT (fuente, sensor_id)) AS sensores
            FROM sensores_unificados
            GROUP BY fuente, nombre
            ORDER BY fuente, ubicacion
            """
        )

    def sensores_por_organismo(self) -> DataFrame:
        """4. Número de sensores por organismo responsable."""
        return self.consultar(
            """
            SELECT organismo,
                   COUNT(DISTINCT (fuente, sensor_id)) AS sensores
            FROM sensores_unificados
            GROUP BY organismo
            ORDER BY sensores DESC
            """
        )

    def ubicaciones_con_mas_sensores(self, n: int = 10) -> DataFrame:
        """5. Ubicaciones con mayor cantidad de sensores (top n)."""
        n = int(n)
        return self.consultar(
            f"""
            SELECT fuente, nombre AS ubicacion,
                   COUNT(DISTINCT (fuente, sensor_id)) AS sensores
            FROM sensores_unificados
            GROUP BY fuente, nombre
            ORDER BY sensores DESC, ubicacion
            LIMIT {n}
            """
        )

    def consulta_con_vista(self, fuente: str | None = None, parametro: str | None = None,
                           organismo: str | None = None) -> DataFrame:
        """9. Listado de sensores desde la vista sensores_unificados, con filtros opcionales."""
        condiciones = []
        if fuente:
            condiciones.append(f"fuente = {self._txt(fuente)}")
        if parametro:
            condiciones.append(f"parametro = {self._txt(parametro)}")
        if organismo:
            condiciones.append(f"organismo = {self._txt(organismo)}")
        donde = ("WHERE " + " AND ".join(condiciones)) if condiciones else ""
        return self.consultar(
            f"""
            SELECT fuente, sensor_id, nombre, parametro, unidad, organismo, latitude, longitude
            FROM sensores_unificados
            {donde}
            ORDER BY fuente, nombre, sensor_id, parametro
            """
        )

    def accidentes_por_severidad_fn(self, fecha_ini: str | None = None, fecha_fin: str | None = None,
                                    estado: str | None = None) -> DataFrame:
        """10. Llama a la función almacenada accidentes_por_severidad (db/funciones.sql).
        Fechas en formato 'AAAA-MM-DD' (UTC); None = sin filtro."""
        ini = f"{self._txt(fecha_ini)}::timestamp" if fecha_ini else "NULL::timestamp"
        fin = f"{self._txt(fecha_fin + ' 23:59:59')}::timestamp" if fecha_fin else "NULL::timestamp"
        est = self._txt(estado) if estado else "NULL::text"
        return self.consultar(f"SELECT severity, accidentes FROM accidentes_por_severidad({ini}, {fin}, {est})")

    # Registro de consultas obligatorias: id -> (descripción, método)
    CONSULTAS = {
        1: ("Sensores por tipo", "sensores_por_tipo"),
        2: ("Sensores por ubicación", "sensores_por_ubicacion"),
        4: ("Sensores por organismo responsable", "sensores_por_organismo"),
        5: ("Ubicaciones con mayor cantidad de sensores", "ubicaciones_con_mas_sensores"),
        9: ("Consulta utilizando una vista", "consulta_con_vista"),
        10: ("Consulta utilizando una función almacenada", "accidentes_por_severidad_fn"),
    }

    def ejecutar(self, numero: int, **parametros) -> DataFrame:
        """Ejecuta una consulta obligatoria por su número (ver CONSULTAS)."""
        if numero not in self.CONSULTAS:
            raise ValueError(f"Consulta {numero} no disponible. Disponibles: {sorted(self.CONSULTAS)}")
        return getattr(self, self.CONSULTAS[numero][1])(**parametros)