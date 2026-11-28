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
            SELECT fuente, sensor_id, nombre, parametro, unidad, organismo, zona, latitude, longitude
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

    # ---- Consultas que usan las zonas y las fechas por sensor (db/sensores_zonas.sql) ----

    @classmethod
    def _rango(cls, fecha_ini: str | None, fecha_fin: str | None) -> tuple[str, str]:
        """Literales SQL (timestamp) del periodo 'AAAA-MM-DD'. Sin fechas = todo el periodo."""
        ini = f"{cls._txt(fecha_ini + ' 00:00:00')}::timestamp" if fecha_ini else "'1900-01-01'::timestamp"
        fin = f"{cls._txt(fecha_fin + ' 23:59:59')}::timestamp" if fecha_fin else "'2999-12-31'::timestamp"
        return ini, fin

    def sensores_activos_inactivos(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        """3. Cantidad de sensores activos e inactivos en el periodo (función estado_sensores)."""
        ini, fin = self._rango(fecha_ini, fecha_fin)
        return self.consultar(
            f"""
            SELECT fuente, estado, COUNT(1) AS sensores
            FROM estado_sensores({ini}, {fin})
            GROUP BY fuente, estado
            ORDER BY fuente, estado
            """
        )

    def sensores_instalados_por_anio(self) -> DataFrame:
        """6. Sensores instalados por año (año de su primera medición)."""
        return self.consultar(
            """
            SELECT EXTRACT(year FROM fecha_inicio_datos)::int AS anio,
                   COUNT(DISTINCT (fuente, sensor_id)) AS sensores
            FROM sensores_unificados
            WHERE fecha_inicio_datos IS NOT NULL
            GROUP BY anio
            ORDER BY anio
            """
        )

    def sensores_sin_actividad(self, fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        """7. Sensores sin actividad registrada en el periodo (inactivos según estado_sensores)."""
        ini, fin = self._rango(fecha_ini, fecha_fin)
        return self.consultar(
            f"""
            SELECT fuente, sensor_id, nombre, zona, fecha_inicio_datos, fecha_fin_datos
            FROM estado_sensores({ini}, {fin})
            WHERE estado = 'inactivo'
            ORDER BY fuente, nombre
            """
        )

    CRITERIOS_RANKING = ("sensores", "accidentes")

    def ranking_zonas(self, n: int = 10, criterio: str = "sensores", ascendente: bool = False,
                      fecha_ini: str | None = None, fecha_fin: str | None = None) -> DataFrame:
        """8. Ranking de zonas por sensores monitoreados o por accidentes en el periodo.
        Los sensores de clima cuentan por la zona simulada (ver db/vistas.sql)."""
        if criterio not in self.CRITERIOS_RANKING:
            raise ValueError(f"Criterio inválido: {criterio}. Use {self.CRITERIOS_RANKING}")
        n = int(n)
        orden = "ASC" if ascendente else "DESC"
        otro = "accidentes" if criterio == "sensores" else "sensores"
        ini, fin = self._rango(fecha_ini, fecha_fin)
        return self.consultar(
            f"""
            WITH sens AS (
                SELECT zona_id, COUNT(DISTINCT (fuente, sensor_id)) AS sensores
                FROM sensores_unificados
                WHERE zona_id IS NOT NULL
                GROUP BY zona_id
            ), acc AS (
                SELECT ez.zona_id, COUNT(1) AS accidentes
                FROM accidents a
                JOIN cities c ON c.city_id = a.city_id
                JOIN estados_zona ez ON ez.state_code = c.state_code
                WHERE a.start_time_utc >= {ini} AND a.start_time_utc <= {fin}
                GROUP BY ez.zona_id
            )
            SELECT z.zona_id, z.nombre AS zona, z.region,
                   COALESCE(s.sensores, 0) AS sensores,
                   COALESCE(a.accidentes, 0) AS accidentes
            FROM zonas z
            LEFT JOIN sens s ON s.zona_id = z.zona_id
            LEFT JOIN acc  a ON a.zona_id = z.zona_id
            ORDER BY {criterio} {orden}, {otro} {orden}, z.nombre
            LIMIT {n}
            """
        )

    def sensores_metadata(self) -> DataFrame:
        """Una fila por sensor físico (fuente, sensor_id, nombre, zona, zona_simulada, parametro).
        `parametro` solo existe en contaminación (en clima cada dispositivo mide varias variables).
        Lo usa Queries_MG para poner nombre y zona a los resultados de Mongo."""
        return self.consultar(
            """
            SELECT fuente, sensor_id, nombre, zona, zona_simulada,
                   CASE WHEN fuente = 'contaminacion' THEN MIN(parametro) END AS parametro
            FROM sensores_unificados
            GROUP BY fuente, sensor_id, nombre, zona, zona_simulada
            ORDER BY fuente, nombre, sensor_id
            """
        )

    # Registro de consultas obligatorias: id -> (descripción, método)
    CONSULTAS = {
        1: ("Sensores por tipo", "sensores_por_tipo"),
        2: ("Sensores por ubicación", "sensores_por_ubicacion"),
        3: ("Sensores activos e inactivos", "sensores_activos_inactivos"),
        4: ("Sensores por organismo responsable", "sensores_por_organismo"),
        5: ("Ubicaciones con mayor cantidad de sensores", "ubicaciones_con_mas_sensores"),
        6: ("Sensores instalados por año", "sensores_instalados_por_anio"),
        7: ("Sensores sin actividad registrada", "sensores_sin_actividad"),
        8: ("Ranking de zonas monitoreadas", "ranking_zonas"),
        9: ("Consulta utilizando una vista", "consulta_con_vista"),
        10: ("Consulta utilizando una función almacenada", "accidentes_por_severidad_fn"),
    }

    def ejecutar(self, numero: int, **parametros) -> DataFrame:
        """Ejecuta una consulta obligatoria por su número (ver CONSULTAS)."""
        if numero not in self.CONSULTAS:
            raise ValueError(f"Consulta {numero} no disponible. Disponibles: {sorted(self.CONSULTAS)}")
        return getattr(self, self.CONSULTAS[numero][1])(**parametros)