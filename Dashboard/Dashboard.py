import io
import json
from contextlib import redirect_stdout
from decimal import Decimal

import streamlit as st


class Dashboard:
    """
    Versión de PRUEBAS del dashboard.

    Ejecuta, uno por uno, los métodos actuales de las cuatro clases de consultas
    (Queries_SP, Queries_PG, Queries_MG y Queries_HDFS) y muestra el resultado.

    - Los DataFrames de Spark se muestran tomando sus primeras filas con
      `limit().collect()` y pasándolas a Streamlit como lista de diccionarios.
    - Si un método falla, se muestra el error completo en pantalla: en una prueba
      eso es justo lo que se quiere ver.
    - Si un método devuelve algo que no es un DataFrame de Spark, se muestra tal cual
      y se avisa del tipo de objeto recibido.
    """

    LIMITE_FILAS = 500

    def __init__(
        self,
        queries,
        queries_pg,
        queries_mg,
        queries_hdfs,
        titulo: str = "Dashboard de SmartCities (pruebas)",
        icono: str = "🧪",
    ):
        self.queries = queries            # Queries_SP   (Spark sobre archivos)
        self.queries_pg = queries_pg      # Queries_PG   (PostgreSQL vía JDBC)
        self.queries_mg = queries_mg      # Queries_MG   (MongoDB, Aggregation Framework)
        self.queries_hdfs = queries_hdfs  # Queries_HDFS (comandos HDFS + MapReduce)
        self.titulo = titulo
        self.icono = icono

    # =====================================================================
    #  Utilidades
    # =====================================================================
    @staticmethod
    def _spec(texto: str):
        """'3' -> posición 3; 'region' -> nombre de columna."""
        texto = texto.strip()
        return int(texto) if texto.isdigit() else texto

    def _filas(self, sdf) -> list:
        """
        Primeras filas de un DataFrame de Spark como lista de diccionarios.
        Los Decimal (resultado habitual de sum/avg en Spark) pasan a float para poder graficarse.
        """
        filas = []
        for fila in sdf.limit(self.LIMITE_FILAS).collect():
            d = fila.asDict(recursive=True)
            filas.append({k: float(v) if isinstance(v, Decimal) else v for k, v in d.items()})
        return filas

    def _probar_spark(self) -> str:
        """Ejecuta Queries_SP.probar() y devuelve lo que imprime (show() escribe en consola)."""
        salida = io.StringIO()
        with redirect_stdout(salida):
            self.queries.probar()
        return salida.getvalue() or "probar() se ejecutó sin imprimir nada."

    def _ejecutar(self, clave: str, etiqueta: str, funcion, deshabilitado: bool = False) -> None:
        """
        Botón que ejecuta `funcion` y muestra su resultado o el error completo.
        El resultado queda guardado en session_state para que sobreviva a los cambios de widgets.
        """
        if st.button(etiqueta, key=f"btn_{clave}", disabled=deshabilitado):
            try:
                with st.spinner("Ejecutando..."):
                    st.session_state[f"res_{clave}"] = funcion()
                st.session_state.pop(f"err_{clave}", None)
            except Exception as e:  # noqa: BLE001 - en pruebas se quiere ver cualquier fallo
                st.session_state[f"err_{clave}"] = e
                st.session_state.pop(f"res_{clave}", None)

        if f"err_{clave}" in st.session_state:
            st.exception(st.session_state[f"err_{clave}"])
        elif f"res_{clave}" in st.session_state:
            self._mostrar(clave, st.session_state[f"res_{clave}"])

    def _mostrar(self, clave: str, resultado) -> None:
        if resultado is None:
            st.info("El método se ejecutó y devolvió None.")
        elif hasattr(resultado, "collect"):  # DataFrame de Spark
            filas = self._filas(resultado)
            st.caption(
                f"DataFrame de Spark · columnas: {', '.join(resultado.columns)} · "
                f"se muestran hasta {self.LIMITE_FILAS} filas"
            )
            st.dataframe(filas, width="stretch")
            self._graficar(clave, list(resultado.columns), filas)
        elif isinstance(resultado, str):
            st.code(resultado)
        elif isinstance(resultado, (bool, int, float)):
            st.write(resultado)
        else:  # lo que devuelva el método, sin transformarlo
            tipo = f"{type(resultado).__module__}.{type(resultado).__name__}"
            st.warning(f"El método devolvió un objeto de tipo {tipo}, no un DataFrame de Spark.")
            st.dataframe(resultado, width="stretch")

    def _graficar(self, clave: str, columnas: list, filas: list) -> None:
        if not filas or len(columnas) < 2:
            return
        with st.expander("Graficar"):
            c1, c2, c3 = st.columns(3)
            x = c1.selectbox("Eje X", columnas, key=f"x_{clave}")
            y = c2.selectbox("Eje Y (numérico)", columnas, index=1, key=f"y_{clave}")
            tipo = c3.selectbox("Tipo", ["barras", "línea"], key=f"t_{clave}")
            try:
                grafico = st.bar_chart if tipo == "barras" else st.line_chart
                grafico(filas, x=x, y=y)
            except Exception as e:  # noqa: BLE001
                st.error(f"No se pudo graficar con esas columnas: {e}")

    # =====================================================================
    #  Pestañas (una por clase de consultas)
    # =====================================================================
    def _tab_spark(self) -> None:
        st.caption("Queries_SP: `probar()` y `sensoresTipo()`")
        st.subheader("probar()")
        self._ejecutar("sp_probar", "Ejecutar probar()", self._probar_spark)
        st.subheader("sensoresTipo()")
        self._ejecutar("sp_tipo", "Ejecutar sensoresTipo()", self.queries.sensoresTipo)

    def _tab_postgres(self) -> None:
        st.caption("Queries_PG: `consultar(sql)`, `top_ubicaciones(n)` y `accidentes_por_severidad()`")
        st.subheader("consultar(sql)")
        sql = st.text_area(
            "SQL (solo SELECT)",
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            key="pg_sql",
        )
        self._ejecutar("pg_consultar", "Ejecutar consultar(sql)",
                       lambda: self.queries_pg.consultar(sql), deshabilitado=not sql.strip())

        st.subheader("top_ubicaciones(n)")
        n = st.number_input("n", min_value=1, max_value=100, value=5, key="pg_n")
        self._ejecutar("pg_top", "Ejecutar top_ubicaciones(n)",
                       lambda: self.queries_pg.top_ubicaciones(int(n)))

        st.subheader("accidentes_por_severidad()")
        self._ejecutar("pg_sev", "Ejecutar accidentes_por_severidad()",
                       self.queries_pg.accidentes_por_severidad)

    def _tab_mongo(self) -> None:
        st.caption("Queries_MG: `agregar`, `conteo_por_campo`, `top_n` y `serie_diaria`")
        coleccion = st.text_input("Colección", "mediciones", key="mg_col")

        st.subheader("agregar(coleccion, pipeline)")
        pipeline = st.text_area("Pipeline (JSON)", '[{"$limit": 10}]', key="mg_pipe")
        self._ejecutar("mg_agregar", "Ejecutar agregar()",
                       lambda: self.queries_mg.agregar(coleccion, json.loads(pipeline)),
                       deshabilitado=not coleccion.strip())

        st.subheader("Consultas de ejemplo")
        c1, c2, c3, c4 = st.columns(4)
        campo = c1.text_input("Campo de grupo", key="mg_campo")
        valor = c2.text_input("Campo numérico", key="mg_valor")
        fecha = c3.text_input("Campo de fecha (tipo Date)", key="mg_fecha")
        n = c4.number_input("n", min_value=1, max_value=100, value=10, key="mg_n")

        self._ejecutar("mg_conteo", "conteo_por_campo(coleccion, campo)",
                       lambda: self.queries_mg.conteo_por_campo(coleccion, campo),
                       deshabilitado=not (coleccion and campo))
        self._ejecutar("mg_top", "top_n(coleccion, campo, valor, n)",
                       lambda: self.queries_mg.top_n(coleccion, campo, valor, int(n)),
                       deshabilitado=not (coleccion and campo and valor))
        self._ejecutar("mg_serie", "serie_diaria(coleccion, fecha, valor)",
                       lambda: self.queries_mg.serie_diaria(coleccion, fecha, valor),
                       deshabilitado=not (coleccion and fecha and valor))

    def _tab_hdfs(self) -> None:
        st.caption("Queries_HDFS: comandos `hdfs dfs` y consultas MapReduce")
        ruta = st.text_input(
            "Ruta (vacía = carpeta base; relativa, absoluta o hdfs://...)", "", key="hd_ruta"
        )

        st.subheader("Comandos de HDFS")
        self._ejecutar("hd_listar", "listar(ruta)", lambda: self.queries_hdfs.listar(ruta))
        self._ejecutar("hd_existe", "existe(ruta)", lambda: self.queries_hdfs.existe(ruta),
                       deshabilitado=not ruta.strip())
        self._ejecutar("hd_carpeta", "es_carpeta(ruta)", lambda: self.queries_hdfs.es_carpeta(ruta),
                       deshabilitado=not ruta.strip())
        filas = st.number_input("Filas a cargar", min_value=1, max_value=10000, value=100, key="hd_filas")
        self._ejecutar("hd_csv", "cargar_csv(ruta, limite)",
                       lambda: self.queries_hdfs.cargar_csv(ruta, limite=int(filas)),
                       deshabilitado=not ruta.strip())

        st.subheader("MapReduce (Hadoop)")
        top = st.number_input("Palabras a mostrar", min_value=1, max_value=100, value=20, key="hd_top")
        self._ejecutar("hd_wc", "contar_palabras(ruta, top)",
                       lambda: self.queries_hdfs.contar_palabras(ruta, top=int(top)),
                       deshabilitado=not ruta.strip())

        c1, c2, c3 = st.columns(3)
        grupo = c1.text_input("Columna de grupo (nombre o posición)", key="hd_grupo")
        valor = c2.text_input("Columna de valor (nombre o posición)", key="hd_valor")
        operacion = c3.selectbox("Operación", ["sum", "count", "avg", "min", "max"], key="hd_op")
        encabezado = st.checkbox("El archivo tiene encabezado", value=True, key="hd_enc")
        self._ejecutar(
            "hd_agrupar", "agrupar(ruta, grupo, valor, operacion)",
            lambda: self.queries_hdfs.agrupar(
                ruta, self._spec(grupo), self._spec(valor), operacion, encabezado=encabezado
            ),
            deshabilitado=not (ruta.strip() and grupo and valor),
        )

    # =====================================================================
    #  Ejecución
    # =====================================================================
    def run(self) -> None:
        st.set_page_config(page_title=self.titulo, page_icon=self.icono, layout="wide")
        st.title(f"{self.icono} {self.titulo}")
        st.caption(
            "Ejecuta los métodos actuales de cada clase de consultas. "
            "Los resultados se muestran como DataFrames de Spark."
        )

        spark, pg, mg, hdfs = st.tabs(["Spark", "PostgreSQL", "MongoDB", "HDFS / Hadoop"])
        with spark:
            self._tab_spark()
        with pg:
            self._tab_postgres()
        with mg:
            self._tab_mongo()
        with hdfs:
            self._tab_hdfs()