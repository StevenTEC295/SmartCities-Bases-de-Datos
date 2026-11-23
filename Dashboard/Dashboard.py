import pandas as pd
import streamlit as st


class Dashboard:
    """Dashboard de ventas en una sola clase."""

    PRECIOS = {"Producto A": 20, "Producto B": 35, "Producto C": 50}
    REGIONES = ["Norte", "Sur", "Este", "Oeste"]

    def __init__(
        self,
        queries,
        queries_pg,
        queries_mg,
        queries_hdfs,
        titulo: str = "Dashboard de SmartCities",
        icono: str = "📊",
    ):
        self.queries = queries          # Queries_SP (Spark)
        self.queries_pg = queries_pg    # Queries_PG (Postgres vía JDBC)
        self.queries_mg = queries_mg    # Queries_MG (MongoDB)
        self.queries_hdfs = queries_hdfs  # Queries_HDFS (comandos HDFS + MapReduce)
        self.titulo = titulo
        self.icono = icono
        self.df = self.queries.ventas_diarias()

    # ---------- Filtros ----------
    def _filtrar(self) -> pd.DataFrame:
        st.sidebar.header("Filtros")
        df = self.df

        fmin, fmax = df["fecha"].min(), df["fecha"].max()
        rango = st.sidebar.date_input(
            "Rango de fechas", value=(fmin, fmax), min_value=fmin, max_value=fmax
        )
        if isinstance(rango, tuple) and len(rango) == 2:
            inicio, fin = pd.to_datetime(rango[0]), pd.to_datetime(rango[1])
        else:  # el usuario aún está eligiendo la segunda fecha
            inicio, fin = fmin, fmax

        regiones = sorted(df["region"].unique())
        productos = sorted(df["producto"].unique())
        regiones_sel = st.sidebar.multiselect("Región", regiones, default=regiones)
        productos_sel = st.sidebar.multiselect("Producto", productos, default=productos)

        mask = (
            df["fecha"].between(inicio, fin)
            & df["region"].isin(regiones_sel)
            & df["producto"].isin(productos_sel)
        )
        return df[mask]

    # ---------- Secciones ----------
    def _kpis(self, df: pd.DataFrame) -> None:
        ingresos = df["ingresos"].sum()
        unidades = df["unidades"].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Ingresos totales", f"${ingresos:,.0f}")
        c2.metric("Unidades vendidas", f"{unidades:,.0f}")
        c3.metric("Precio promedio por unidad", f"${ingresos / unidades:,.2f}")

    def _graficos(self, df: pd.DataFrame) -> None:
        izq, der = st.columns(2)
        with izq:
            st.subheader("Ingresos por mes")
            st.line_chart(df.set_index("fecha")["ingresos"].resample("MS").sum())
        with der:
            st.subheader("Ingresos por región")
            st.bar_chart(df.groupby("region")["ingresos"].sum().sort_values(ascending=False))

        st.subheader("Ingresos por producto y región")
        st.bar_chart(
            df.pivot_table(index="region", columns="producto", values="ingresos", aggfunc="sum")
        )

    def _top_productos_pg(self) -> None:
        st.subheader("Top productos (desde Postgres)")
        top = self.queries_pg.top_productos(n=5)
        st.bar_chart(top.set_index("producto")["ingresos"])

    def _consultas_hadoop(self) -> None:
        st.subheader("Consultas MapReduce (Hadoop sobre HDFS)")
        carpeta = st.text_input("Carpeta en HDFS", self.queries_hdfs.base_uri, key="mr_carpeta")
        try:
            items = self.queries_hdfs.listar(carpeta)
        except RuntimeError as e:
            st.error(str(e))
            return
        if items.empty:
            st.info("La carpeta está vacía.")
            return

        ruta = st.selectbox("Archivo o carpeta de datos", items["ruta"], key="mr_ruta")
        c1, c2, c3 = st.columns(3)
        grupo = c1.text_input("Columna de grupo (nombre o posición)", key="mr_grupo")
        valor = c2.text_input("Columna de valor (nombre o posición)", key="mr_valor")
        operacion = c3.selectbox("Operación", ["sum", "count", "avg", "min", "max"], key="mr_op")
        encabezado = st.checkbox("El archivo tiene encabezado", value=True, key="mr_enc")

        if st.button("Ejecutar MapReduce", disabled=not (grupo and valor)):
            def spec(x: str):                       # "3" -> posición 3; "region" -> nombre
                x = x.strip()
                return int(x) if x.isdigit() else x
            try:
                st.session_state["mr_resultado"] = self.queries_hdfs.agrupar(
                    ruta, spec(grupo), spec(valor), operacion, encabezado=encabezado
                )
            except (RuntimeError, ValueError) as e:
                st.session_state.pop("mr_resultado", None)
                st.error(str(e))

        resultado = st.session_state.get("mr_resultado")
        if resultado is not None:
            st.dataframe(resultado, width="stretch")
            if not resultado.empty:
                st.bar_chart(resultado.set_index(resultado.columns[0]).iloc[:, 0])

    def _tabla(self, df: pd.DataFrame) -> None:
        with st.expander("Ver datos detallados"):
            st.dataframe(df.sort_values("fecha", ascending=False), width="stretch")
            st.download_button(
                "Descargar CSV",
                df.to_csv(index=False).encode("utf-8"),
                file_name="ventas_filtradas.csv",
                mime="text/csv",
            )

    # ---------- Ejecución ----------
    def run(self) -> None:
        st.set_page_config(page_title=self.titulo, page_icon=self.icono, layout="wide")
        filtrado = self._filtrar()

        st.title(f"{self.icono} {self.titulo}")
        st.caption("Ejemplo con Streamlit, pandas y datos sintéticos.")

        if filtrado.empty:
            st.warning("No hay datos con los filtros seleccionados.")
            return

        self._kpis(filtrado)
        st.divider()
        self._graficos(filtrado)
        self._top_productos_pg()
        self._consultas_hadoop()
        self._tabla(filtrado)