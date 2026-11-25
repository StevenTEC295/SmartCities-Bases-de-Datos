# Filtros.py
from datetime import date, datetime

class Filtros:

    def __init__(self, zonas_validas, origenes_validos,
                 categorias_validas, max_ranking):

        # Configuration
        self.zonas_validas = zonas_validas
        self.origenes_validos = origenes_validos
        self.categorias_validas = categorias_validas
        self.max_ranking = max_ranking

        # Filter values (predetermined initial values)
        self._zona = None
        self._fecha = (datetime(2016, 1, 14), datetime(2026, 11, 4))   
        self._origen = None
        self._categoria = None
        self._ranking_cantidad = 3
        self._usar_maximo = True 
        self._umbral = (True, 0.0)

    # -------------------------
    # Get methods
    # -------------------------

    def get_zona(self):
        return self._zona

    def get_fecha(self):
        return self._fecha

    def get_origen(self):
        return self._origen

    def get_categoria(self):
        return self._categoria

    def get_ranking_cantidad(self):
        return self._ranking_cantidad

    def get_usar_maximo(self):
        return self._usar_maximo

    def get_umbral(self):
        return self._umbral

    # -------------------------
    # Set methods
    # -------------------------

    def set_zona(self, valor):
        # None significa "todas las zonas" (sin filtro)
        if valor is None:
            self._zona = None
            return True

        if not isinstance(valor, str) or valor not in self.zonas_validas:
            return False

        self._zona = valor
        return True

    # Recibe una tupla de (datetime, datetime)
    def set_fecha(self, valor):
        if not isinstance(valor, tuple) or len(valor) != 2:
            return False

        fecha_inicio, fecha_fin = valor

        if not isinstance(fecha_inicio, (date, datetime)):
            return False

        if not isinstance(fecha_fin, (date, datetime)):
            return False

        if fecha_inicio > fecha_fin:
            return False

        self._fecha = valor
        return True


    def set_origen(self, valor):
        # None significa "todos los orígenes" (sin filtro)
        if valor is None:
            self._origen = None
            return True

        if not isinstance(valor, str) or valor not in self.origenes_validos:
            return False

        self._origen = valor
        return True

    def set_categoria(self, valor):
        if not isinstance(valor, str) or valor not in self.categorias_validas:
            return False

        self._categoria = valor
        return True

    def set_ranking_cantidad(self, valor):
        if not isinstance(valor, int) or isinstance(valor, bool):
            return False

        if valor <= 0 or valor > self.max_ranking:
            return False

        self._ranking_cantidad = valor
        return True

    def set_usar_maximo(self, valor):
        if not isinstance(valor, bool):
            return False

        self._usar_maximo = valor
        return True

    def set_umbral(self, es_limite_superior_y_umbral):
        es_limite_superior = es_limite_superior_y_umbral[0]
        umbral= es_limite_superior_y_umbral[1]
        if not isinstance(es_limite_superior, bool):
            return False
        if not isinstance(umbral, float) or isinstance(umbral, int):
            return False

        self._umbral = (es_limite_superior, float(umbral))
        return True

    