#!/usr/bin/env python
"""Actualiza matricula.csv y flujo.csv con datos de reportes.sie.gob.bo.

Educación Regular solamente. El sitio publica solo la gestión vigente; la
gestión que se guarda es la que dice el sitio, no la fecha del sistema.

- matricula.csv: estudiantes efectivos por unidad, nivel, grado y sexo. Sale
  del reporte XLS de cada distrito educativo.
- flujo.csv: inscritos, no incorporados, abandono, traslado y efectivos por
  unidad. Sale de la página de cada unidad.

Cada corrida reemplaza las filas de la gestión vigente. También pide la
gestión anterior: si el sitio todavía tiene datos de esa gestión, también
reemplaza sus filas. Las demás gestiones no cambian.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://reportes.sie.gob.bo/reporteestadistico/"
URL_PAGINA = BASE + "informacion/general/regular"
URL_XLS = URL_PAGINA + "/print/xls"
CARPETA = Path(__file__).parent

ROL_DEPARTAMENTO, ROL_DISTRITO, ROL_UNIDAD = "7", "10", "9"
NIVELES = {
    "Inicial en Familia Comunitaria": "inicial",
    "Primaria Comunitaria Vocacional": "primaria",
    "Secundaria Comunitaria Productiva": "secundaria",
}
SEXOS = {"Mujer": "mujer", "Varón": "hombre"}
FLUJO = {
    "Total Inscritos": "inscritos",
    "No Incorporados": "no_incorporados",
    "Retiros por Abandono": "abandono",
    "Retiros por Traslado": "traslado",
    "Efectivos": "efectivos",
}
COLUMNAS_MATRICULA = ["gestion", "rue", "nivel", "grado", "sexo", "estudiantes"]
COLUMNAS_FLUJO = ["gestion", "rue", *FLUJO.values()]

SS = "{urn:schemas-microsoft-com:office:spreadsheet}"
PUNTO = re.compile(r"\{name: '([^']+)', y: [^,}]+, label: '?([\d.]+)'?\}")  # "1.106" = 1106

_local = threading.local()


def sesion() -> requests.Session:
    """Una sesión por hilo. Las páginas exigen una cookie de sesión."""
    s = getattr(_local, "sesion", None)
    if s is None:
        retry = Retry(
            total=4,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            respect_retry_after_header=True,
        )
        s = requests.Session()
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.headers["User-Agent"] = "rue-datos (actualizacion periodica)"
        s.get(BASE, timeout=120).raise_for_status()
        _local.sesion = s
    return s


def log(mensaje: str) -> None:
    print(mensaje, file=sys.stderr, flush=True)


def post(url: str, datos: dict, timeout: float, intentos: int = 4) -> requests.Response:
    """POST con reintentos para conexiones cortadas a mitad de la respuesta."""
    for intento in range(intentos):
        try:
            r = sesion().post(url, data=datos, timeout=timeout)
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            if intento == intentos - 1:
                raise
            _local.sesion = None
            time.sleep(2**intento)
    raise AssertionError("inalcanzable")


def pagina(codigo: str, rol: str, timeout: float) -> BeautifulSoup:
    r = post(URL_PAGINA, {"codigo": codigo, "rol": rol}, timeout)
    return BeautifulSoup(r.text, "html.parser")


def enlaces(html: BeautifulSoup, rol: str) -> list[str]:
    """Códigos de los formularios que bajan al nivel siguiente."""
    codigos = []
    for form in html.select("form"):
        if not (form.get("action") or "").endswith("general/regular"):
            continue
        campos = {i["name"]: i.get("value") for i in form.select("input[name]")}
        if campos.get("rol") == rol:
            codigos.append(campos["codigo"])
    return codigos


def gestion_vigente(timeout: float) -> tuple[int, list[str]]:
    r = sesion().get(BASE, timeout=timeout)
    r.raise_for_status()
    html = BeautifulSoup(r.text, "html.parser")
    campo = html.select_one('form[action$="print/xls"] input[name="gestion"]')
    if campo is None:
        raise RuntimeError("No se encontró la gestión vigente")
    return int(campo["value"]), enlaces(html, ROL_DEPARTAMENTO)


def distritos(departamentos: list[str], timeout: float) -> list[str]:
    codigos = set()
    for d in departamentos:
        codigos.update(enlaces(pagina(d, ROL_DEPARTAMENTO, timeout), ROL_DISTRITO))
    return sorted(codigos, key=int)


# matricula.csv ---------------------------------------------------------------


def celdas(fila: ET.Element) -> dict[int, tuple[str, int]]:
    """{columna: (texto, columnas adicionales unidas)}; ss:Index salta columnas."""
    resultado, posicion = {}, 0
    for celda in fila.iter(f"{SS}Cell"):
        posicion = int(celda.get(f"{SS}Index", posicion + 1))
        union = int(celda.get(f"{SS}MergeAcross", 0))
        resultado[posicion] = (" ".join("".join(celda.itertext()).split()), union)
        posicion += union
    return resultado


def leer_xls(contenido: bytes) -> tuple[int, list[dict]]:
    filas = [celdas(f) for f in ET.fromstring(contenido).iter(f"{SS}Row")]
    texto = " ".join(t for f in filas for t, _ in f.values())
    m = re.search(r"GESTI[ÓO]N (\d{4})", texto)
    if m is None:
        raise ValueError("El reporte no indica la gestión")
    gestion = int(m[1])

    # Encabezados: fila de niveles (con columnas unidas) y fila de grados.
    i = next((i for i, f in enumerate(filas) if any(t in NIVELES for t, _ in f.values())), None)
    if i is None:
        return gestion, []  # reporte sin datos
    rangos = [(c, c + u, NIVELES[t]) for c, (t, u) in filas[i].items() if t in NIVELES]
    columnas = {}
    for c, (t, _) in filas[i + 1].items():
        if m := re.fullmatch(r"(\d)°", t):
            nivel = next((n for a, b, n in rangos if a <= c <= b), None)
            if nivel is None:
                raise ValueError(f"Grado {t} en la columna {c} sin nivel")
            columnas[c] = (nivel, int(m[1]))
    if len(columnas) != 14:
        raise ValueError(f"Se esperaban 14 grados y hay {len(columnas)}")

    resultado, rue = [], None
    for fila in filas[i + 2 :]:
        if m := re.match(r"(\d{8}) - ", fila.get(1, ("", 0))[0]):
            rue = m[1]
        sexo = SEXOS.get(fila.get(4, ("", 0))[0])
        if sexo is None:
            continue
        if rue is None:
            raise ValueError("Fila de sexo antes de la primera unidad")
        for c, (nivel, grado) in columnas.items():
            valor = fila.get(c, ("", 0))[0]
            estudiantes = int(float(valor)) if valor else 0
            if estudiantes:
                resultado.append(
                    {"gestion": gestion, "rue": rue, "nivel": nivel, "grado": grado, "sexo": sexo, "estudiantes": estudiantes}
                )
    return gestion, resultado


def xls(distrito: str, gestion: int, timeout: float) -> list[dict]:
    datos = {"gestion": str(gestion), "codigo": distrito, "rol": ROL_DISTRITO}
    r = post(URL_XLS, datos, timeout)
    leida, filas = leer_xls(r.content)
    if leida != gestion:
        raise ValueError(f"Se pidió la gestión {gestion} y el reporte es de {leida}")
    return filas


def matricula(distritos: list[str], gestion: int, hilos: int, timeout: float) -> list[dict]:
    filas = []
    with ThreadPoolExecutor(hilos) as ex:
        futuros = [ex.submit(xls, d, gestion, timeout) for d in distritos]
        for n, futuro in enumerate(as_completed(futuros), 1):
            filas.extend(futuro.result())  # un distrito con error detiene todo
            if n % 50 == 0 or n == len(futuros):
                log(f"Matrícula {gestion}: {n}/{len(futuros)} distritos")
    return filas


# flujo.csv -------------------------------------------------------------------


def flujo_unidad(rue: str, gestion: int, timeout: float) -> dict:
    html = pagina(rue, ROL_UNIDAD, timeout)
    texto = " ".join(html.get_text(" ").split())
    if f"Gestión: {gestion}" not in texto:
        raise ValueError(f"La página de {rue} no es de la gestión {gestion}")
    scripts = "\n".join(s.get_text() for s in html.select("script"))
    inicio = scripts.find("chartContainerMatriculaLoad")
    if inicio < 0:
        raise ValueError(f"La página de {rue} no tiene el gráfico de matrícula")
    puntos = dict(PUNTO.findall(scripts[inicio : inicio + 4000]))
    faltan = set(FLUJO) - set(puntos)
    if faltan:
        raise ValueError(f"La página de {rue} no tiene {faltan}")
    return {"gestion": gestion, "rue": rue, **{FLUJO[k]: int(puntos[k].replace(".", "")) for k in FLUJO}}


def flujo(rues: list[str], gestion: int, hilos: int, timeout: float) -> tuple[list[dict], dict[str, Exception]]:
    filas, errores = [], {}
    inicio = time.monotonic()
    with ThreadPoolExecutor(hilos) as ex:
        futuros = {ex.submit(flujo_unidad, rue, gestion, timeout): rue for rue in rues}
        for n, futuro in enumerate(as_completed(futuros), 1):
            try:
                filas.append(futuro.result())
            except (requests.RequestException, ValueError) as e:
                errores[futuros[futuro]] = e
            if n % 1000 == 0 or n == len(futuros):
                log(f"Flujo {gestion}: {n}/{len(futuros)} ({time.monotonic() - inicio:.0f} s, {len(errores)} errores)")
    return filas, errores


# Archivos --------------------------------------------------------------------


def leer_csv(ruta: Path) -> list[dict]:
    if not ruta.exists():
        return []
    with ruta.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def escribir_csv(ruta: Path, columnas: list[str], filas: list[dict], orden) -> None:
    temporal = ruta.with_name(ruta.name + ".tmp")
    with temporal.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columnas, lineterminator="\n")
        w.writeheader()
        w.writerows(sorted(({c: str(fila[c]) for c in columnas} for fila in filas), key=orden))
    temporal.replace(ruta)


def reemplazar(ruta: Path, columnas: list[str], nuevas: dict[int, list[dict]], orden) -> None:
    """Reemplaza las filas de las gestiones de `nuevas`; mantiene las demás."""
    gestiones = {str(g) for g in nuevas}
    filas = [f for f in leer_csv(ruta) if f["gestion"] not in gestiones]
    filas += [f for lista in nuevas.values() for f in lista]
    escribir_csv(ruta, columnas, filas, orden)


def orden_matricula(f: dict):
    return (int(f["gestion"]), int(f["rue"]), f["nivel"], int(f["grado"]), f["sexo"])


def orden_flujo(f: dict):
    return (int(f["gestion"]), int(f["rue"]))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--carpeta", type=Path, default=CARPETA, help="carpeta de matricula.csv y flujo.csv")
    p.add_argument("--sin-flujo", action="store_true", help="no actualiza flujo.csv")
    p.add_argument("--hilos", type=int, default=16, help="consultas en paralelo")
    p.add_argument("--timeout", type=float, default=180)
    p.add_argument("--max-errores", type=float, default=0.02, help="fracción máxima de unidades con error en el flujo")
    p.add_argument("--limite", type=int, help="usa solo N distritos (prueba)")
    a = p.parse_args()
    ruta_matricula, ruta_flujo = a.carpeta / "matricula.csv", a.carpeta / "flujo.csv"

    vigente, departamentos = gestion_vigente(a.timeout)
    lista = distritos(departamentos, a.timeout)[: a.limite]
    log(f"Gestión vigente: {vigente}; distritos: {len(lista)}")

    nuevas = {vigente: matricula(lista, vigente, a.hilos, a.timeout)}
    if not nuevas[vigente]:
        sys.exit(f"La gestión {vigente} no tiene matrícula; no se escribe nada.")
    # La gestión anterior: se prueba con 3 distritos antes de pedir todos.
    if any(xls(d, vigente - 1, a.timeout) for d in lista[:3]):
        nuevas[vigente - 1] = matricula(lista, vigente - 1, a.hilos, a.timeout)
    for g, filas in nuevas.items():
        total = sum(f["estudiantes"] for f in filas)
        log(f"Matrícula {g}: {len({f['rue'] for f in filas})} unidades, {total} estudiantes")
    reemplazar(ruta_matricula, COLUMNAS_MATRICULA, nuevas, orden_matricula)

    if a.sin_flujo:
        return
    # El flujo de la página siempre es de la gestión vigente.
    rues = sorted({f["rue"] for f in nuevas[vigente]}, key=int)
    filas, errores = flujo(rues, vigente, a.hilos, a.timeout)
    for rue, e in sorted(errores.items(), key=lambda x: int(x[0])):
        log(f"Error en {rue}: {type(e).__name__}: {e}")
    if len(errores) > a.max_errores * len(rues):
        sys.exit(f"{len(errores)} de {len(rues)} unidades con error en el flujo; no se escribe flujo.csv.")
    anteriores = {f["rue"]: f for f in leer_csv(ruta_flujo) if f["gestion"] == str(vigente)}
    filas += [anteriores[r] for r in errores if r in anteriores]
    reemplazar(ruta_flujo, COLUMNAS_FLUJO, {vigente: filas}, orden_flujo)
    log(f"Flujo {vigente}: {len(filas)} unidades, {len(errores)} errores")


if __name__ == "__main__":
    main()
