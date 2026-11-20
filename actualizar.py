#!/usr/bin/env python
"""Actualiza unidades.csv y niveles.csv con datos del RUE (rue.sie.gob.bo).

Pasos:
1. Descarga el listado público de unidades: una consulta por departamento y
   estado (abierta o cerrada). Si el servidor no puede dar una de estas
   consultas, el script obtiene los códigos RUE de esa parte con el reporte
   por departamento (reportesrue/rptUno).
2. Descarga la ficha de cada unidad. La ficha tiene la resolución, las
   coordenadas y los niveles autorizados.

El RUE no publica una fecha de modificación ni cabeceras de caché. Por eso el
modo incremental (por defecto) compara el listado nuevo con unidades.csv y
descarga solo las fichas de las unidades nuevas, las unidades con cambios en
el listado y las unidades sin ficha válida. Un cambio que aparece solo en la
ficha (por ejemplo, coordenadas) se detecta con el modo --completo.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
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

URL_CONSULTA = "https://rue.sie.gob.bo/accesopub/consultarue/"
URL_LISTADO = URL_CONSULTA + "findieducativa"
URL_FICHA = URL_CONSULTA + "show/"
URL_REPORTES = "https://rue.sie.gob.bo/accesopub/reportesrue/"
URL_REPORTE = URL_REPORTES + "rptUno"
CARPETA = Path(__file__).parent

COLUMNAS = ["rue", "le", "nombre", "abierta", "dependencia", "resolucion", "fecha_resolucion", "longitud", "latitud"]
LISTADO = ["le", "nombre", "abierta", "dependencia", "fecha_resolucion"]  # también en el listado
FICHA = ["resolucion", "longitud", "latitud"]  # solo en la ficha

# Encabezado del listado o etiqueta de la ficha -> columna.
ETIQUETAS_LISTADO = {
    "Código RUE": "rue",
    "Institución Educativa": "nombre",
    "Código LE": "le",
    "Fecha Resolución": "fecha_resolucion",
    "Dependencia": "dependencia",
    "Estado": "estado",
}
ETIQUETAS_FICHA = {
    "Código RUE": "rue",
    "Nombre": "nombre",
    "Código LE": "le",
    "Fecha Resolución": "fecha_resolucion",
    "Dependencia": "dependencia",
    "Estado": "estado",
    "Resolución": "resolucion",
}

SIN_DATO = {"", "SIN DATO", "S/D", "-", "--"}
FECHA_MINIMA = dt.date(1901, 1, 1)  # 1900-01-01 es un valor de relleno
LONGITUD = (-70.0, -57.0)  # caja que contiene Bolivia
LATITUD = (-23.0, -9.5)
NUMERO = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
COORDENADAS = re.compile(rf"coordinates\s*:\s*\[\s*({NUMERO})\s*,\s*({NUMERO})\s*\]")
SS = "{urn:schemas-microsoft-com:office:spreadsheet}"

_local = threading.local()


class FichaNoEncontrada(Exception):
    pass


def sesion() -> requests.Session:
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
        _local.sesion = s
    return s


def log(mensaje: str) -> None:
    print(mensaje, file=sys.stderr, flush=True)


def limpiar(texto: str) -> str:
    texto = " ".join(texto.split())
    return "" if texto.upper() in SIN_DATO else texto


def fecha(texto: str) -> str:
    """Fecha ISO, o vacío si no es válida: acepta AAAA-MM-DD... y DD-MM-AAAA."""
    if m := re.fullmatch(r"(\d{2})-(\d{2})-(\d{4})", texto):
        texto = f"{m[3]}-{m[2]}-{m[1]}"
    try:
        valor = dt.date.fromisoformat(texto[:10])
    except ValueError:
        return ""
    return valor.isoformat() if FECHA_MINIMA <= valor <= dt.date.today() else ""


def normalizar(datos: dict) -> dict:
    """Convierte etiquetas del sitio en los valores de unidades.csv."""
    if "estado" in datos:
        datos["abierta"] = {"Abierta": "true", "Cerrada": "false"}.get(datos.pop("estado"), "")
    if "fecha_resolucion" in datos:
        datos["fecha_resolucion"] = fecha(datos["fecha_resolucion"])
    return datos


def opciones(html: BeautifulSoup, selector: str) -> dict[str, str]:
    """Devuelve {nombre: código} de un <select>."""
    nodo = html.select_one(selector)
    if nodo is None:
        raise RuntimeError(f"No existe {selector}")
    return {limpiar(o.get_text()): o["value"] for o in nodo.select("option") if o.get("value")}


# Listado -------------------------------------------------------------------


def listado_parcial(departamento: str, estado: str, timeout: float) -> list[dict]:
    datos = {
        "form[departamento]": departamento,
        "form[institucioneducativaEstado]": estado,
        "form[buscarEstado]": "",
        "form[tipo_search]": "institucioneducativaEstado",
    }
    r = sesion().post(URL_LISTADO, data=datos, timeout=timeout)
    r.raise_for_status()
    tabla = BeautifulSoup(r.text, "html.parser").select_one("table.dataTable")
    if tabla is None:
        raise RuntimeError("La respuesta no tiene tabla")

    encabezados = [limpiar(th.get_text()) for th in tabla.select("thead th")]
    faltan = set(ETIQUETAS_LISTADO) - set(encabezados)
    if faltan:
        raise RuntimeError(f"El listado cambió; faltan columnas {faltan}: {encabezados}")

    filas = []
    for tr in tabla.select("tbody tr"):
        celdas = [limpiar(td.get_text(" ")) for td in tr.find_all("td", recursive=False)]
        if len(celdas) != len(encabezados):
            raise RuntimeError(f"Fila con {len(celdas)} celdas y {len(encabezados)} encabezados")
        filas.append(normalizar({ETIQUETAS_LISTADO[e]: v for e, v in zip(encabezados, celdas) if e in ETIQUETAS_LISTADO}))
    return filas


def listado(hilos: int, timeout: float) -> tuple[dict[str, dict], list[tuple[str, str]]]:
    """Devuelve las unidades y las partes (departamento, estado) que fallaron."""
    r = sesion().get(URL_CONSULTA, timeout=timeout)
    r.raise_for_status()
    html = BeautifulSoup(r.text, "html.parser")
    departamentos = opciones(html, "select#form_departamento")
    estados = opciones(html, "select#form_institucioneducativaEstado")

    unidades: dict[str, dict] = {}
    fallidas = []
    with ThreadPoolExecutor(hilos) as ex:
        futuros = {
            ex.submit(listado_parcial, departamentos[d], estados[e], timeout): (d, e)
            for d in departamentos
            for e in estados
        }
        for futuro in as_completed(futuros):
            d, e = futuros[futuro]
            try:
                filas = futuro.result()
            except (requests.RequestException, RuntimeError) as error:
                log(f"Listado {d}/{e}: error ({type(error).__name__}); se usa el reporte")
                fallidas.append((d, e))
                continue
            unidades.update((f["rue"], f) for f in filas)
            log(f"Listado {d}/{e}: {len(filas)}")
    return unidades, sorted(fallidas)


def celdas(fila: ET.Element) -> dict[int, str]:
    """Celdas de una fila de SpreadsheetML; ss:Index salta columnas vacías."""
    resultado, posicion = {}, 0
    for celda in fila.iter(f"{SS}Cell"):
        posicion = int(celda.get(f"{SS}Index", posicion + 1))
        resultado[posicion - 1] = limpiar("".join(celda.itertext()))
    return resultado


def reporte(fallidas: list[tuple[str, str]], timeout: float) -> set[str]:
    """Códigos RUE de las partes fallidas según el reporte por departamento."""
    r = sesion().get(URL_REPORTES, timeout=timeout)
    r.raise_for_status()
    form = BeautifulSoup(r.text, "html.parser").select_one("form[action$=rptUno]")
    if form is None:
        raise RuntimeError("No existe el formulario del reporte")
    token = form.select_one('input[name="form[_token]"]')["value"]
    departamentos = opciones(form, 'select[name="form[departamento]"]')
    dependencias = opciones(form, 'select[name="form[institucioneducativaDependencia]"]')
    tipos = opciones(form, 'select[name="form[institucioneducativaTipo]"]')
    estados = opciones(form, 'select[name="form[institucioneducativaEstado]"]')

    rues = set()
    for d, e in fallidas:
        for dependencia in dependencias.values():
            for tipo in tipos.values():
                datos = {
                    "form[departamento]": departamentos[d],
                    "form[institucioneducativaDependencia]": dependencia,
                    "form[institucioneducativaTipo]": tipo,
                    "form[institucioneducativaEstado]": estados[e],
                    "form[localizacion]": "localizacionDepartamento",
                    "form[_token]": token,
                }
                r = sesion().post(URL_REPORTE, data=datos, timeout=timeout)
                r.raise_for_status()
                filas = [celdas(fila) for fila in ET.fromstring(r.content).iter(f"{SS}Row")]
                columna = next((k for f in filas for k, v in f.items() if v == "CÓDIGO"), None)
                if columna is None:
                    if filas:
                        raise RuntimeError(f"El reporte {d}/{e} no tiene la columna CÓDIGO")
                    continue
                rues.update(f[columna] for f in filas if f.get(columna, "").isdigit())
        log(f"Reporte {d}/{e}: {len(rues)} códigos acumulados")
    return rues


# Fichas --------------------------------------------------------------------


def leer_ficha(html_texto: str) -> tuple[dict, list[str]]:
    html = BeautifulSoup(html_texto, "html.parser")
    datos: dict[str, str] = {}
    for li in html.select("li"):
        valor = li.select_one("span.pull-right")
        if valor is None:
            continue
        etiqueta = limpiar("".join(li.find_all(string=True, recursive=False))).rstrip(":").strip()
        if etiqueta in ETIQUETAS_FICHA:
            datos[ETIQUETAS_FICHA[etiqueta]] = limpiar(valor.get_text())
    if not datos.get("rue"):
        raise ValueError("La ficha no tiene Código RUE")
    normalizar(datos)

    datos["longitud"] = datos["latitud"] = ""
    scripts = "\n".join(s.get_text() for s in html.select("script"))
    if m := COORDENADAS.search(scripts):
        longitud, latitud = m.groups()
        if LONGITUD[0] <= float(longitud) <= LONGITUD[1] and LATITUD[0] <= float(latitud) <= LATITUD[1]:
            datos["longitud"], datos["latitud"] = longitud, latitud

    niveles = []
    titulo = next((h for h in html.find_all("h4") if "niveles autorizados" in h.get_text().lower()), None)
    if titulo is not None:
        niveles = [limpiar(li.get_text()) for li in titulo.find_parent("ul").select("li")]
    return datos, sorted({n for n in niveles if n})


def ficha(rue: str, timeout: float) -> tuple[dict, list[str]]:
    r = sesion().post(URL_FICHA, data={"idRue": rue}, timeout=timeout)
    if r.status_code == 404:
        raise FichaNoEncontrada(rue)
    r.raise_for_status()
    datos, niveles = leer_ficha(r.text)
    if datos["rue"] != rue:
        raise ValueError(f"Ficha de {datos['rue']} para {rue}")
    return datos, niveles


def fichas(rues: list[str], hilos: int, timeout: float) -> tuple[dict, dict[str, Exception]]:
    resultado, errores = {}, {}
    inicio = time.monotonic()
    with ThreadPoolExecutor(hilos) as ex:
        futuros = {ex.submit(ficha, rue, timeout): rue for rue in rues}
        for n, futuro in enumerate(as_completed(futuros), 1):
            rue = futuros[futuro]
            try:
                resultado[rue] = futuro.result()
            except (requests.RequestException, ValueError, AttributeError, FichaNoEncontrada) as e:
                errores[rue] = e
            if n % 1000 == 0 or n == len(futuros):
                log(f"Fichas {n}/{len(futuros)} ({time.monotonic() - inicio:.0f} s, {len(errores)} errores)")
    return resultado, errores


# Archivos ------------------------------------------------------------------


def leer_csv(ruta: Path) -> list[dict]:
    if not ruta.exists():
        return []
    with ruta.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def escribir_csv(ruta: Path, columnas: list[str], filas: list[dict]) -> None:
    temporal = ruta.with_name(ruta.name + ".tmp")
    with temporal.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columnas, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows({c: fila.get(c, "") for c in columnas} for fila in filas)
    temporal.replace(ruta)


def requiere_ficha(nueva: dict, anterior: dict | None) -> bool:
    if anterior is None:
        return True
    if any(nueva.get(c, "") != anterior.get(c, "") for c in LISTADO):
        return True
    return not anterior.get("_niveles") and not anterior.get("latitud")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--carpeta", type=Path, default=CARPETA, help="carpeta de unidades.csv y niveles.csv")
    p.add_argument("--completo", action="store_true", help="descarga todas las fichas")
    p.add_argument("--hilos", type=int, default=16, help="consultas de fichas en paralelo")
    p.add_argument("--hilos-listado", type=int, default=3, help="consultas del listado en paralelo")
    p.add_argument("--timeout", type=float, default=180)
    p.add_argument("--max-errores", type=float, default=0.02, help="fracción máxima de fichas con error")
    p.add_argument("--limite", type=int, help="descarga solo N fichas (prueba)")
    a = p.parse_args()
    ruta_unidades, ruta_niveles = a.carpeta / "unidades.csv", a.carpeta / "niveles.csv"

    anteriores = {f["rue"]: f for f in leer_csv(ruta_unidades)}
    for f in leer_csv(ruta_niveles):
        anteriores[f["rue"]].setdefault("_niveles", []).append(f["nivel"])

    unidades, fallidas = listado(a.hilos_listado, a.timeout)

    # Unidades que el listado no pudo dar: los códigos del reporte y las
    # unidades anteriores que no aparecen. Se completan con la ficha.
    sin_listado = set()
    if fallidas:
        sin_listado = (reporte(fallidas, a.timeout) | set(anteriores)) - set(unidades)
        unidades.update((rue, {"rue": rue}) for rue in sin_listado)

    log(f"Unidades: {len(unidades)} ({len(sin_listado)} sin listado); antes: {len(anteriores)}")
    if anteriores and len(unidades) < 0.95 * len(anteriores):
        sys.exit("Hay menos del 95 % de las unidades anteriores; no se escribe nada.")

    pendientes = sorted(
        (
            rue
            for rue, fila in unidades.items()
            if a.completo or rue in sin_listado or requiere_ficha(fila, anteriores.get(rue))
        ),
        key=int,
    )[: a.limite]
    log(f"Fichas por descargar: {len(pendientes)}")
    nuevas, errores = fichas(pendientes, a.hilos, a.timeout)

    for rue, error in sorted(errores.items(), key=lambda x: int(x[0])):
        log(f"Error en {rue}: {type(error).__name__}: {error}")
    for rue in list(unidades):
        fila = unidades[rue]
        if rue in nuevas:
            datos, fila["_niveles"] = nuevas[rue]
            fila.update(datos if rue in sin_listado else {c: datos[c] for c in FICHA})
        elif rue in anteriores and not isinstance(errores.get(rue), FichaNoEncontrada):
            fuente = anteriores[rue]
            fila.update({c: fuente.get(c, "") for c in (COLUMNAS if rue in sin_listado else FICHA)})
            fila["_niveles"] = fuente.get("_niveles", [])
        elif rue in sin_listado:
            del unidades[rue]

    graves = [r for r, e in errores.items() if not isinstance(e, FichaNoEncontrada)]
    if pendientes and len(graves) > a.max_errores * len(pendientes):
        sys.exit(f"{len(graves)} de {len(pendientes)} fichas con error; no se escribe nada.")

    filas = [unidades[rue] for rue in sorted(unidades, key=int)]
    escribir_csv(ruta_unidades, COLUMNAS, filas)
    escribir_csv(
        ruta_niveles,
        ["rue", "nivel"],
        [{"rue": f["rue"], "nivel": n} for f in filas for n in sorted(f.get("_niveles", []))],
    )
    log(
        f"Escrito: {len(unidades)} unidades, "
        f"{len(set(unidades) - set(anteriores))} nuevas, "
        f"{len(set(anteriores) - set(unidades))} retiradas, "
        f"{len(nuevas)} fichas descargadas, {len(errores)} errores"
    )


if __name__ == "__main__":
    main()
