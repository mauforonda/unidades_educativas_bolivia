#!/usr/bin/env python

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import InsecureRequestWarning
import pandas as pd
from bs4 import BeautifulSoup
from collections import ChainMap
from io import StringIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import argparse

RAW = "data/unidades_educativas_raw.csv"
ESTADO = "data/unidades_educativas_estado.csv"
TIEMPO = "data/unidades_educativas_tiempo.csv"
MAX_RETRIES = 20
TIMEOUT = 30
MAX_WORKERS = 12
PAGE_SIZE = 50

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def iniciar_sesion() -> requests.sessions.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def asegurar_respuesta_ok(response: requests.Response, contexto: str):
    if response.ok:
        return
    try:
        detalle = response.text[:300].replace("\n", " ")
    except Exception:
        detalle = "<sin respuesta>"
    print(f"[ERROR] {contexto}: HTTP {response.status_code} {detalle}")
    response.raise_for_status()


def construir_datos() -> pd.core.frame.DataFrame:
    unidades = []

    selectores = {
        "general": ".dl-horizontal dd",
        "estudiantes": "#data-table-sexo",
        "infraestructura": ".box .info-box-content h3",
        "listas": "ul.tama",
    }

    campos = {
        "general": [
            "director",
            "direccion",
            "telefono",
            "dependencia",
            "nivel",
            "turno",
        ],
        "estudiantes": [
            "matricula",
            "promovidos",
            "reprobados",
            "abandono",
        ],
        "infraestructura": [
            "agua",
            "energia_electrica",
            "baterias_de_bano",
            "internet",
        ],
        "listas": [
            "ambientes_pedagogicos",
            "ambientes_deportivos",
            "ambientes_administrativos",
            "bachillerato_humanistico",
            "viviendas_maestros",
        ],
    }

    # Construir el índice de unidades
    print("Contruyendo un índice de unidades educativas ...")
    response = sesion.get(
        "https://seie.minedu.gob.bo:8443/geoserver/minedu/ows",
        params={
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": "minedu:vw_unidad_geo7",
            "outputFormat": "application/json",
        },
        timeout=TIMEOUT,
        verify=False,
    )
    indice = pd.DataFrame(
        [feature["properties"] for feature in response.json()["features"]]
    )

    print(f"Existen {len(indice)} unidades educativas listadas.")

    thread_local = threading.local()

    def obtener_sesion() -> requests.sessions.Session:
        if not hasattr(thread_local, "sesion"):
            thread_local.sesion = iniciar_sesion()
        return thread_local.sesion

    def procesar_unidad(u: int, unidad: pd.Series):
        try:
            datos_unidad = []
            sesion_local = obtener_sesion()
            response = sesion_local.get(
                f"http://seie.minedu.gob.bo/reportes/mapas_unidades_educativas/ficha/ver/{unidad['cod_ue'].strip()}",
                timeout=TIMEOUT,
                verify=False,
            )
            asegurar_respuesta_ok(
                response, f"Ficha unidad RUE {unidad['cod_ue'].strip()}"
            )
            html = BeautifulSoup(response.text, "html.parser")

            # Datos generales
            datos_generales = {}
            nodos = html.select(selectores["general"])
            for i, campo in enumerate(campos["general"]):
                datos_generales[campo] = nodos[i].get_text().strip()
            datos_unidad.append(datos_generales)

            # Estadísticas de estudiantes
            nodos = html.select(selectores["estudiantes"])
            for i, campo in enumerate(campos["estudiantes"]):
                tabla = pd.read_html(StringIO(str(nodos[i])))[0]
                tabla = tabla.set_index("Sexo").unstack()
                tabla.index = tabla.index.map(
                    lambda x: f"{campo}_{x[1].lower()}_{x[0]}"
                )
                datos_unidad.append(tabla.to_dict())

            # Datos de infraestructura
            datos_infraestructura = {}
            nodos = html.select(selectores["infraestructura"])
            for i, campo in enumerate(campos["infraestructura"]):
                datos_infraestructura[campo] = nodos[i].get_text().strip()
            datos_unidad.append(datos_infraestructura)

            nodos = html.select(selectores["listas"])
            for i, campo in enumerate(campos["listas"]):
                lista = {
                    f"{campo}_{li.contents[0].get_text().strip().replace(' ', '_')}"
                    if len(li.contents) > 1
                    else campo: li.contents[-1].get_text().strip()
                    for li in nodos[i].select("li")
                }
                datos_unidad.append(lista)

            return {**unidad.to_dict(), **dict(ChainMap(*datos_unidad))}
        except Exception as e:
            return {
                "_error": f"{u}. RUE {unidad['cod_ue'].strip()}\n{e}",
            }

    # Construir datos para cada unidad en paralelo

    print("Consultando información para cada unidad ...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {
            executor.submit(procesar_unidad, u, unidad): (u, unidad)
            for u, unidad in indice.iterrows()
        }
        for futuro in tqdm(as_completed(futuros), total=len(futuros)):
            resultado = futuro.result()
            if "_error" in resultado:
                print(resultado["_error"])
            else:
                unidades.append(resultado)
    return pd.DataFrame(unidades)


def archivar_datos(datos: pd.core.frame.DataFrame):
    # Guardar datos sin más procesamiento
    print("Archivando datos ...")
    datos.to_csv(RAW, index=False)

    # Construir la tabla que describe a unidades
    estado = datos[[col for col in datos.columns if "20" not in col]].copy()
    estado.columns = [
        "geoserver_id",
        "departamento_codigo",
        "departamento",
        "provincia_codigo",
        "provincia",
        "municipio_codigo",
        "municipio",
        "distrito_educativo_codigo",
        "distrito_educativo",
        "cod_le",
        "codigo_rue",
    ] + [
        col.lower().replace("nº", "numero").replace(":", "").strip()
        for col in estado.columns[11:]
    ]

    # Acomodar tipos de datos
    for col in [
        "codigo_rue",
        "geoserver_id",
        "departamento_codigo",
        "provincia_codigo",
        "municipio_codigo",
        "distrito_educativo_codigo",
        "turnoals",
        "depend",
        "ambientes_pedagogicos_numero_de_aulas",
        "ambientes_pedagogicos_numero_de_laboratorios",
        "ambientes_pedagogicos_numero_de_bibliotecas",
        "ambientes_pedagogicos_numero_de_salas_de_computación",
        "ambientes_deportivos_numero_de_canchas",
        "ambientes_deportivos_numero_de_gimnasios",
        "ambientes_deportivos_numero_de_coliseos",
        "ambientes_deportivos_numero_de_piscinas",
        "bachillerato_humanistico_numero_de_talleres",
    ]:
        if col in estado.columns:
            estado[col] = pd.to_numeric(estado[col].astype(str).str.replace("--", "0"))
    for col in ["latitud", "longitud"]:
        if col in estado.columns:
            estado[col] = pd.to_numeric(estado[col])
    for col in [
        "viviendas_maestros",
        "agua",
        "energia_electrica",
        "baterias_de_bano",
        "internet",
        "ambientes_administrativos_dirección",
        "ambientes_administrativos_secretaría",
        "ambientes_administrativos_sala_de_reuniones",
    ]:
        if col in estado.columns:
            estado[col] = estado[col].map({"SI": True, "--": False})

    estado = estado.set_index("codigo_rue")
    estado.index = pd.to_numeric(estado.index, errors="coerce").astype("Int64")

    resumen_estado = None
    if os.path.exists(ESTADO):
        estado_existente = pd.read_csv(ESTADO, index_col="codigo_rue")
        estado_existente.index = pd.to_numeric(
            estado_existente.index, errors="coerce"
        ).astype("Int64")
        columnas_base = list(estado_existente.columns)
        columnas_nuevas = [col for col in estado.columns if col not in columnas_base]
        estado_existente = estado_existente.reindex(
            columns=columnas_base + columnas_nuevas
        )
        for col in columnas_nuevas:
            estado_existente[col] = pd.NA
        columnas_comunes = [
            col for col in estado.columns if col in estado_existente.columns
        ]
        comunes = estado.index.intersection(estado_existente.index)
        if columnas_comunes and len(comunes) > 0:
            left = estado.loc[comunes, columnas_comunes]
            right = estado_existente.loc[comunes, columnas_comunes]
            cambios = (left.ne(right) & ~(left.isna() & right.isna())).any(axis=1)
            resumen_estado = {
                "actualizadas": int(cambios.sum()),
                "nuevas": int(estado.index.difference(estado_existente.index).size),
            }
        estado_existente.update(estado)
        faltantes = estado.loc[~estado.index.isin(estado_existente.index)]
        estado = pd.concat([estado_existente, faltantes], axis=0)

    estado.to_csv(ESTADO)

    # Construir la tabla con estadísticas anuales de estudiantes
    tiempo = []
    datos_tiempo = datos[["cod_ue"] + [col for col in datos.columns if "20" in col]]
    for col in datos_tiempo.columns[1:]:
        s = col.split("_")
        segmento = datos_tiempo[["cod_ue", col]].rename(columns={col: "valor"})
        for i, nombre in enumerate(["variable", "sexo", "anio"]):
            segmento.insert(i + 1, nombre, s[i])
        tiempo.append(segmento)
    tiempo = pd.concat(tiempo)
    tiempo = tiempo.rename(columns={"cod_ue": "codigo_rue"})
    tiempo["codigo_rue"] = pd.to_numeric(tiempo["codigo_rue"], errors="coerce").astype(
        "Int64"
    )
    tiempo["anio"] = pd.to_numeric(
        tiempo["anio"].str.extract(r"([0-9]*)")[0], errors="coerce"
    ).astype("Int64")
    tiempo = tiempo.dropna()
    tiempo["valor"] = (
        tiempo["valor"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"\.", "", regex=True)
        .astype(int)
    )

    resumen_tiempo = None
    if os.path.exists(TIEMPO):
        tiempo_existente = pd.read_csv(TIEMPO)
        tiempo_existente["codigo_rue"] = pd.to_numeric(
            tiempo_existente["codigo_rue"], errors="coerce"
        ).astype("Int64")
        tiempo_existente["anio"] = pd.to_numeric(
            tiempo_existente["anio"], errors="coerce"
        ).astype("Int64")
        claves = ["codigo_rue", "variable", "sexo", "anio"]
        existentes = tiempo_existente[claves].drop_duplicates()
        nuevas = tiempo[claves]
        comparacion = nuevas.merge(existentes, on=claves, how="left", indicator=True)
        resumen_tiempo = {
            "nuevas": int((comparacion["_merge"] == "left_only").sum()),
            "actualizadas": int((comparacion["_merge"] == "both").sum()),
        }
        tiempo = pd.concat([tiempo_existente, tiempo], ignore_index=True)
        tiempo = tiempo.drop_duplicates(
            subset=["codigo_rue", "variable", "sexo", "anio"], keep="last"
        )

    tiempo.to_csv(TIEMPO, index=False)
    if resumen_estado:
        print(
            "Resumen estado:",
            f"{resumen_estado['actualizadas']} actualizadas,",
            f"{resumen_estado['nuevas']} nuevas.",
        )
    if resumen_tiempo:
        print(
            "Resumen tiempo:",
            f"{resumen_tiempo['actualizadas']} actualizadas,",
            f"{resumen_tiempo['nuevas']} nuevas.",
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-raw",
        action="store_true",
        help="Usa data/unidades_educativas_raw.csv y salta la descarga.",
    )
    args = parser.parse_args()

    if args.from_raw:
        if not os.path.exists(RAW):
            raise FileNotFoundError(f"No existe el archivo RAW: {RAW}")
        print(f"Cargando datos desde {RAW} ...")
        datos = pd.read_csv(RAW)
    else:
        sesion = iniciar_sesion()
        datos = construir_datos()

    archivar_datos(datos)


if __name__ == "__main__":
    main()
