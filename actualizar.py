#!/usr/bin/env python

import requests
from requests.adapters import HTTPAdapter
import pandas as pd
from bs4 import BeautifulSoup
from collections import ChainMap
from tqdm import tqdm

RAW = "data/unidades_educativas_raw.csv"
ESTADO = "data/unidades_educativas_estado.csv"
TIEMPO = "data/unidades_educativas_tiempo.csv"
MAX_RETRIES = 20
TIMEOUT = 30


def iniciar_sesion() -> requests.sessions.Session:
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=MAX_RETRIES))
    session.mount("https://", HTTPAdapter(max_retries=MAX_RETRIES))
    return session


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
    response = sesion.get(
        "http://seie.minedu.gob.bo:8080/geoserver/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=minedu:vw_unidad_geo7&outputFormat=json",
        timeout=TIMEOUT,
    )
    indice = pd.DataFrame(
        [feature["properties"] for feature in response.json()["features"]]
    )

    # Construir datos para cada unidad
    for u, unidad in tqdm(indice.iterrows(), total=len(indice)):

        try:
            datos_unidad = []

            response = sesion.get(
                f"http://seie.minedu.gob.bo/reportes/mapas_unidades_educativas/ficha/ver/{unidad['cod_ue'].strip()}",
                timeout=TIMEOUT,
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
                tabla = pd.read_html(str(nodos[i]), decimal=',', thousands='.')[0]
                tabla = tabla.set_index("Sexo").unstack()
                tabla.index = tabla.index.map(lambda x: f"{campo}_{x[1].lower()}_{x[0]}")
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
                    f'{campo}_{li.contents[0].get_text().strip().replace(" ", "_")}'
                    if len(li.contents) > 1
                    else campo: li.contents[-1].get_text().strip()
                    for li in nodos[i].select("li")
                }
                datos_unidad.append(lista)

            unidades.append({**unidad.to_dict(), **dict(ChainMap(*datos_unidad))})
        except Exception as e:
            print(f'{u}. RUE {unidad["cod_ue"].strip()}\n{e}')
    return pd.DataFrame(unidades)


def archivar_datos(datos: pd.core.frame.DataFrame):
    # Guardar datos sin más procesamiento
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
        estado[col] = pd.to_numeric(estado[col].astype(str).str.replace("--", "0"))
    for col in ["latitud", "longitud"]:
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
        estado[col] = estado[col].map({"SI": True, "--": False})

    estado.set_index("codigo_rue").to_csv(ESTADO)

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
    tiempo = tiempo.dropna()
    tiempo["valor"] = (
        tiempo["valor"]
        .astype(str)
        .str.replace("\.0$", "", regex=True)
        .str.replace("\.", "", regex=True)
        .astype(int)
    )

    tiempo.to_csv(TIEMPO, index=False)


sesion = iniciar_sesion()
datos = construir_datos()
archivar_datos(datos)
