#!/usr/bin/env python

import pandas as pd

estado = pd.read_csv("data/unidades_educativas_estado.csv")
tiempo = pd.read_csv("data/unidades_educativas_tiempo.csv")

anios = [2017, 2018, 2019]
total = estado.shape[0]
area = estado.area.value_counts().to_dict()


def procesar_rural_urbano(df, total=total, area=area):
    df["total"] = df.sum(axis=1)
    df = pd.concat(
        [
            df[col].div(t)
            for col, t in zip(["R", "U", "total"], [area["R"], area["U"], total])
        ],
        axis=1,
    )
    df.columns = ["Rural", "Urbano", "Total"]
    df.index = df.index.str.lower()
    df.index.name = None
    return df


def desempeño(df, años):
    trayectoria = []
    for año in años:
        dfi = df[(df.anio == año)].pivot_table(
            index="codigo_rue", columns="variable", values="valor"
        )
        dfi = dfi[dfi.matricula > 0].dropna().astype(int)
        dfi = dfi[(dfi.promovidos + dfi.reprobados + dfi.abandono) == dfi.matricula]
        trayectoria.append(
            dfi[["promovidos", "reprobados", "abandono"]]
            .div(dfi.matricula, axis=0)
            .mean(axis=0)
            .rename(año)
        )
    trayectoria = pd.concat(trayectoria, axis=1)
    trayectoria.index.name = None
    return trayectoria


unico = {
    i: estado[f"{i}_codigo"].unique().shape[0]
    for i in ["departamento", "provincia", "municipio", "distrito_educativo"]
}
niveles = procesar_rural_urbano(
    pd.DataFrame(
        {
            area: {
                nivel: estado[estado.area == area].nivel.str.contains(nivel).sum()
                for nivel in ["Inicial", "Primaria", "Secundaria"]
            }
            for area in ["R", "U"]
        }
    )
)
dependencias = procesar_rural_urbano(
    estado[["area", "dependencia"]]
    .value_counts()
    .reset_index()
    .pivot_table(index="dependencia", columns="area", values="count")
)
infraestructura_columns = [
    "viviendas_maestros",
    "bachillerato_humanistico_numero_de_talleres",
    "ambientes_administrativos_dirección",
    "ambientes_administrativos_secretaría",
    "ambientes_administrativos_sala_de_reuniones",
    "ambientes_deportivos_numero_de_canchas",
    "ambientes_deportivos_numero_de_gimnasios",
    "ambientes_deportivos_numero_de_coliseos",
    "ambientes_deportivos_numero_de_piscinas",
    "ambientes_pedagogicos_numero_de_aulas",
    "ambientes_pedagogicos_numero_de_laboratorios",
    "ambientes_pedagogicos_numero_de_bibliotecas",
    "ambientes_pedagogicos_numero_de_salas_de_computación",
    "agua",
    "energia_electrica",
    "baterias_de_bano",
    "internet",
]

infraestructura = procesar_rural_urbano(
    pd.concat(
        [
            estado[estado[i] > 0].area.value_counts().rename(i)
            for i in infraestructura_columns
        ],
        axis=1,
    )
    .T.fillna(0)
    .astype(int)
)

estudiantes = (
    tiempo[tiempo.sexo == "total"][["variable", "anio"]]
    .value_counts()
    .reset_index()
    .pivot_table(index="anio", columns="variable", values="count")
    .fillna(0)
    .astype(int)
    .div(total)
)
estudiantes.index.name = None

al_menos_1_matriculado = (
    pd.DataFrame(
        tiempo[
            (tiempo.sexo == "total")
            & (tiempo.variable == "matricula")
            & (tiempo.valor > 0)
        ].anio.value_counts()
    )
    .rename(columns={"count": "unidades con al menos 1 matriculado"})
    .sort_index()
)

descripcion = f"""

# Datos de unidades educativas en Bolivia

Información sobre **{total} unidades educativas** en 

- {unico['departamento']} departamentos, 
- {unico['provincia']} provincias,
- {unico['municipio']} municipios y 
- {unico['distrito_educativo']} distritos educativos de Bolivia.

Recolectada del [Sistema de Estadísticas e Indicadores Educativos](https://seie.minedu.gob.bo/).

## Características de unidades

{area["R"]} ({area["R"]/total:.0%}) de ellas se encuentra en el **área rural** y {area["U"]} ({area["U"]/total:.0%}) en el **área urbana**.

Cada unidad ofrece cursos en una combinación de **niveles educativos**: iniciales, primarios y secundarios. La proporción de unidades por área que imparte un nivel es:

{niveles.to_markdown(floatfmt=".0%")}

Una unidad puede ser **fiscal o privada**. La proporción por área es:

{dependencias.to_markdown(floatfmt=".0%")}

La infraestructura de una unidad incluye diversos **servicios y ambientes**. La proporción por área es la siguiente:

{infraestructura.to_markdown(floatfmt=".0%")}

Más información sobre características de cada unidad: [datos/unidades_educativas_estado.csv](datos/unidades_educativas_estado.csv)

## Características de estudiantes

Cada año, las unidades registran **estudiantes que se matriculan, que aprueban, reprueban o abandonan** un curso. La proporción de unidades que reportan estos valores en el sistema público es:

{estudiantes.to_markdown(floatfmt=".0%")}

En muchos casos, particularmente 2016, el sistema reporta 0 matriculados. El número de unidades que reportan **al menos 1 estudiante matriculado** por año es:

{al_menos_1_matriculado.to_markdown()}

Estos valores pueden servir para evaluar el **desempeño** de los estudiantes y la unidad. Por ejemplo, la proporción de estudiantes matriculados que aprueban, reprueban o abandonan el año escolar en los periodos y unidades donde es razonable hacer comparaciones es:

{desempeño(tiempo[tiempo.sexo == 'total'], anios).to_markdown(floatfmt=".1%")}

Sólo entre mujeres:

{desempeño(tiempo[tiempo.sexo == 'mujer'], anios).to_markdown(floatfmt=".1%")}

Y entre hombres:

{desempeño(tiempo[tiempo.sexo == 'hombre'], anios).to_markdown(floatfmt=".1%")}

En unidades del área rural:

{desempeño(tiempo[(tiempo.sexo == 'total') & (tiempo.codigo_rue.isin(estado[estado.area == 'R'].codigo_rue))], anios).to_markdown(floatfmt=".1%")}

Y del área urbana:

{desempeño(tiempo[(tiempo.sexo == 'total') & (tiempo.codigo_rue.isin(estado[estado.area == 'U'].codigo_rue))], anios).to_markdown(floatfmt=".1%")}

Más información sobre características de estudiantes: [datos/unidades_educativas_tiempo.csv](datos/unidades_educativas_tiempo.csv)

"""

with open("readme.md", "w+") as f:
    f.write(descripcion)
