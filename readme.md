# Unidades educativas de Bolivia

Este repositorio guarda los datos públicos del Ministerio de Educación de Bolivia sobre las unidades educativas del país y sus estudiantes. Los datos vienen del [Registro de Unidades Educativas (RUE)](https://rue.sie.gob.bo/accesopub/consultarue/) y de los [reportes estadísticos del Sistema de Información Educativa (SIE)](https://reportes.sie.gob.bo/reporteestadistico/). Una [GitHub Action](.github/workflows/actualizar.yml) consulta las fuentes cada semana y actualiza cuatro tablas. El archivo [`datapackage.json`](datapackage.json) describe cada tabla, cada campo y las precauciones de uso.

## Qué es

### Qué contiene cada tabla

- [`unidades.csv`](unidades.csv) es el registro de las unidades educativas, abiertas y cerradas. Cada fila es una unidad con su nombre, su local, su estado, su dependencia, su resolución de creación y las coordenadas de su local.
- [`niveles.csv`](niveles.csv) es la lista de niveles que la resolución de cada unidad autoriza. Cada fila es una unidad y un nivel.
- [`matricula.csv`](matricula.csv) es el número de estudiantes efectivos de Educación Regular. Cada fila es una gestión, una unidad, un nivel, un grado y un sexo.
- [`flujo.csv`](flujo.csv) es el movimiento de estudiantes de Educación Regular durante la gestión: inscritos, no incorporados, traslados, abandono y efectivos. Cada fila es una gestión y una unidad.

El código RUE (`rue`) identifica a cada unidad y une las cuatro tablas.

### Cómo interpretar los datos

Una **unidad educativa** es una escuela o un colegio con un código RUE propio. Un **local educativo** (`le`) es el edificio donde funciona la unidad. Varias unidades pueden compartir un local. Las coordenadas son del local.

Una unidad está **abierta** si funcionaba en la gestión vigente el día de la consulta. La **gestión** es el año escolar.

Un **nivel autorizado** es un nivel que la resolución de la unidad permite ofrecer. Una unidad puede tener un nivel autorizado y no tener estudiantes en ese nivel. Para saber qué niveles funcionan, use la matrícula.

La matrícula y el flujo cubren solo **Educación Regular**, que tiene tres niveles: inicial, primaria y secundaria. Inicial tiene dos grados, probablemente pre-kínder y kínder. Primaria y secundaria tienen seis grados cada una.

El SIE publica solo la gestión vigente y no indica una fecha de corte. Por eso, la matrícula y el flujo muestran el estado del sistema el día de la consulta. Los números cambian durante el año escolar. Cuando el SIE publica una gestión nueva, las tablas la agregan y conservan las gestiones anteriores.

El flujo usa estas definiciones del Ministerio:

- **Inscritos**: estudiantes que se inscribieron en la unidad.
- **No incorporados**: estudiantes que se inscribieron pero nunca asistieron a clases.
- **Traslado**: estudiantes que dejaron la unidad para inscribirse en otra.
- **Abandono**: estudiantes que asistieron, dejaron la unidad y no se inscribieron en otra.
- **Efectivos**: inscritos menos no incorporados y menos traslados. Los efectivos incluyen a los estudiantes que después abandonaron.

### Cobertura

`unidades.csv` y `niveles.csv` cubren todas las unidades que publica el RUE: abiertas y cerradas, de Educación Regular, Alternativa y Especial, y de todas las dependencias. `matricula.csv` y `flujo.csv` cubren las unidades de Educación Regular con estudiantes desde la gestión 2026. Para años anteriores, vea [Datos anteriores](#datos-anteriores). Las tablas no tienen departamento ni municipio. Para agrupar por territorio, use las coordenadas.

### Datos anteriores

Una versión anterior de este repositorio tenía dos tablas del [Sistema de Estadísticas e Indicadores Educativos](https://seie.minedu.gob.bo/), con datos hasta 2024. Las tablas actuales no las actualizan, pero el historial de git las conserva:

- [`unidades_educativas_tiempo.csv`](https://github.com/mauforonda/unidades_educativas_bolivia/blob/1388643e81c76864f7ee1fb70ee4ff02578bad73/data/unidades_educativas_tiempo.csv) es la serie histórica de matrícula, abandono, promovidos y reprobados por unidad, sexo y año, de 2014 a 2024. Las cifras suman todos los niveles de la unidad.
- [`unidades_educativas_estado.csv`](https://github.com/mauforonda/unidades_educativas_bolivia/blob/1388643e81c76864f7ee1fb70ee4ff02578bad73/data/unidades_educativas_estado.csv) es la tabla de características de los establecimientos. Cada fila es una unidad con su ubicación administrativa, área, ambientes, servicios básicos y contacto.

El [`readme.md`](https://github.com/mauforonda/unidades_educativas_bolivia/blob/1388643e81c76864f7ee1fb70ee4ff02578bad73/readme.md) y el [`datapackage.json`](https://github.com/mauforonda/unidades_educativas_bolivia/blob/1388643e81c76864f7ee1fb70ee4ff02578bad73/datapackage.json) de esa versión describen las dos tablas.

### Preguntas que las tablas ayudan a responder

- ¿Cuántos estudiantes hay en cada nivel, grado y sexo, y cómo cambia ese número entre gestiones?
- ¿Dónde están las unidades educativas y qué zonas tienen pocas unidades de secundaria?
- ¿Qué unidades pierden más estudiantes por abandono o por traslado?
- ¿Cómo se distribuyen los estudiantes entre unidades fiscales, privadas y de convenio?
- ¿Qué unidades tienen niveles autorizados que no ofrecen?

## Cómo usarlo

Lea primero [`datapackage.json`](datapackage.json). Describe cada campo, trae las advertencias (`advertencias`) y reúne las mismas consultas de abajo (`ejemplos`). Consulte las tablas con [DuckDB](https://duckdb.org/) desde la carpeta donde están los CSV. Las consultas de abajo están completas y funcionan tal como están, salvo la unión espacial, que necesita un archivo de límites.

```sql
SELECT * FROM 'unidades.csv' LIMIT 5;
```

Elija la recomendación que corresponde a su pregunta.

### Si necesita contar unidades

Filtre las unidades abiertas con `abierta`. La tabla también tiene las unidades cerradas.

```sql
SELECT dependencia, count(*) AS unidades
FROM 'unidades.csv'
WHERE abierta
GROUP BY dependencia
ORDER BY unidades DESC;
```

### Si necesita contar estudiantes

Sume `estudiantes` y no cuente filas. Filtre siempre por `gestion`, porque la tabla acumula gestiones. Una combinación de unidad, nivel, grado y sexo sin estudiantes no tiene fila.

```sql
SELECT nivel, grado, sexo, sum(estudiantes) AS estudiantes
FROM 'matricula.csv'
WHERE gestion = (SELECT max(gestion) FROM 'matricula.csv')
GROUP BY ALL
ORDER BY ALL;
```

La suma de `estudiantes` de una unidad es igual a `efectivos` en `flujo.csv`, salvo en unas pocas unidades.

### Si necesita saber qué niveles ofrece una unidad

Use `matricula.csv` para los niveles con estudiantes y `niveles.csv` para los niveles autorizados. Las dos tablas nombran los niveles de forma distinta: `inicial`, `primaria` y `secundaria` en `matricula.csv`, y `Inicial en Familia Comunitaria`, `Primaria Comunitaria Vocacional` y `Secundaria Comunitaria Productiva` en `niveles.csv`. Esta consulta cuenta las unidades abiertas con secundaria autorizada y sin estudiantes de secundaria.

```sql
SELECT count(*) AS unidades
FROM 'unidades.csv' u
WHERE u.abierta
  AND u.rue IN (SELECT rue FROM 'niveles.csv' WHERE nivel = 'Secundaria Comunitaria Productiva')
  AND u.rue NOT IN (
      SELECT rue FROM 'matricula.csv'
      WHERE nivel = 'secundaria' AND gestion = (SELECT max(gestion) FROM 'matricula.csv')
  );
```

`niveles.csv` también tiene los niveles de Educación Alternativa y Especial. Esta consulta los lista.

```sql
SELECT nivel, count(*) AS unidades
FROM 'niveles.csv'
GROUP BY nivel
ORDER BY unidades DESC;
```

### Si necesita medir abandono o traslados

Use `flujo.csv`. `efectivos` no resta el abandono, así que no lo use como el número de estudiantes al final del año. Esta consulta calcula el abandono como porcentaje de los efectivos, por dependencia. Indique en su análisis qué denominador usó.

```sql
SELECT u.dependencia,
       sum(f.abandono) AS abandono,
       sum(f.traslado) AS traslado,
       sum(f.efectivos) AS efectivos,
       round(100.0 * sum(f.abandono) / sum(f.efectivos), 2) AS abandono_pct
FROM 'flujo.csv' f
JOIN 'unidades.csv' u USING (rue)
WHERE f.gestion = (SELECT max(gestion) FROM 'flujo.csv')
GROUP BY u.dependencia
ORDER BY abandono_pct DESC;
```

En la mayoría de las unidades, `efectivos = inscritos - no_incorporados - traslado`. En algunas unidades la fuente no cumple esta relación. Las tablas conservan los valores de la fuente. Esta consulta cuenta esas unidades.

```sql
SELECT count(*) FILTER (WHERE efectivos <> inscritos - no_incorporados - traslado) AS distintas,
       count(*) AS unidades
FROM 'flujo.csv'
WHERE gestion = (SELECT max(gestion) FROM 'flujo.csv');
```

### Si necesita ubicar unidades en un mapa

Use `longitud` y `latitud`. Las coordenadas son del local, así que las unidades de un mismo local caen en el mismo punto. Para contar edificios y no unidades, agrupe por `le`. Algunas unidades no tienen coordenadas.

```sql
SELECT u.le,
       any_value(u.longitud) AS longitud,
       any_value(u.latitud) AS latitud,
       count(DISTINCT u.rue) AS unidades,
       sum(m.estudiantes) AS estudiantes
FROM 'unidades.csv' u
JOIN 'matricula.csv' m USING (rue)
WHERE m.gestion = (SELECT max(gestion) FROM 'matricula.csv')
  AND u.longitud IS NOT NULL
GROUP BY u.le
ORDER BY u.le;
```

Para agrupar por departamento o municipio, una las coordenadas con límites administrativos de la fecha que necesite. Con la extensión `spatial` de DuckDB, la unión tiene esta forma (`municipios.geojson` es un archivo suyo):

```sql
INSTALL spatial;
LOAD spatial;
SELECT m.*, u.rue
FROM 'unidades.csv' u
JOIN ST_Read('municipios.geojson') m
  ON ST_Contains(m.geom, ST_Point(u.longitud, u.latitud));
```

### Si necesita comparar gestiones

Compare gestiones con el mismo momento del año. Los datos de la gestión vigente cambian cada semana. Las cifras de una gestión anterior son las de la última consulta en que el SIE publicó esa gestión. El [historial de git](https://github.com/mauforonda/unidades_educativas_bolivia/commits/master/matricula.csv) de `matricula.csv` y `flujo.csv` conserva cada versión semanal.

```sql
SELECT gestion, count(DISTINCT rue) AS unidades, sum(estudiantes) AS estudiantes
FROM 'matricula.csv'
GROUP BY gestion
ORDER BY gestion;
```

### Si necesita trabajar con fechas de creación

Use `fecha_resolucion`. Es la fecha de la resolución que creó la unidad. Está vacía cuando la fuente no tiene una fecha válida.

```sql
SELECT decada, count(*) AS unidades
FROM (SELECT year(fecha_resolucion) // 10 * 10 AS decada FROM 'unidades.csv' WHERE fecha_resolucion IS NOT NULL)
GROUP BY decada
ORDER BY decada;
```

## Cómo actualizarlo

El workflow [`actualizar.yml`](.github/workflows/actualizar.yml) actualiza las tablas cada lunes. Para actualizarlas a mano:

1. Instale las dependencias con Python 3.12.

   ```bash
   pip install -r requirements.txt
   ```

2. Actualice `unidades.csv` y `niveles.csv`.

   ```bash
   python actualizar.py --completo
   ```

3. Actualice `matricula.csv` y `flujo.csv`.

   ```bash
   python actualizar_matricula.py
   ```

4. Valide las tablas con el esquema de `datapackage.json`.

   ```bash
   pip install frictionless
   frictionless validate datapackage.json
   ```

[`actualizar.py`](actualizar.py) descarga el listado público del RUE, con una consulta por departamento y estado. Si el servidor no responde a una de esas consultas, el script obtiene los códigos RUE de esa parte con el reporte por departamento del RUE. Después descarga la ficha de cada unidad, que tiene la resolución, las coordenadas y los niveles. Sin `--completo`, el script descarga solo las fichas de las unidades nuevas o con cambios en el listado. Ese modo es más rápido, pero no detecta cambios que aparecen solo en la ficha, como las coordenadas.

[`actualizar_matricula.py`](actualizar_matricula.py) lee la gestión vigente en el SIE. El script usa la gestión que publica el SIE y no la fecha del sistema, así que funciona aunque el Ministerio publique la gestión nueva tarde. Después descarga el reporte de matrícula de cada distrito educativo y la página de flujo de cada unidad. También consulta la gestión anterior. Si el SIE todavía la publica, el script actualiza sus filas. El script reemplaza solo las filas de las gestiones que descarga. El flujo necesita una consulta por unidad y es la parte más lenta. Con `--sin-flujo`, el script actualiza solo la matrícula.

`actualizar.py` convierte los textos como "SIN DATO" en valores vacíos, valida las fechas y borra las coordenadas fuera de Bolivia. Los dos scripts ordenan las filas por `rue`, así que dos consultas con los mismos datos producen archivos idénticos. `actualizar.py` no escribe ningún archivo si encuentra menos del 95 % de las unidades anteriores. Los dos scripts no escriben ningún archivo si más del 2 % de las consultas fallan, y `actualizar_matricula.py` tampoco escribe si falla el reporte de un distrito. Cada script escribe primero un archivo temporal y después reemplaza el anterior. `--help` muestra todas las opciones.

En el workflow, los dos scripts corren al mismo tiempo en jobs separados. Un último job guarda los archivos de los jobs que terminaron bien y hace un commit si algo cambió.
