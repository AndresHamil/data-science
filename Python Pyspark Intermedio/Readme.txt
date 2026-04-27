PYSPARK INTERMEDIO

Este proyecto contiene ejercicios de nivel intermedio para practicar PySpark con un enfoque mas cercano al trabajo real. La idea es dejar los ejemplos mas basicos y empezar a pensar en lectura de datos, limpieza, transformaciones, agrupaciones profesionales, joins y escritura de archivos.

--------------------------------------------------
LIBRERIA A INSTALAR

La libreria principal que necesitas instalar es:

pip install pyspark

Si quieres actualizarla:

pip install --upgrade pyspark

Nota:
PySpark tambien necesita Java para funcionar correctamente en muchos entornos. Si los scripts ejecutan sin problema, entonces tu entorno ya esta resolviendo esa parte.

--------------------------------------------------
URL DE GITHUB

Repositorio oficial de Apache Spark:
https://github.com/apache/spark

PySpark forma parte del proyecto Apache Spark.

--------------------------------------------------
MENTALIDAD CORRECTA

En esta fase cambia la forma de pensar:

No usar loops como herramienta principal para transformar datos.
No usar apply como en pandas.
Pensar en transformaciones sobre columnas.
Pensar en pipelines de trabajo.
Trabajar como si los datos vinieran de archivos o procesos reales.

--------------------------------------------------
QUE ES PYSPARK PRO

En este nivel, PySpark se usa como una herramienta para construir procesos de datos mas cercanos a empresa. Ya no solo se trata de ver un DataFrame en pantalla, sino de limpiar datos, enriquecerlos, unir datasets, agregar resultados y guardar salidas en formatos utiles como CSV o Parquet.

--------------------------------------------------
TOPICOS DE ESTE PROYECTO

1. Leer datos (modo real)
En este ejercicio se simula la lectura de un CSV como en un entorno de trabajo. Se muestran los datos, el schema y el conteo total de registros.

2. Limpieza de datos
Aqui se trabajan casos comunes de data cleaning: eliminar nulos, rellenar valores faltantes y quitar registros duplicados.

3. Transformaciones
En este tema se crean nuevas columnas con withColumn y logica condicional con when. Es una parte clave de PySpark profesional.

4. Agrupaciones PRO
Este ejercicio se enfoca en analisis real con groupBy para obtener promedios, conteos y maximos por ciudad.

5. Joins
Aqui se unen datasets relacionados, por ejemplo clientes y ventas. Este tema es muy importante porque aparece mucho en trabajo real.

6. Lectura y escritura de archivos
En este ejercicio se leen datos desde CSV y luego se escriben resultados en CSV y Parquet.

7. Mini proyecto
Este reto final simula un caso de negocio con ventas. Se calcula el total por cliente, el cliente que mas compro y el promedio de ventas.

--------------------------------------------------
ARCHIVOS SIMULADOS

Esta carpeta incluye archivos CSV de ejemplo dentro de la carpeta datos para trabajar de una manera mas parecida a un entorno real:

empleados.csv
clientes.csv
ventas.csv

Los ejercicios leen estos archivos directamente para practicar lectura, limpieza, joins, transformaciones y escritura de resultados.

--------------------------------------------------
OBJETIVO GENERAL

El objetivo de estos ejercicios es subir de nivel en PySpark y empezar a trabajar con patrones mas reales de procesamiento de datos. Despues de completar esta fase, tendras mejor base para pipelines, transformaciones de negocio, joins, lectura de archivos y analisis mas utiles en entornos profesionales.