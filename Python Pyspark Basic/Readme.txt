PYSPARK BASICO

Este proyecto contiene ejercicios introductorios para practicar PySpark con DataFrames. La idea es aprender las operaciones mas comunes paso a paso: crear datos, filtrarlos, seleccionar columnas, crear nuevas columnas, agrupar, ordenar y resolver un mini reto final.

--------------------------------------------------
LIBRERIA A INSTALAR

La libreria principal que necesitas instalar es:

pip install pyspark

Si quieres actualizarla:

pip install --upgrade pyspark

Nota:
PySpark tambien necesita Java para funcionar correctamente en muchos entornos. Si ya te ejecutaron los scripts, entonces tu entorno ya esta resolviendo esa parte.

--------------------------------------------------
URL DE GITHUB

Repositorio oficial de Apache Spark:
https://github.com/apache/spark

PySpark forma parte del proyecto Apache Spark.

--------------------------------------------------
QUE ES PYSPARK

PySpark es la interfaz de Apache Spark para Python. Sirve para trabajar con grandes volumenes de datos de forma distribuida, pero tambien es muy util para aprender transformaciones de datos con DataFrames de manera parecida a pandas. Con PySpark puedes filtrar, seleccionar, agrupar, ordenar y transformar datos usando codigo Python.

--------------------------------------------------
TOPICOS DE ESTE PROYECTO

1. Crear y explorar datos
En este ejercicio se crea un DataFrame desde una lista de datos en Python. Tambien se muestran las filas, la estructura del DataFrame y algunas estadisticas basicas.

2. Filtros
Aqui se aprende a filtrar registros segun condiciones. Por ejemplo: edad mayor a 25, ciudad igual a CDMX o salario mayor a cierta cantidad.

3. Seleccion de columnas
En este tema se usan selecciones especificas de columnas con select. Sirve para mostrar solo la informacion que interesa, como nombre y salario o solo edad.

4. Crear nuevas columnas
Aqui se usa withColumn para generar nuevas columnas a partir de las existentes. Por ejemplo: salario anual, indicadores booleanos y categorias calculadas.

5. Agrupaciones
Este topico muestra como agrupar informacion por una columna, como ciudad, y calcular conteos, promedios, sumas, maximos y minimos.

6. Ordenar datos
En este ejercicio se aprende a ordenar registros de forma ascendente o descendente. Por ejemplo: ordenar por salario, edad o varias columnas al mismo tiempo.

7. Mini reto
Este ejercicio final combina varias ideas vistas antes. Responde preguntas concretas como quien gana mas, que ciudad tiene mayor salario promedio y cuantas personas hay por ciudad.

--------------------------------------------------
OBJETIVO GENERAL

El objetivo de estos ejercicios es construir una base practica en PySpark usando ejemplos pequenos y faciles de entender. Despues de completar esta serie, ya tendras una base para empezar a trabajar con archivos reales, transformaciones mas complejas y analisis de datos a mayor escala.
