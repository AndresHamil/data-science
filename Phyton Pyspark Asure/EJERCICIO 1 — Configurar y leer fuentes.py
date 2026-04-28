import os
import sys

from config import obtener_ruta_entrada, resumen_entorno
from pipeline_utils import crear_sesion_spark, leer_fuentes


"""
Ejercicio 1.

Aqui se hace el primer contacto con el pipeline.
No transformamos ni limpiamos datos todavia.

Aqui se usa esto:
- config.py para resolver las rutas de entrada.
- pipeline_utils.py para crear Spark y leer las fuentes.

Objetivo:
confirmar que el proyecto puede encontrar y leer los archivos raw.
"""


spark = crear_sesion_spark("ConfigurarYLeerFuentes")

df_clientes, df_productos, df_ventas = leer_fuentes(spark)

os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------")
print("Ejercicio 1 — Configurar y leer fuentes")
print("-----------------------------------------------")
print("GUIA DEL EJERCICIO\n")
print("Aqui se usa config.py para obtener las rutas de los CSV.")
print("Aqui se usa pipeline_utils.py para crear Spark y leer clientes, productos y ventas.")
print("Aqui se hace una validacion inicial: rutas, schema, muestra de datos y conteos.")
print("Los archivos ahora tienen mas columnas para simular un caso mas parecido a empresa.\n")
print(resumen_entorno())
print(f"Clientes: {obtener_ruta_entrada('clientes.csv')}")
print(f"Productos: {obtener_ruta_entrada('productos.csv')}")
print(f"Ventas: {obtener_ruta_entrada('ventas.csv')}\n")
print("Primeras filas de clientes\n")
df_clientes.show(5, truncate=False)
print("-----------------------------------------------")
print("Primeras filas de productos\n")
df_productos.show(5, truncate=False)
print("-----------------------------------------------")
print("Schema de ventas\n")
df_ventas.printSchema()
print("-----------------------------------------------")
print("Primeras filas de ventas\n")
df_ventas.show(truncate=False)
print("-----------------------------------------------")
print("Conteos\n")
print(f"Clientes: {df_clientes.count()}")
print(f"Productos: {df_productos.count()}")
print(f"Ventas: {df_ventas.count()}")
print("-----------------------------------------------")
print("Aqui termina la etapa de entrada. Si esto esta bien, el pipeline ya puede pasar a limpieza.")
print("-----------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")