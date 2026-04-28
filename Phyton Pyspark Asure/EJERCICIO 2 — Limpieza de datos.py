import os
import sys

from pyspark.sql.functions import col

from pipeline_utils import crear_sesion_spark, leer_fuentes, limpiar_datos_clientes, limpiar_datos_productos, limpiar_datos_ventas


"""
Ejercicio 2.

Aqui se usa esto:
- leer_fuentes para leer clientes, productos y ventas.
- limpiar_datos_clientes, limpiar_datos_productos y limpiar_datos_ventas para aplicar reglas basicas de calidad.

Relacion con otros archivos:
este ejercicio no conoce rutas ni joins; solo consume funciones del motor central.
"""


spark = crear_sesion_spark("LimpiezaDeDatosAzure")

df_clientes, df_productos, df_ventas = leer_fuentes(spark)
df_clientes_limpios = limpiar_datos_clientes(df_clientes)
df_productos_limpios = limpiar_datos_productos(df_productos)
df_ventas_limpias = limpiar_datos_ventas(df_ventas)

os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------")
print("Ejercicio 2 — Limpieza de datos")
print("-----------------------------------------------")
print("GUIA DEL EJERCICIO\n")
print("Aqui se leen las tres fuentes crudas desde pipeline_utils.leer_fuentes.")
print("Aqui se corrigen nulos, duplicados y textos mal formateados como nombres en minuscula.")
print("Aqui todavia no hacemos joins ni metricas; solo dejamos datos confiables.\n")
print("Cliente crudo vs cliente limpio\n")
df_clientes.select("cliente_id", "nombre_cliente", "apellido_cliente", "ciudad", "correo").show(7, truncate=False)
print("-----------------------------------------------")
df_clientes_limpios.select("cliente_id", "nombre_cliente", "apellido_cliente", "ciudad", "correo").show(7, truncate=False)
print("-----------------------------------------------")
print("Producto crudo vs producto limpio\n")
df_productos.select("producto_id", "nombre_producto", "categoria", "marca").show(truncate=False)
print("-----------------------------------------------")
df_productos_limpios.select("producto_id", "nombre_producto", "categoria", "marca").show(truncate=False)
print("-----------------------------------------------")
print("Ventas originales\n")
df_ventas.show(truncate=False)
print("-----------------------------------------------")
print(f"Ventas con nulos criticos: {df_ventas.filter(col('cantidad').isNull() | col('precio_unitario').isNull()).count()}")
print(f"Ventas duplicadas por venta_id: {df_ventas.count() - df_ventas.dropDuplicates(['venta_id']).count()}")
print("-----------------------------------------------")
print(f"Registros originales: {df_ventas.count()}")
print(f"Registros limpios: {df_ventas_limpias.count()}")
print("-----------------------------------------------")
print("Ventas despues de eliminar nulos y duplicados\n")
df_ventas_limpias.show(truncate=False)
print("-----------------------------------------------")
print("Aqui termina la capa de limpieza que luego reutiliza el resto del pipeline.")
print("-----------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")