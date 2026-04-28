import os
import sys

from pipeline_utils import crear_sesion_spark, enriquecer_ventas, leer_fuentes


"""
Ejercicio 3.

Aqui se usa esto:
- leer_fuentes para traer clientes, productos y ventas.
- enriquecer_ventas para ejecutar la parte central del negocio.

Relacion con otros archivos:
enriquecer_ventas llama internamente a limpiar_datos_ventas y luego hace joins y columnas nuevas.
"""


spark = crear_sesion_spark("TransformacionesDeNegocio")

df_clientes, df_productos, df_ventas = leer_fuentes(spark)
df_ventas_enriquecidas = enriquecer_ventas(df_clientes, df_productos, df_ventas)

os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------")
print("Ejercicio 3 — Transformaciones de negocio")
print("-----------------------------------------------")
print("GUIA DEL EJERCICIO\n")
print("Aqui se unen ventas, clientes y productos usando enriquecer_ventas.")
print("Aqui tambien se generan columnas nuevas como anio, mes, ingreso_bruto, descuento_aplicado y venta_total.")
print("Esta es la primera vista del dataset ya listo para analisis.\n")
print("Ventas enriquecidas con nombres normalizados y metricas de negocio\n")
df_ventas_enriquecidas.select(
	"venta_id",
	"nombre_cliente_completo",
	"ciudad",
	"nombre_producto",
	"canal_venta",
	"estado_venta",
	"anio",
	"mes",
	"ingreso_bruto",
	"descuento_aplicado",
	"venta_total",
	"margen_estimado"
).show(truncate=False)
print("-----------------------------------------------")
print("Aqui termina la etapa donde el dato deja de ser raw y empieza a ser dato de negocio.")
print("-----------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")