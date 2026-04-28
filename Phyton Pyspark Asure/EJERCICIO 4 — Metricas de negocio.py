import os
import sys

from pipeline_utils import construir_metricas, crear_sesion_spark, enriquecer_ventas, leer_fuentes


"""
Ejercicio 4.

Aqui se usa esto:
- enriquecer_ventas para obtener el dataset ya limpio y enriquecido.
- construir_metricas para producir agregados de negocio.

Relacion con otros archivos:
este ejercicio depende de toda la preparacion anterior, pero ya no repite esa logica.
"""


spark = crear_sesion_spark("MetricasDeNegocio")

df_clientes, df_productos, df_ventas = leer_fuentes(spark)
df_ventas_enriquecidas = enriquecer_ventas(df_clientes, df_productos, df_ventas)
metricas = construir_metricas(df_ventas_enriquecidas)

os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------")
print("Ejercicio 4 — Metricas de negocio")
print("-----------------------------------------------")
print("GUIA DEL EJERCICIO\n")
print("Aqui se toma el dataset enriquecido del ejercicio anterior.")
print("Aqui se construyen tablas agregadas por producto, ciudad, cliente y periodo.")
print("Estas metricas son las que normalmente se consumen en analitica o BI.\n")
print("Ventas por producto\n")
metricas["ventas_por_producto"].show(truncate=False)
print("-----------------------------------------------")
print("Ventas por canal\n")
metricas["ventas_por_canal"].show(truncate=False)
print("-----------------------------------------------")
print("Ventas por ciudad\n")
metricas["ventas_por_ciudad"].show(truncate=False)
print("-----------------------------------------------")
print("Ventas por cliente\n")
metricas["ventas_por_cliente"].show(truncate=False)
print("-----------------------------------------------")
print("Ventas mensuales\n")
metricas["ventas_mensuales"].show(truncate=False)
print("-----------------------------------------------")
print("Resumen por estado de venta\n")
metricas["resumen_estados"].show(truncate=False)
print("-----------------------------------------------")
print("Aqui termina la capa de metricas. Lo siguiente es guardarlas en formato profesional.")
print("-----------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")