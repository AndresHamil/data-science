import os
import sys

from config import obtener_ruta_salida
from pipeline_utils import construir_metricas, crear_sesion_spark, enriquecer_ventas, escribir_metricas, leer_fuentes


"""
Ejercicio 5.

Aqui se usa esto:
- obtener_ruta_salida de config.py para saber a donde escribir.
- escribir_metricas de pipeline_utils.py para guardar los resultados.

Relacion con otros archivos:
no se recalculan rutas manualmente; se respeta la configuracion central del proyecto.
"""


spark = crear_sesion_spark("EscrituraEnParquet")

df_clientes, df_productos, df_ventas = leer_fuentes(spark)
df_ventas_enriquecidas = enriquecer_ventas(df_clientes, df_productos, df_ventas)
metricas = construir_metricas(df_ventas_enriquecidas)
escribir_metricas(metricas)

os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------")
print("Ejercicio 5 — Escritura en Parquet")
print("-----------------------------------------------")
print("GUIA DEL EJERCICIO\n")
print("Aqui se ejecuta el flujo de lectura, transformacion y metricas.")
print("Despues se usa escribir_metricas para guardar cada DataFrame en Parquet.")
print("La ruta de salida sale desde config.py, no desde strings hardcodeados.\n")
print("Se generaron salidas Parquet en:\n")
print(obtener_ruta_salida("ventas_por_producto"))
print(obtener_ruta_salida("ventas_por_ciudad"))
print(obtener_ruta_salida("ventas_por_cliente"))
print(obtener_ruta_salida("ventas_mensuales"))
print(obtener_ruta_salida("ventas_por_canal"))
print(obtener_ruta_salida("resumen_estados"))
print("-----------------------------------------------")
print("Aqui termina la etapa de persistencia de resultados.")
print("-----------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")