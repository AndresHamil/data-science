import os
import sys

from pipeline_utils import (
	construir_metricas,
	crear_sesion_spark,
	enriquecer_ventas,
	escribir_metricas,
	leer_fuentes,
	obtener_cliente_top,
)


"""
Ejercicio 6.

Aqui se usa casi todo el proyecto junto.

Relacion con otros archivos:
- leer_fuentes lee las fuentes.
- enriquecer_ventas aplica limpieza y transformacion.
- construir_metricas crea agregados.
- obtener_cliente_top resume una metrica clave.
- escribir_metricas guarda resultados.

Este archivo representa el pipeline completo de extremo a extremo.
"""


spark = crear_sesion_spark("PipelineEndToEnd")

df_clientes, df_productos, df_ventas = leer_fuentes(spark)
df_ventas_enriquecidas = enriquecer_ventas(df_clientes, df_productos, df_ventas)
metricas = construir_metricas(df_ventas_enriquecidas)
cliente_top = obtener_cliente_top(metricas)
escribir_metricas(metricas)

os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------")
print("Ejercicio 6 — Pipeline end to end")
print("-----------------------------------------------")
print("GUIA DEL EJERCICIO\n")
print("Aqui se encadenan todas las etapas del proyecto en un solo recorrido.")
print("Primero lectura, luego limpieza y enriquecimiento, luego metricas y al final escritura.")
print("Este es el archivo que mas se parece a un pipeline real de trabajo.\n")
print(f"Registros crudos de ventas: {df_ventas.count()}")
print(f"Registros enriquecidos: {df_ventas_enriquecidas.count()}")
print("-----------------------------------------------")
print("Cliente top del pipeline\n")
print(
	f"{cliente_top['nombre_cliente_completo']} del segmento {cliente_top['segmento']} con ventas acumuladas de {cliente_top['ventas_totales']}"
)
print("-----------------------------------------------")
print("Metricas disponibles\n")
for nombre_metrica in metricas.keys():
	print(f"- {nombre_metrica}")
print("-----------------------------------------------")
print("Pipeline ejecutado y metricas guardadas en Parquet")
print("-----------------------------------------------")
print("Aqui ves la foto completa del viaje del dato: raw -> limpio -> enriquecido -> metrica -> parquet")
print("-----------------------------------------------")

spark.stop()

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")