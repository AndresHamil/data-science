import os
import sys

from config import ENTORNO_DATOS, obtener_ruta_entrada, obtener_ruta_salida, resumen_entorno


"""
Ejercicio 7.

Aqui no se transforman datos.
Aqui se explica la relacion entre la logica del pipeline y la configuracion del entorno.

Relacion con otros archivos:
config.py controla las rutas y permite que el mismo pipeline luego apunte a Azure.
"""


os.system("cls" if os.name == "nt" else "clear")

print("-----------------------------------------------------")
print("Ejercicio 7 — Preparacion para Azure Databricks")
print("-----------------------------------------------------")
print("GUIA DEL EJERCICIO\n")
print("Aqui se muestra como las rutas dependen de config.py y del valor ENTORNO_DATOS.")
print("Aqui no cambia la logica de negocio; solo cambia el origen y destino de los datos.")
print("Esta es la pieza que te prepara para mover el proyecto a Azure mas adelante.")
print("Los mismos CSV mas ricos y las mismas metricas podran vivir en la nube sin reescribir el pipeline.\n")
print(resumen_entorno())
print(f"Modo actual: {ENTORNO_DATOS}\n")
print("Rutas de entrada esperadas\n")
print(obtener_ruta_entrada("clientes.csv"))
print(obtener_ruta_entrada("productos.csv"))
print(obtener_ruta_entrada("ventas.csv"))
print("-----------------------------------------------------")
print("Rutas de salida esperadas\n")
print(obtener_ruta_salida("ventas_por_producto"))
print(obtener_ruta_salida("ventas_por_ciudad"))
print(obtener_ruta_salida("ventas_por_cliente"))
print(obtener_ruta_salida("ventas_mensuales"))
print(obtener_ruta_salida("ventas_por_canal"))
print(obtener_ruta_salida("resumen_estados"))
print("-----------------------------------------------------")
print("Para migrar a Azure, cambia ENTORNO_DATOS=azure y apunta las rutas al Data Lake")
print("La logica del pipeline no cambia; cambia solo la configuracion")
print("-----------------------------------------------------")

if sys.stdin.isatty():
	input("Presiona Enter para cerrar...")