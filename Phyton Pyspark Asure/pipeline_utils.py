import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, coalesce, col, concat_ws, count, initcap, lit, month, round, sum, to_date, trim, year

from config import obtener_ruta_entrada, obtener_ruta_salida


"""
Archivo de utilidades reutilizables del pipeline.

Aqui se concentra la logica real de ingenieria de datos para no repetir codigo en cada ejercicio.

Relacion con otros archivos:
- config.py define las rutas.
- Los ejercicios llaman a estas funciones para leer, limpiar, transformar y guardar.

Piensa en este archivo como el motor del proyecto.
Los ejercicios son pantallas guiadas para ver cada etapa del flujo.
"""


def crear_sesion_spark(nombre_app: str) -> SparkSession:
	# Aqui se prepara Spark para ejecutar el flujo localmente.
	os.environ["PYSPARK_PYTHON"] = sys.executable
	os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

	spark = SparkSession.builder.appName(nombre_app).getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
	return spark


def leer_fuentes(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
	# Aqui se leen las fuentes raw del proyecto usando las rutas centralizadas en config.py.
	df_clientes = spark.read.csv(obtener_ruta_entrada("clientes.csv"), header=True, inferSchema=True)
	df_productos = spark.read.csv(obtener_ruta_entrada("productos.csv"), header=True, inferSchema=True)
	df_ventas = spark.read.csv(obtener_ruta_entrada("ventas.csv"), header=True, inferSchema=True)
	return df_clientes, df_productos, df_ventas


def limpiar_datos_clientes(df_clientes: DataFrame) -> DataFrame:
	# Aqui se corrigen nombres, ciudades y algunos faltantes basicos en clientes.
	return df_clientes.dropDuplicates(["cliente_id"]).withColumn(
		"nombre_cliente",
		initcap(trim(col("nombre_cliente")))
	).withColumn(
		"apellido_cliente",
		initcap(trim(col("apellido_cliente")))
	).withColumn(
		"ciudad",
		initcap(trim(col("ciudad")))
	).withColumn(
		"segmento",
		initcap(trim(col("segmento")))
	).withColumn(
		"canal_origen",
		initcap(trim(col("canal_origen")))
	).withColumn(
		"correo",
		coalesce(col("correo"), lit("sin_correo@pendiente.com"))
	)


def limpiar_datos_productos(df_productos: DataFrame) -> DataFrame:
	# Aqui se estandarizan textos y se rellenan atributos faltantes en productos.
	return df_productos.dropDuplicates(["producto_id"]).withColumn(
		"nombre_producto",
		initcap(trim(col("nombre_producto")))
	).withColumn(
		"categoria",
		initcap(trim(col("categoria")))
	).withColumn(
		"subcategoria",
		initcap(trim(col("subcategoria")))
	).withColumn(
		"marca",
		coalesce(initcap(trim(col("marca"))), lit("Sin Marca"))
	).withColumn(
		"estatus_producto",
		initcap(trim(col("estatus_producto")))
	)


def limpiar_datos_ventas(df_ventas: DataFrame) -> DataFrame:
	# Aqui se eliminan duplicados, nulos criticos y valores imposibles antes de cualquier join o metrica.
	return df_ventas.dropDuplicates(["venta_id"]).dropna(
		subset=["venta_id", "cliente_id", "producto_id", "fecha", "cantidad", "precio_unitario"]
	).filter(
		(col("cantidad") > 0) & (col("precio_unitario") > 0)
	).withColumn(
		"descuento",
		coalesce(col("descuento"), lit(0))
	).withColumn(
		"metodo_pago",
		initcap(trim(col("metodo_pago")))
	).withColumn(
		"canal_venta",
		initcap(trim(col("canal_venta")))
	).withColumn(
		"estado_venta",
		initcap(trim(col("estado_venta")))
	).withColumn(
		"ejecutivo",
		initcap(trim(col("ejecutivo")))
	)


def enriquecer_ventas(
	df_clientes: DataFrame,
	df_productos: DataFrame,
	df_ventas: DataFrame,
) -> DataFrame:
	# Aqui se conecta todo: fuentes limpias + joins + columnas de negocio.
	df_clientes_limpios = limpiar_datos_clientes(df_clientes)
	df_productos_limpios = limpiar_datos_productos(df_productos)
	df_ventas_limpias = limpiar_datos_ventas(df_ventas)

	return df_ventas_limpias.join(df_clientes_limpios, on="cliente_id", how="inner").join(
		df_productos_limpios,
		on="producto_id",
		how="inner",
	).withColumn("fecha", to_date(col("fecha"))).withColumn(
		"nombre_cliente_completo",
		concat_ws(" ", col("nombre_cliente"), col("apellido_cliente"))
	).withColumn(
		"anio",
		year(col("fecha"))
	).withColumn(
		"mes",
		month(col("fecha"))
	).withColumn(
		"ingreso_bruto",
		col("cantidad") * col("precio_unitario")
	).withColumn(
		"descuento_aplicado",
		col("descuento")
	).withColumn(
		"venta_total",
		col("ingreso_bruto") - col("descuento_aplicado")
	).withColumn(
		"margen_estimado",
		round(col("venta_total") - (col("cantidad") * col("costo_unitario")), 2)
	)


def construir_metricas(df: DataFrame) -> dict[str, DataFrame]:
	# Aqui se construyen tablas agregadas que ya sirven para analisis o capa gold.
	df_ventas_confirmadas = df.filter(col("estado_venta") == "Completada")

	df_ventas_por_producto = df_ventas_confirmadas.groupBy("nombre_producto", "categoria").agg(
		sum("venta_total").alias("ventas_totales")
	).orderBy(col("ventas_totales").desc())

	df_ventas_por_ciudad = df_ventas_confirmadas.groupBy("ciudad").agg(
		sum("venta_total").alias("ventas_totales"),
		count("venta_id").alias("transacciones")
	).orderBy(col("ventas_totales").desc())

	df_ventas_por_cliente = df_ventas_confirmadas.groupBy("nombre_cliente_completo", "segmento").agg(
		sum("venta_total").alias("ventas_totales")
	).orderBy(col("ventas_totales").desc())

	df_ventas_mensuales = df_ventas_confirmadas.groupBy("anio", "mes").agg(
		sum("venta_total").alias("ventas_totales"),
		avg("venta_total").alias("ticket_promedio")
	).orderBy("anio", "mes")

	df_ventas_por_canal = df_ventas_confirmadas.groupBy("canal_venta").agg(
		sum("venta_total").alias("ventas_totales"),
		count("venta_id").alias("transacciones")
	).orderBy(col("ventas_totales").desc())

	df_resumen_estados = df.groupBy("estado_venta").agg(
		count("venta_id").alias("total_registros"),
		sum(coalesce(col("venta_total"), lit(0))).alias("monto_asociado")
	).orderBy(col("total_registros").desc())

	return {
		"ventas_por_producto": df_ventas_por_producto,
		"ventas_por_ciudad": df_ventas_por_ciudad,
		"ventas_por_cliente": df_ventas_por_cliente,
		"ventas_mensuales": df_ventas_mensuales,
		"ventas_por_canal": df_ventas_por_canal,
		"resumen_estados": df_resumen_estados,
	}


def escribir_metricas(metricas: dict[str, DataFrame]) -> None:
	# Aqui se guarda cada metrica en Parquet para simular una salida analitica profesional.
	for nombre_metrica, df_metrica in metricas.items():
		df_metrica.write.mode("overwrite").parquet(obtener_ruta_salida(nombre_metrica))


def obtener_cliente_top(metricas: dict[str, DataFrame]):
	# Aqui se reutiliza la metrica agregada para obtener rapidamente el cliente top.
	return metricas["ventas_por_cliente"].first()