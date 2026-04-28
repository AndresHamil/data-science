PYSPARK AZURE

Este proyecto simula un flujo de trabajo de Data Engineering con PySpark pensando en una migracion futura a Azure Databricks. La logica de negocio se desarrolla en local, pero la estructura del proyecto ya separa configuracion, lectura, limpieza, transformaciones, metricas y escritura para que el cambio a nube sea principalmente un cambio de rutas.

Los datos de entrada ya no son tan pequenos ni tan limpios. Ahora incluyen mas columnas, valores faltantes, duplicados, diferencias de capitalizacion y registros con claves invalidas para que el ejercicio se sienta mas cercano a un escenario real.

--------------------------------------------------
ENFOQUE DEL PROYECTO

Tu enfoque es correcto para prepararte para un rol real de Data Engineer:

Leer desde archivos reales o simulados.
Separar limpieza, transformaciones y metricas.
Guardar salidas curadas en Parquet.
Diseñar el flujo para cambiar de local a Azure sin reescribir la logica.

Eso ya se parece mas a un pipeline profesional que a ejercicios aislados.

--------------------------------------------------
MEJORAS TIPO EMPRESA

Usar una capa de configuracion para las rutas.
Evitar hardcodear paths dentro de cada script.
Separar funciones reutilizables del codigo de demostracion.
Guardar salidas en carpetas por capa o por fecha cuando el flujo crezca.
Pensar desde ahora en particionamiento, calidad de datos y validaciones.
Normalizar texto y nombres desde una etapa temprana para no contaminar metricas.

--------------------------------------------------
MALAS PRACTICAS A EVITAR

Repetir la misma logica en varios scripts.
Hacer transformaciones con loops o collect sin necesidad.
Escribir todo en un solo archivo gigante.
No validar duplicados, nulos o llaves rotas en joins.
Acoplar la logica a rutas locales fijas.

--------------------------------------------------
OPTIMIZACIONES Y EXTENSIONES

Particionar Parquet por anio y mes.
Agregar validaciones de calidad antes de guardar.
Crear una capa silver y una capa gold.
Preparar el flujo para recibir parametros por entorno.
Agregar pruebas de volumen, skew y joins mas grandes cuando subas de nivel.

--------------------------------------------------
ESTRUCTURA DEL PROYECTO

config.py
Configuracion de rutas locales y rutas Azure.

pipeline_utils.py
Funciones reutilizables para lectura, limpieza, transformacion, metricas y escritura.

datos/raw
Archivos CSV simulados como fuente de entrada, ahora con mas columnas de negocio y errores controlados para practicar limpieza real.

datos/output
Destino local para archivos Parquet generados por los ejercicios.

--------------------------------------------------
TOPICOS DE ESTE PROYECTO

1. Configurar y leer fuentes
Se leen tres CSV mas ricos: clientes, productos y ventas. Se valida schema, volumen y primeras filas.

2. Limpieza de datos
Se corrigen nulos, duplicados y formatos inconsistentes como nombres con inicial minuscula o ciudades mal capitalizadas.

3. Transformaciones de negocio
Se unen fuentes, se crean columnas como nombre completo, anio, mes, ingreso bruto, descuento aplicado, venta total y margen estimado.

4. Metricas de negocio
Se calculan ventas por producto, ciudad, cliente, canal, mes y estado de venta.

5. Escritura en Parquet
Las metricas procesadas se guardan en formato Parquet para simular una capa analitica lista para consumo.

6. Pipeline end to end
Se corre el flujo completo de entrada a salida y se resume el resultado principal del negocio.

7. Preparacion para Azure Databricks
Se muestra como el mismo pipeline puede apuntar a Azure cambiando solo las rutas y el entorno.

--------------------------------------------------
DATOS SIMULADOS Y SUCIEDAD CONTROLADA

clientes.csv
Incluye nombre, apellido, ciudad, segmento, correo, fecha de registro y canal de origen. Tiene nombres en minuscula, mayuscula y un correo faltante.

productos.csv
Incluye categoria, subcategoria, marca, costo y precio lista. Tiene una marca faltante y textos sin normalizar.

ventas.csv
Incluye cantidad, precio, descuento, metodo de pago, canal, estado y ejecutivo comercial. Tiene duplicados, nulos, una clave de cliente invalida y registros con datos imposibles.

Todo esto existe a proposito para que la etapa de limpieza tenga sentido practico.

--------------------------------------------------
OBJETIVO GENERAL

El objetivo de este modulo es que dejes de pensar solo en ejercicios de PySpark y empieces a pensar como Data Engineer. La meta no es solo transformar datos, sino construir una base de pipeline portable, ordenada y lista para moverse a Azure cambiando principalmente la configuracion de acceso.

Tambien se busca que el ejercicio se sienta tangible: leer datos mas reales, limpiar problemas comunes, normalizar nombres y generar analitica que si podrias contar en un proyecto de portafolio.