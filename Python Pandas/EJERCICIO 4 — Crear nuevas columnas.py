import pandas as pd

print("-------------------------------------")
print("Ejercicio 4 — Crear nuevas columnas")

data = {
    "nombre": ["Luis", "Ana", "Pedro", "Sofia", "Juan"],
    "edad": [25, 30, 22, 28, 35],
    "ciudad": ["Monterrey", "CDMX", "Monterrey", "CDMX", "Guadalajara"],
    "salario": [10000, 15000, 8000, 12000, 20000]
}

df = pd.DataFrame(data)

print("-----------------------------------------------------")
print("Crear una nueva columna 'salario_anual'\n")
df["salario_anual"] = df["salario"] * 12
print(df) 
print("--------------------------------------------------------------------------------")
print("Crear una nueva columna 'es_mayor' que indique si la persona es mayor de 30 años\n")
df["es_mayor"] = df["edad"] > 30
print(df)
print("--------------------------------------------------------------------------------")
print("Crear una nueva columna 'ciudad_mayus' con el nombre de la ciudad en mayúsculas\n")
df["ciudad_mayus"] = df["ciudad"].str.upper()
print(df)
print("-----------------------------------------------------------------------------------------------------")
print("Crear una nueva columna 'salario_categoria' que clasifique el salario en 'Bajo', 'Medio' o 'Alto'\n")
df["salario_categoria"] = pd.cut(df["salario"], bins=[0, 10000, 15000, float("inf")], labels=["Bajo", "Medio", "Alto"])
print(df)   
print("--------------------------------------------------------------------------------------------------------")
print("Crear una nueva columna 'edad_grupo' que clasifique la edad en 'Joven', 'Adulto' o 'Mayor'\n")
df["edad_grupo"] = pd.cut(df["edad"], bins=[0, 25, 35, float("inf")], labels=["Joven", "Adulto", "Mayor"])
print(df)
print("-------------------------------------------------------------------------------------------------------------------------")
print("Crear una nueva columna 'nombre_inicial' con la primera letra del nombre\n")
df["nombre_inicial"] = df["nombre"].str[0]
print(df)
print("-------------------------------------------------------------------------------------------------------------------------")
print("Crear una nueva columna 'salario_mensual' que sea el salario anual dividido entre 12\n")
df["salario_mensual"] = df["salario_anual"] / 12
print(df)
print("-------------------------------------------------------------------------------------------------------------------------")


