import pandas as pd

print("-------------------------------------")
print("Ejercicio 7 — Mini reto (nivel entrevista)")

data = {
    "nombre": ["Luis", "Ana", "Pedro", "Sofia", "Juan"],
    "edad": [25, 30, 22, 28, 35],
    "ciudad": ["Monterrey", "CDMX", "Monterrey", "CDMX", "Guadalajara"],
    "salario": [10000, 15000, 8000, 12000, 20000]
}

df = pd.DataFrame(data)

print("-------------------------------------")
print("Persona que gana mas\n")
print(df.loc[df["salario"].idxmax()])
print("-------------------------------------")
print("Persona que gana menos\n")
print(df.loc[df["salario"].idxmin()])
print("-------------------------------------")
print("Promedio de salario\n")  
print(df["salario"].mean())
print("-------------------------------------")
print("Persona mas joven\n")
print(df.loc[df["edad"].idxmin()])
print("-------------------------------------")
print("Persona mas vieja\n")
print(df.loc[df["edad"].idxmax()])
print("-------------------------------------")
print("Promedio de edad\n")
print(df["edad"].mean())
print("-------------------------------------")
print("Número de personas por ciudad\n")
print(df["ciudad"].value_counts())
print("-------------------------------------")
print("Salario promedio por ciudad\n")
print(df.groupby("ciudad")["salario"].mean())
print("-------------------------------------")