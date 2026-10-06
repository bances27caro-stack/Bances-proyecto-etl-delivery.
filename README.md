# <img src="portada.png" width="100%" height="260px">

# Trabajo Práctico Integrador: Pipeline ETL & Inteligencia de Mercado (Food Delivery)

## Contexto de negocio
---
He sido contratada como Data Engineer y Analista BI por el CEO de una startup de Food Delivery que busca expandirse en Estados Unidos. Mi objetivo es analizar a los competidores, las categorías de mercado y los precios por región para apoyar la toma de decisiones. Para ello, desarrollaré un pipeline ETL que incluya la limpieza, transformación y optimización de los datos, su carga en SQLite y el desarrollo de consultas y visualizaciones que faciliten el análisis de los resultados.

## Base de datos
---
Los datos originales se pueden encontrar [aquí](https://github.com/IanCN-23/dalatam_ed3_TPI_Python/tree/main).

Primero descargué y descomprimí el archivo ZIP que contenía los datasets. Luego, creé la carpeta **Trabajo-Final-Python** como espacio de trabajo y añadí los archivos `restaurant-menus.csv` y `restaurants.csv`, que serán utilizados para desarrollar el proceso ETL.

## Librerías utilizadas
---
Para el análisis y procesamiento de los datos se utilizaron las siguientes librerías:

```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sqlalchemy import create_engine, text
```

**Descripción:**
- `pandas`: trabajar con los datos.
- `numpy`: operaciones numéricas.
- `matplotlib` y `seaborn`: gráficos.
- `sqlalchemy`: conexión y carga de datos a SQLite.

### Parte 1 - Extracción y Diagnóstico (Extract)

#### 1.1  Cargar los archivos CSV en DataFrames:

Archivo de restaurante

```python
import pandas as pd

# Identifico la ruta del archivo de restaurantes
url_restaurante = "/Workspace/Users/bances27caro@gmail.com/CURSO_PHYTON/Trabajo-Final-Python/restaurants.csv/restaurants.csv"

# Cargo el archivo
df_restaurantes = pd.read_csv(url_restaurante)

# Visualizó las primeras filas
display(df_restaurantes.head())
```

| id | position | name | score | ratings | category | price_range | full_address | zip_code | lat | lng |
|---:|---:|---|---:|---:|---|:---:|---|---:|---:|---:|
| 1 | 19 | PJ Fresh (224 Daniel Payne Drive) | null | null | Burgers, American, Sandwiches | $ | 224 Daniel Payne Drive, Birmingham, AL, 35207 | 35207 | 33.5623653 | -86.8307025 |
| 2 | 9 | J' ti`'z Smoothie-N-Coffee Bar | null | null | Coffee and Tea, Breakfast and Brunch, Bubble Tea | null | 1521 Pinson Valley Parkway, Birmingham, AL, 35217 | 35217 | 33.58364 | -86.77333 |
| 3 | 6 | Philly Fresh Cheesesteaks (541-B Graymont Ave) | null | null | American, Cheesesteak, Sandwiches, Alcohol | $ | 541-B Graymont Ave, Birmingham, AL, 35204 | 35204 | 33.5098 | -86.85464 |
| 4 | 17 | Papa Murphy's (1580 Montgomery Highway) | null | null | Pizza | $ | 1580 Montgomery Highway, Hoover, AL, 35226 | 35226 | 33.4044388 | -86.8066142 |
| 5 | 162 | Nelson Brothers Cafe (17th St N) | 4.7 | 22 | Breakfast and Brunch, Burgers, Sandwiches | null | 314 17th St N, Birmingham, AL, 35203 | 35203 | 33.51473 | -86.8117 |

Archivo de menús, aquí se debe unir las 10 partes.

```python
import pandas as pd

# Ruta de la carpeta donde están las partes de los menús
ruta_menus = "/Workspace/Users/bances27caro@gmail.com/CURSO_PHYTON/Trabajo-Final-Python/restaurant-menus.csv"

# Lista con los nombres de los 10 archivos
archivos_menus = [
    f"restaurant_menus_parte_{i}.csv"
    for i in range(1, 11)
]

# Leer cada archivo y lo guardo en una lista
lista_menus = []

for archivo in archivos_menus:
    ruta = f"{ruta_menus}/{archivo}"
    df_parte = pd.read_csv(ruta)
    lista_menus.append(df_parte)

# Unir todas las partes
df_menus = pd.concat(lista_menus, ignore_index=True)

# Mostrar las primeras filas
display(df_menus.head())
```

| restaurant_id | category | name | description | price |
|---:|---|---|---|---:|
| 1 | Extra Large Pizza | Extra Large Meat Lovers | Whole pie. | 15.99 USD |
| 1 | Extra Large Pizza | Extra Large Supreme | Whole pie. | 15.99 USD |
| 1 | Extra Large Pizza | Extra Large Pepperoni | Whole pie. | 14.99 USD |
| 1 | Extra Large Pizza | Extra Large BBQ Chicken &amp; Bacon | Whole Pie | 15.99 USD |
| 1 | Extra Large Pizza | Extra Large 5 Cheese | Whole pie. | 14.99 USD |

#### 1.2 Análisis exploratorio (Data Profiling):

Primero, se busca conocer las dimensiones:

```python
# Dimensiones del DataFrame de restaurantes
print(f"df_restaurantes: contiene {df_restaurantes.shape[0]} filas y {df_restaurantes.shape[1]} columnas")

# Dimensiones del DataFrame de menús
print(f"df_menus: contiene {df_menus.shape[0]} filas y {df_menus.shape[1]} columnas")
```

**Resultado**:
```
df_restaurantes: contiene 63469 filas y 11 columnas

df_menus: contiene 836350 filas y 5 columnas
```
Ahora identifico los tipos de datos y la cantidad de valores nulos por columna.

```python
# Perfil de datos de restaurantes
perfil_restaurantes = pd.DataFrame({
    "Categoria": df_restaurantes.columns,
    "Tipo de dato": df_restaurantes.dtypes.astype(str).values,
    "Valores nulos": df_restaurantes.isna().sum().values
})
display(perfil_restaurantes)
```

| Categoria | Tipo de dato | Valores nulos |
|---|---|---:|
| id | int64 | 0 |
| position | int64 | 0 |
| name | object | 0 |
| score | float64 | 28167 |
| ratings | float64 | 28167 |
| category | object | 85 |
| price_range | object | 10617 |
| full_address | object | 453 |
| zip_code | object | 517 |
| lat | float64 | 0 |
| lng | float64 | 0 |

```python
# Perfil de datos de menús
perfil_menus = pd.DataFrame({
    "Categoria": df_menus.columns,
    "Tipo de dato": df_menus.dtypes.astype(str).values,
    "Valores nulos": df_menus.isna().sum().values
})
display(perfil_menus)
```

| Categoria | Tipo de dato | Valores nulos |
|---|---|---:|
| restaurant_id | int64 | 0 |
| category | object | 0 |
| name | object | 1 |
| description | object | 200324 |
| price | object | 0 |

### Parte 2 - Ingeniería de Características y Limpieza (Transform)

#### _En df_restaurantes:_

#### 2.1  Mapeo de Precios:

En el DataFrame df_restaurantes, la columna price_range contiene símbolos como $ que representan diferentes rangos de precios. Para facilitar su interpretación y análisis, se creó un diccionario que permite reemplazar cada símbolo por una categoría de precio expresada mediante nombres descriptivos.

```python
mapeo_precios = {
    "$": "Económico",
    "$$": "Moderadamente caro",
    "$$$": "Caro",
    "$$$$": "Muy caro"
}

# Se determina la cantidad de símbolos
df_restaurantes["rango_de_precios"] = df_restaurantes["price_range"].map(mapeo_precios)
df_restaurantes["rango_de_precios"].value_counts()
```

**Resultado**
```
rango_de_precios
Económico             37637
Moderadamente caro    14952
Caro                    237
Muy caro                 25
Name: count, dtype: int64
```
#### 2.2  Desanidado de Ubicaciones:

En este punto voy a separar la información que está junta en la columna `full_address`.

| full_address |
|---|
| 224 Daniel Payne Drive, Birmingham, AL, 35207 |
| 1521 Pinson Valley Parkway, Birmingham, AL, 35217 |
| 541-B Graymont Ave, Birmingham, AL, 35204 |
| 1580 Montgomery Highway, Hoover, AL, 35226 |
| 314 17th St N, Birmingham, AL, 35203 |

```python
# Se utiliza .str.split() para dividir la columna "full_address" en tres columnas.
df_restaurantes[["calle", "ciudad", "estado_zip"]] = (
    df_restaurantes["full_address"].str.split(",", n=2, expand=True)
)

# Se elimina los espacios que puedan quedar antes o después del nombre de la ciudad.
df_restaurantes["ciudad"] = df_restaurantes["ciudad"].str.strip()

display(df_restaurantes[["calle", "ciudad", "estado_zip"]].head())
```

**Resultado**
| calle | ciudad | estado_zip |
|---|---|---|
| 224 Daniel Payne Drive | Birmingham | AL, 35207 |
| 1521 Pinson Valley Parkway | Birmingham | AL, 35217 |
| 541-B Graymont Ave | Birmingham | AL, 35204 |
| 1580 Montgomery Highway | Hoover | AL, 35226 |
| 314 17th St N | Birmingham | AL, 35203 |

#### 2.3 Filtro de Calidad:

En este apartado, se eliminarán los restaurantes que no cuenten con un puntaje registrado o cuyo puntaje sea igual a 0. Asimismo, se estandarizará la columna category convirtiendo todos sus valores a minúsculas, con el objetivo de mantener un formato uniforme en los datos.

```python
# Eliminamos los score nulos
df_restaurantes = df_restaurantes[df_restaurantes["score"].notna()]

# Eliminamos los score iguales a 0
df_restaurantes = df_restaurantes[df_restaurantes["score"] != 0]

# Estandarizamos category
df_restaurantes["category"] = (df_restaurantes["category"].str.lower())

display(df_restaurantes[["score", "category"]].head(10))
```
| score | category |
|---:|---|
| 4.7 | breakfast and brunch, burgers, sandwiches |
| 4.7 | sushi, asian, japanese |
| 4.6 | breakfast and brunch, salad, sandwich, family meals, pizza, healthy, american, chicken |
| 5 | ice cream &amp; frozen yogurt, comfort food, desserts |
| 4.9 | middle eastern, mediterranean, vegetarian, greek, healthy |
| 3.7 | american, burgers, sandwich |
| 4.7 | italian, exclusive to eats |
| 4.6 | bakery, breakfast and brunch, cafe, coffee &amp; tea |
| 4.8 | mexican, fast food, salads, healthy |
| 4.3 | mexican, breakfast and brunch, burritos |

Para comprobar que ya no quedan score nulos ni ceros:
```python
print("Valores nulos en score:", df_restaurantes["score"].isna().sum())
print("Scores iguales a 0:", (df_restaurantes["score"] == 0).sum())
```

**Resultado**
```python
Valores nulos en score: 0
Scores iguales a 0: 0
```

#### _En df_menus:_

#### 2.4 Limpieza de precios:

En este apartado, se transformará la columna `price`, que actualmente contiene los precios como texto. Primero se eliminará el texto " USD", luego se reemplazará la coma decimal por un punto y finalmente se convertirá el resultado a formato `float`. Esto permitirá trabajar posteriormente con los precios como valores numéricos.

```python
# Eliminamos los ítems del menú que no tienen precio registrado
df_menus = df_menus[df_menus["price"].notna()]

# Eliminamos el texto " USD"
df_menus["price"] = df_menus["price"].astype(str).str.replace(" USD", "", regex=False)

# Reemplazamos la coma decimal por un punto
df_menus["price"] = df_menus["price"].str.replace(",", ".", regex=False)

# Convertimos la columna a formato decimal
df_menus["price"] = df_menus["price"].astype(float)

# Visualizamos el resultado de la transformación
display(df_menus[["price"]].head(10))
```

| price |
|---:|
| 15.99 |
| 15.99 |
| 14.99 |
| 15.99 |
| 14.99 |
| 3.99 |
| 3.99 |
| 3.99 |
| 3.99 |
| 3.99 |

#### 2.5 Eliminación de precios nulos o iguales a 0.0

Se eliminarán los ítems del menú que no tengan un precio registrado o cuyo precio sea igual a `0.0`. De esta manera, el DataFrame conservará únicamente los registros que tengan un precio válido para el análisis.

```python
# Comprobamos los precios nulos y los que tienen valor 0
print("Precios nulos:", df_menus["price"].isna().sum())
print("Precios iguales a 0:", (df_menus["price"] == 0).sum())
```

**Resultado**
```
Precios nulos: 0
Precios iguales a 0: 37551
```

```python
# Eliminamos los precios nulos
df_menus = df_menus[df_menus["price"].notna()]

# Eliminamos los precios iguales a 0
df_menus = df_menus[df_menus["price"] != 0]

# Visualizamos los datos después de la limpieza
display(df_menus[["price"]].head(10))
```

| price |
|---:|
| 15.99 |
| 15.99 |
| 14.99 |
| 15.99 |
| 14.99 |
| 3.99 |
| 3.99 |
| 3.99 |
| 3.99 |
| 3.99 |

Para comprobar que la eliminación se realizó correctamente:

```python
print("Precios nulos:", df_menus["price"].isna().sum())
print("Precios iguales a 0:", (df_menus["price"] == 0).sum())
```

**Resultado**
```
Precios nulos: 0
Precios iguales a 0: 0
```

## Optimización y Cruce (Control de Memoria RAM):
---

En este apartado se reducirá el tamaño de `df_restaurantes` antes de realizar el cruce con `df_menus`. Esto permitirá trabajar únicamente con restaurantes consolidados, evitando procesar información innecesaria y ayudando a controlar el consumo de memoria RAM.

```python
# Visualizamos la columna de calificaciones
display(df_restaurantes[["ratings"]].head(10))

# Filtramos los restaurantes que tienen más de 100 calificaciones
df_restaurantes = df_restaurantes[df_restaurantes["ratings"] > 100]

# Visualizamos el resultado del filtro
display(df_restaurantes[["ratings"]].head(10))
```

Para comprobar que el filtro se realizó correctamente:

```python
print("Cantidad de restaurantes después del filtro:", len(df_restaurantes))
print("Cantidad mínima de ratings:",df_restaurantes["ratings"].min())
```

**Resultado**
```
Cantidad de restaurantes después del filtro: 8702
Cantidad mínima de ratings: 101.0
```

Una vez filtrados los restaurantes, realizaremos un INNER JOIN entre `df_restaurantes` y `df_menus`. El cruce permitirá combinar la información de los restaurantes con sus respectivos productos del menú.

Para realizarlo necesitamos utilizar la columna que contiene el ID del restaurante en ambos DataFrames.

```python
# Realizamos el INNER JOIN mediante el ID del restaurante
df_master = pd.merge(
    df_restaurantes,
    df_menus,
    left_on="id",
    right_on="restaurant_id",
    how="inner"
)
# Visualizamos el DataFrame resultante
display(df_master.head())
```
| id | position | name_x | score | ratings | category_x | price_range | full_address | zip_code | lat | lng | rango_de_precios | calle | ciudad | estado_zip | restaurant_id | category_y | name_y | description | price |
|---:|---:|---|---:|---:|---|:---:|---|---:|---:|---:|---|---|---|---|---:|---|---|---|---:|
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 4756 S 27th St, Milwaukee, WI, 53221 | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | 1739 | Picked for you | Spring Roll | Three rolls. Shrimps, sliced pork, vermicelli noodles, lettuce are wrapped in steamed rice paper. Served with peanut dipping sauce. | 6.99 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 4756 S 27th St, Milwaukee, WI, 53221 | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | 1739 | Picked for you | Egg Roll | Pork, carrot, bean thread noodles, and are wrapped in rice paper. Served deep fried egg roll with mixed fish sauce. | 6.5 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 4756 S 27th St, Milwaukee, WI, 53221 | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | 1739 | Picked for you | Grilled Pork | Foot long. | 9.5 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 4756 S 27th St, Milwaukee, WI, 53221 | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | 1739 | Picked for you | House Special | Rice noodle with steak, rare, flank, brisket, tripe, meatball, and tendon. | 10.95 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 4756 S 27th St, Milwaukee, WI, 53221 | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | 1739 | Picked for you | Rice Noodle with Rare Steak | null | 10.95 |

Limpieza final del DataFrame maestro

Después de realizar el `INNER JOIN`, se revisará la estructura de `df_master` para identificar columnas que ya no sean necesarias o que puedan generar redundancia. En este caso, `restaurant_id` se utilizó como clave para realizar el cruce, mientras que `full_address` ya fue descompuesta previamente en columnas relacionadas con la ubicación.

Por ello, se eliminarán dichas columnas y posteriormente se cambiarán algunos nombres para facilitar la interpretación de la información en las consultas SQL.

```python
# Creamos una copia del DataFrame resultante del INNER JOIN
df_final = df_master.copy()

# Revisamos las columnas disponibles antes de realizar la limpieza
print("Columnas iniciales:")
print(df_final.columns.tolist())

# Eliminamos las columnas que ya no son necesarias
df_final = df_final.drop(
    columns=["restaurant_id", "full_address"]
)

# Cambiamos los nombres para identificar mejor la información
df_final = df_final.rename(
    columns={
        "name_x": "nombre_restaurante",
        "category_x": "categoria_restaurante",
        "name_y": "nombre_plato",
        "category_y": "categoria_menu"
    }
)

# Visualizamos el DataFrame después de la limpieza
display(df_final.head())
```
| id | position | nombre_restaurante | score | ratings | categoria_restaurante | price_range | zip_code | lat | lng | rango_de_precios | calle | ciudad | estado_zip | categoria_menu | nombre_plato | description | price |
|---:|---:|---|---:|---:|---|:---:|---:|---:|---:|---|---|---|---|---|---|---|---:|
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | Picked for you | Spring Roll | Three rolls. Shrimps, sliced pork, vermicelli noodles, lettuce are wrapped in steamed rice paper. Served with peanut dipping sauce. | 6.99 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | Picked for you | Egg Roll | Pork, carrot, bean thread noodles, and are wrapped in rice paper. Served deep fried egg roll with mixed fish sauce. | 6.5 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | Picked for you | Grilled Pork | Foot long. | 9.5 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | Picked for you | House Special | Rice noodle with steak, rare, flank, brisket, tripe, meatball, and tendon. | 10.95 |
| 1739 | 21 | Pho Cali Noodle House | 4.4 | 200 | vietnamese, noodles, sandwich, asian | $ | 53221 | 42.958143 | -87.94815 | Económico | 4756 S 27th St | Milwaukee | WI, 53221 | Picked for you | Rice Noodle with Rare Steak | null | 10.95 |

### Parte 3 - Carga a Base de Datos (Load)

