import pandas as pd
from pymongo import MongoClient

# Primeiro dataframe
df1 = pd.DataFrame({'Carro': ['Onix', 'Polo', 'Sandero', 'Fiesta', 'City'],
                    'Cor': ['Prata', 'Branco', 'Prata', 'Vermelho', 'Preto'],
                    'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda']})

df2 = pd.DataFrame({'Montadora': ['Chevrolet', 'Volkswagen', 'Renault', 'Ford', 'Honda'],
                    'Pais': ['EUA', 'Alemanha', 'França', 'EUA', 'Japão']})


# Conectar ao servidor do MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Selecionar o banco de dados
db = client['avaliacaoDataOps']

# Coleção "Carros"
carros_collection = db['Carros']

# Coleção "Montadoras"
montadoras_collection = db['Montadoras']

# Inserir os dados do df1 na coleção "Carros"
df1_records = df1.to_dict('records')
carros_collection.insert_many(df1_records)

# Inserir os dados do df2 na coleção "Montadoras"
df2_records = df2.to_dict('records')
montadoras_collection.insert_many(df2_records)

# Realizar a agregação para obter o resultado desejado
pipeline = [
    {
        '$lookup': {
            'from': 'Montadoras',
            'localField': 'Montadora',
            'foreignField': 'Montadora',
            'as': 'Montadora_info'
        }
    },
    {
        '$unwind': '$Montadora_info'
    },
    {
        '$project': {
            'Carro': 1,
            'Cor': 1,
            'Montadora': 1,
            'Pais': '$montadora_info.Pais'
        }
    }
]

resultado = carros_collection.aggregate(pipeline)

# Exibir o resultado
for doc in resultado:
    print(doc)
