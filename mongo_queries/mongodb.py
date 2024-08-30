from pymongo import MongoClient

def connect():
    try:
        client = MongoClient("mongodb://localhost:27017")
        print("Conexão com Mongodb realizada com sucesso")
        return client
    except Exception as e:
        print(f"Não foi possível conectar ao banco de dados Mongodb | {e}")
        return None

client_mongo = connect()

