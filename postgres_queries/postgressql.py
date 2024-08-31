import psycopg2

def connect():
    try:
        conn = psycopg2.connect(
            dbname="tpc-h",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        print("Conexão com Postgres realizada com sucesso")
        return conn
    except Exception as e:
        print(f"Não foi possível conectar ao banco de dados Postgres | {e}")
        return None
    
client_postgres = connect()
