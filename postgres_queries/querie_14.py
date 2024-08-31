if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time


def execute(cursor, date):
    cursor.execute(f"""
        SELECT
            100.00 * SUM(CASE
                WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (1 - l_discount)
                ELSE 0
            END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM
            lineitem,
            part
        WHERE
            l_partkey = p_partkey
            AND l_shipdate >= DATE '{date}'
            AND l_shipdate < DATE '{date}' + INTERVAL '1 month';
    """)
    print(cursor.fetchall()) 

def generate_random_date():
    # Gera a data como o primeiro dia de um mês aleatório dentro de um ano aleatório entre 1993 e 1997
    year = randint(1993, 1997)
    month = randint(1, 12)
    date = f'{year}-{month:02}-01'  # Primeiro dia do mês selecionado
    return date

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):

        date = generate_random_date() 
        start_time = time.time()
        execute(cursor, date)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()