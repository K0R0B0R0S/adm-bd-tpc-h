if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

def generate_random_date():
    year = randint(1993, 1994)
    month = randint(1, 12)
    date = f'{year}-{month:02}-01'  # Primeiro dia do mês selecionado
    return date

def execute(cursor, date):
    cursor.execute(f"""
                 SELECT
            c_custkey,
            c_name,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
        FROM
            customer,
            orders,
            lineitem,
            nation
        WHERE
            c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate >= DATE '{date}'
            AND o_orderdate < DATE '{date}' + INTERVAL '3' MONTH
            AND l_returnflag = 'R'
            AND c_nationkey = n_nationkey
        GROUP BY
            c_custkey,
            c_name,
            c_acctbal,
            c_phone,
            n_name,
            c_address,
            c_comment
        ORDER BY
            revenue DESC;
                """)
    print(cursor.fetchall()) 

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):

        date = generate_random_date()

        start_time = time.time()
        execute(cursor, date)
        cursor.fetchall()
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()