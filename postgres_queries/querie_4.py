from random import choice, randint
import time

if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres

SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']

def execute(cursor, date):
    cursor.execute(f"""
        SELECT
            o_orderpriority,
            COUNT(*) AS order_count
        FROM
            orders
        WHERE
            o_orderdate >= DATE '{date}'
            AND o_orderdate < DATE '{date}' + INTERVAL '3 months'
            AND EXISTS (
                SELECT
                    1
                FROM
                    lineitem
                WHERE
                    l_orderkey = o_orderkey
                    AND l_commitdate < l_receiptdate
            )
        GROUP BY
            o_orderpriority
        ORDER BY
            o_orderpriority;
    """)
    print(cursor.fetchall())



def generate_random_date():
    year = randint(1993, 1997)
    month = randint(1, 12) if year != 1997 else randint(1, 10)  # Limita os meses de 1997 até outubro
    return f'{year}-{month:02}-01'  # Sempre retorna o primeiro dia do mês

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(100):
        date = generate_random_date()
        
        start_time = time.time()
        execute(cursor, date)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo médio de execução: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()
