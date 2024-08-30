if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import choice, randint
import time

SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']

def execute(cursor, segment, date):
    cursor.execute(f"""
                SELECT
                    l_orderkey,
                    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                    o_orderdate,
                    o_shippriority
                FROM
                    customer,
                    orders,
                    lineitem
                WHERE
                    c_mktsegment = '{segment}'
                    AND c_custkey = o_custkey
                    AND l_orderkey = o_orderkey
                    AND o_orderdate < '{date}'
                    AND l_shipdate > '{date}'
                GROUP BY
                    l_orderkey,
                    o_orderdate,
                    o_shippriority
                ORDER BY
                    revenue DESC,
                    o_orderdate;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(1, 101):
        segment = choice(SEGMENTS)
        date = f'1995-03-{randint(1, 31)}'
        
        start_time = time.time()
        execute(cursor, segment, date)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()