if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint
import time

DELTA_START = 60
DELTA_END = 120

def execute(cursor, delta):
    cursor.execute(f"""
                SELECT
                    l_returnflag,
                    l_linestatus,
                    SUM(l_quantity) AS sum_qty,
                    SUM(l_extendedprice) AS sum_base_price,
                    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                    AVG(l_quantity) AS avg_qty,
                    AVG(l_extendedprice) AS avg_price,
                    AVG(l_discount) AS avg_disc,
                    COUNT(*) AS count_order
                FROM
                    tpc_h.lineitem
                WHERE
                    l_shipdate <= DATE '1998-12-01' - INTERVAL '{delta}' DAY
                GROUP BY
                    l_returnflag,
                    l_linestatus
                ORDER BY
                    l_returnflag,
                    l_linestatus;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        delta = randint(DELTA_START, DELTA_END)

        start_time = time.time()
        execute(cursor, delta)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()