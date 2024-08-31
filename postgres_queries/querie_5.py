if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

REGIONS = [
    "AFRICA",
    "AMERICA",
    "ASIA",
    "EUROPE",
    "MIDDLE EAST"
]

def execute(cursor, region, date):
    cursor.execute(f"""
                SELECT
                    n_name,
                    SUM(l_extendedprice * (1 - l_discount)) AS revenue
                FROM
                    customer,
                    orders,
                    lineitem,
                    supplier,
                    nation,
                    region
                WHERE
                    c_custkey = o_custkey
                    AND l_orderkey = o_orderkey
                    AND l_suppkey = s_suppkey
                    AND c_nationkey = s_nationkey
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_name = '{region}'
                    AND o_orderdate >= '{date}'
                    AND o_orderdate < '{date}'::date + INTERVAL '1 year'
                GROUP BY
                    n_name
                ORDER BY
                    revenue DESC;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        region = choice(REGIONS)
        date = f'{randint(1993, 1998)}-01-01'

        start_time = time.time()
        execute(cursor, region, date)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()