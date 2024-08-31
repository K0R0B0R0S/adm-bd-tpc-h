if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

DELTA_START = 60
DELTA_END = 120

REGIONS = [
    "AFRICA",
    "AMERICA",
    "ASIA",
    "EUROPE",
    "MIDDLE EAST"
]
lista = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']

def execute(cursor, lista, size, region):
    cursor.execute(f"""
                select
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
        from
                part,
                supplier,
                partsupp,
                nation,
                region
        where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and p_size = '{size}'
                and p_type like '%{lista}'
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = '{region}'
                and ps_supplycost = (
                        select
                                min(ps_supplycost)
                        from
                                partsupp,
                                supplier,
                                nation,
                                region
                        where
                                p_partkey = ps_partkey
                                and s_suppkey = ps_suppkey
                                and s_nationkey = n_nationkey
                                and n_regionkey = r_regionkey
                                and r_name = '{region}'
                )
        order by
                s_acctbal desc,
                n_name,
                s_name,
                p_partkey;
                """)
    print(cursor.fetchall())

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):
        size = randint(1, 50)
        region = choice(REGIONS)
        delta = choice(lista)
        start_time = time.time()
        execute(cursor, delta, size, region)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()

    lista = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']