if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice, sample
import time


def generate_random_brand():
    return f"Brand#{randint(1, 5)}{randint(1, 5)}"

def generate_random_sizes():
    return sample(range(1, 51), 8)  # Seleciona 8 valores únicos entre 1 e 50

TYPES = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']

def execute(cursor, brand, type_prefix, sizes):
    cursor.execute(f"""
                SELECT
            p_brand,
            p_type,
            p_size,
            COUNT(DISTINCT ps_suppkey) AS supplier_cnt
        FROM
            partsupp,
            part
        WHERE
            p_partkey = ps_partkey
            AND p_brand <> '{brand}'
            AND p_type NOT LIKE '{type_prefix}%'
            AND p_size IN ({', '.join(map(str, sizes))})
            AND ps_suppkey NOT IN (
                SELECT
                    s_suppkey
                FROM
                    supplier
                WHERE
                    s_comment LIKE '%Customer%Complaints%'
            )
        GROUP BY
            p_brand,
            p_type,
            p_size
        ORDER BY
            supplier_cnt DESC,
            p_brand,
            p_type,
            p_size;
                """)
    print(cursor.fetchall())

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):
        brand = generate_random_brand()  # Gera a marca aleatória
        type_prefix = choice(TYPES)  # Gera o prefixo do tipo aleatório
        sizes = generate_random_sizes()

        start_time = time.time()
        execute(cursor, brand, type_prefix, sizes)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()