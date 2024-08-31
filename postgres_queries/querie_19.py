if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint
import time


def execute(cursor, brand1, quantity1, brand2, quantity2, brand3, quantity3):
    cursor.execute(f"""
                SELECT
                    SUM(l_extendedprice * (1 - l_discount)) AS revenue
                FROM
                    lineitem,
                    part
                WHERE
                    (
                        p_partkey = l_partkey
                        AND p_brand = '{brand1}'
                        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                        AND l_quantity BETWEEN {quantity1} AND {quantity1 + 10}
                        AND p_size BETWEEN 1 AND 5
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    OR
                    (
                        p_partkey = l_partkey
                        AND p_brand = '{brand2}'
                        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                        AND l_quantity BETWEEN {quantity2} AND {quantity2 + 10}
                        AND p_size BETWEEN 1 AND 10
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    OR
                    (
                        p_partkey = l_partkey
                        AND p_brand = '{brand3}'
                        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                        AND l_quantity BETWEEN {quantity3} AND {quantity3 + 10}
                        AND p_size BETWEEN 1 AND 15
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    );
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        quantity1 = randint(1, 10)
        quantity2 = randint(10, 20)
        quantity3 = randint(20, 30)
        brand1 = f"Brand#{randint(1, 5)}{randint(1, 5)}"
        brand2 = f"Brand#{randint(1, 5)}{randint(1, 5)}"
        brand3 = f"Brand#{randint(1, 5)}{randint(1, 5)}"

        start_time = time.time()
        execute(cursor, brand1, quantity1, brand2, quantity2, brand3, quantity3)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()