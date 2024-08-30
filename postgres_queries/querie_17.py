if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

START = 1
END = 5
SYLLABLE_1 = ["SM", "LG", "MED", "JUMBO", "WRAP"]
SYLLABLE_2 = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]

def execute(cursor, brand, container):
    cursor.execute(f"""
                SELECT
                    SUM(l_extendedprice) / 7.0 AS avg_yearly
                FROM
                    lineitem,
                    part
                WHERE
                    p_partkey = l_partkey
                    AND p_brand = '{brand}'
                    AND p_container = '{container}'
                    AND l_quantity < (
                        SELECT
                            0.2 * AVG(l_quantity)
                        FROM
                            lineitem
                        WHERE
                            l_partkey = p_partkey
                    );
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        print(_)
        M = randint(START, END)
        N = randint(START, END)
        brand = f"Brand#{M}{N}"
        container = f"{choice(SYLLABLE_1)} {choice(SYLLABLE_2)}"

        start_time = time.time()
        execute(cursor, brand, container)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()