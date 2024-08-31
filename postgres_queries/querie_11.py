if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

SF = 0.1
NATIONS = [
    'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
    ]

def execute(cursor, nation, fraction):
    cursor.execute(f"""
                SELECT
                    ps_partkey,
                    SUM(ps_supplycost * ps_availqty) AS value
                FROM
                    partsupp,
                    supplier,
                    nation
                WHERE
                    ps_suppkey = s_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_name = '{nation}'
                GROUP BY
                    ps_partkey
                HAVING
                    SUM(ps_supplycost * ps_availqty) > (
                        SELECT
                            SUM(ps_supplycost * ps_availqty) * {fraction}
                        FROM
                            partsupp,
                            supplier,
                            nation
                        WHERE
                            ps_suppkey = s_suppkey
                            AND s_nationkey = n_nationkey
                            AND n_name = '{nation}'
                    )
                ORDER BY
                    value DESC;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        nation = choice(NATIONS)
        fraction = 0.0001 / SF

        start_time = time.time()
        execute(cursor, nation, fraction)
        end_time = time.time()
        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()