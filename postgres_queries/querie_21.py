if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import choice
import time

NATIONS = [
    'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
]

def execute(cursor, nation):
    cursor.execute(f"""
                SELECT
                    s_name,
                    COUNT(*) AS numwait
                FROM
                    supplier,
                    lineitem l1,
                    orders,
                    nation
                WHERE
                    s_suppkey = l1.l_suppkey
                    AND o_orderkey = l1.l_orderkey
                    AND o_orderstatus = 'F'
                    AND l1.l_receiptdate > l1.l_commitdate
                    AND EXISTS (
                        SELECT
                            *
                        FROM
                            lineitem l2
                        WHERE
                            l2.l_orderkey = l1.l_orderkey
                            AND l2.l_suppkey <> l1.l_suppkey
                    )
                    AND NOT EXISTS (
                        SELECT
                            *
                        FROM
                            lineitem l3
                        WHERE
                            l3.l_orderkey = l1.l_orderkey
                            AND l3.l_suppkey <> l1.l_suppkey
                            AND l3.l_receiptdate > l3.l_commitdate
                    )
                    AND s_nationkey = n_nationkey
                    AND n_name = '{nation}'
                GROUP BY
                    s_name
                ORDER BY
                    numwait DESC,
                    s_name;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        nation = choice(NATIONS)

        start_time = time.time()
        execute(cursor, nation)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()