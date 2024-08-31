if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

WORDS1 = ['special', 'pending', 'unusual', 'express']
WORDS2 = ['packages', 'requests', 'accounts', 'deposit']

def execute(cursor, word1, word2):
    cursor.execute(f"""
                SELECT
                    c_count,
                    COUNT(*) AS custdist
                FROM
                    (
                        SELECT
                            c_custkey,
                            COUNT(o_orderkey)
                        FROM
                            customer LEFT OUTER JOIN orders ON
                                c_custkey = o_custkey
                                AND o_comment NOT LIKE '%{word1}%{word2}%'
                        GROUP BY
                            c_custkey
                    ) AS c_orders (c_custkey, c_count)
                GROUP BY
                    c_count
                ORDER BY
                    custdist DESC,
                    c_count DESC;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        word1 = choice(WORDS1)
        word2 = choice(WORDS2)

        start_time = time.time()
        execute(cursor, word1, word2)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()