if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

START_YEAR = 1993
END_YEAR = 1997
START_MONTH = 1
END_MONTH = 10

def execute(cursor, date):
    cursor.execute(f"""
                CREATE VIEW revenue0 (supplier_no, total_revenue) AS
                    SELECT
                        l_suppkey,
                        SUM(l_extendedprice * (1 - l_discount))
                    FROM
                        lineitem
                    WHERE
                        l_shipdate >= DATE '{date}'
                        AND l_shipdate < DATE '{date}' + INTERVAL '3' MONTH
                    GROUP BY
                        l_suppkey;

                SELECT
                    s_suppkey,
                    s_name,
                    s_address,
                    s_phone,
                    total_revenue
                FROM
                    supplier,
                    revenue0
                WHERE
                    s_suppkey = supplier_no
                    AND total_revenue = (
                        SELECT
                            MAX(total_revenue)
                        FROM
                            revenue0
                    )
                ORDER BY
                    s_suppkey;

                DROP VIEW revenue0;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 5):
        date = f"{randint(START_YEAR, END_YEAR)}-{randint(START_MONTH, END_MONTH)}-01"

        start_time = time.time()
        execute(cursor, date)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()