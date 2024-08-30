if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

NATIONS = [
    'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
]

def execute(cursor, nation1, nation2):
    cursor.execute(f"""
                SELECT
                    supp_nation,
                    cust_nation,
                    l_year,
                    SUM(volume) AS revenue
                FROM
                    (
                        SELECT
                            n1.n_name AS supp_nation,
                            n2.n_name AS cust_nation,
                            EXTRACT(YEAR FROM l_shipdate) AS l_year,
                            l_extendedprice * (1 - l_discount) AS volume
                        FROM
                            supplier,
                            lineitem,
                            orders,
                            customer,
                            nation n1,
                            nation n2
                        WHERE
                            s_suppkey = l_suppkey
                            AND o_orderkey = l_orderkey
                            AND c_custkey = o_custkey
                            AND s_nationkey = n1.n_nationkey
                            AND c_nationkey = n2.n_nationkey
                            AND (
                                (n1.n_name = '{nation1}' AND n2.n_name = '{nation2}')
                                OR (n1.n_name = '{nation2}' AND n2.n_name = '{nation1}')
                            )
                            AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                    ) AS shipping
                GROUP BY
                    supp_nation,
                    cust_nation,
                    l_year
                ORDER BY
                    supp_nation,
                    cust_nation,
                    l_year;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):
        nation1 = choice(NATIONS)
        nation2 = choice([nation for nation in NATIONS if nation != nation1])

        start_time = time.time()
        execute(cursor, nation1, nation2)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()