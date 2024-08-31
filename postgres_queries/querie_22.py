if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time
import random

def generate_country_codes(nations, num_codes=7):
    country_codes = [str(index + 10) for index in range(len(nations))]
    
    selected_codes = random.sample(country_codes, num_codes)
    
    return selected_codes

nations = [
    'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
]

def execute(cursor, country_codes_str):
    cursor.execute(f"""
        SELECT
            cntrycode,
            COUNT(*) AS numcust,
            SUM(c_acctbal) AS totacctbal
        FROM (
            SELECT
                SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
                c_acctbal
            FROM
                customer
            WHERE
                SUBSTRING(c_phone FROM 1 FOR 2) IN ({country_codes_str})
                AND c_acctbal > (
                    SELECT
                        AVG(c_acctbal)
                    FROM
                        customer
                    WHERE
                        c_acctbal > 0.00
                        AND SUBSTRING(c_phone FROM 1 FOR 2) IN ({country_codes_str})
                )
                AND NOT EXISTS (
                    SELECT
                        *
                    FROM
                        orders
                    WHERE
                        o_custkey = c_custkey
                )
        ) AS custsale
        GROUP BY
            cntrycode
        ORDER BY
            cntrycode;
    """)

    print(cursor.fetchall()) 



def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):
        country_codes = generate_country_codes(nations)
        country_codes_str = ', '.join(f"'{code}'" for code in country_codes)
        start_time = time.time()
        execute(cursor, country_codes_str)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()