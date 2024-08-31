if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

MODES = ['REG AIR', 'AIR', 'RAIL', 'SHIP', 'TRUCK', 'MAIL', 'FOB']

def execute(cursor, shipmode1, shipmode2, date):
    cursor.execute(f"""
        SELECT
            l_shipmode,
            SUM(CASE
                WHEN o_orderpriority = '1-URGENT'
                     OR o_orderpriority = '2-HIGH'
                THEN 1
                ELSE 0
            END) AS high_line_count,
            SUM(CASE
                WHEN o_orderpriority <> '1-URGENT'
                     AND o_orderpriority <> '2-HIGH'
                THEN 1
                ELSE 0
            END) AS low_line_count
        FROM
            orders,
            lineitem
        WHERE
            o_orderkey = l_orderkey
            AND l_shipmode IN ('{shipmode1}', '{shipmode2}')
            AND l_commitdate < l_receiptdate
            AND l_shipdate < l_commitdate
            AND l_receiptdate >= DATE '{date}'
            AND l_receiptdate < DATE '{date}' + INTERVAL '1 year'
        GROUP BY
            l_shipmode
        ORDER BY
            l_shipmode;
    """)
    print(cursor.fetchall()) 

def generate_random_shipmodes():
    shipmode1 = choice(MODES)
    shipmode2 = choice([mode for mode in MODES if mode != shipmode1])
    return shipmode1, shipmode2

def generate_random_date():
    year = randint(1993, 1997)
    date = f'{year}-01-01' 
    return date

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):

        shipmode1, shipmode2 = generate_random_shipmodes()  # Gera dois modos de envio aleatórios e distintos
        date = generate_random_date()

        start_time = time.time()
        execute(cursor, shipmode1, shipmode2, date)
        end_time = time.time()
        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()