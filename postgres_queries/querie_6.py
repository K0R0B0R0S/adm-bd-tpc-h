if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice, uniform
import time

REGIONS = [
    "AFRICA",
    "AMERICA",
    "ASIA",
    "EUROPE",
    "MIDDLE EAST"
]

def execute(cursor, date, discount, quantity):
    cursor.execute(f"""
                SELECT
            SUM(l_extendedprice * l_discount) AS revenue
        FROM
            lineitem
        WHERE
            l_shipdate >= DATE '{date}'
            AND l_shipdate < DATE '{date}' + INTERVAL '1 year'
            AND l_discount BETWEEN {discount - 0.01:.2f} AND {discount + 0.01:.2f}
            AND l_quantity < {quantity};
                """)
    print(cursor.fetchall())
    
def generate_random_date():
    year = randint(1993, 1997)
    return f'{year}-01-01'  # Sempre o primeiro de janeiro

def generate_random_discount():
    return round(uniform(0.02, 0.09), 2)  # Desconto aleatório entre 0.02 e 0.09

def generate_random_quantity():
    return randint(24, 25)  # Quantidade aleatória entre 24 e 25

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):
        region = choice(REGIONS)
        date = generate_random_date()
        discount = generate_random_discount()
        quantity = generate_random_quantity()

        start_time = time.time()
        execute(cursor, date, discount, quantity)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()