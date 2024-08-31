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

REGIONS = [
    "AFRICA",
    "AMERICA",
    "ASIA",
    "EUROPE",
    "MIDDLE EAST"
]

Syllable1 = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO']
Syllable2 = ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED']
Syllable3 = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']

def execute(cursor, nation, region, type1, type2, type3):
    cursor.execute(f"""
        SELECT
            o_year,
            SUM(CASE
                WHEN nation = '{nation}'
                THEN volume
                ELSE 0
            END) / SUM(volume) AS mkt_share
        FROM (
            SELECT
                EXTRACT(YEAR FROM o_orderdate) AS o_year,
                l_extendedprice * (1 - l_discount) AS volume,
                n2.n_name AS nation
            FROM
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            WHERE
                p_partkey = l_partkey
                AND s_suppkey = l_suppkey
                AND l_orderkey = o_orderkey
                AND o_custkey = c_custkey
                AND c_nationkey = n1.n_nationkey
                AND n1.n_regionkey = r_regionkey
                AND r_name = '{region}'
                AND s_nationkey = n2.n_nationkey
                AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                AND p_type = '{type1} {type2} {type3}'
        ) AS all_nations
        GROUP BY
            o_year
        ORDER BY
            o_year;
    """)
    print(cursor.fetchall())    

def generate_random_nation():
    return choice(NATIONS)

def generate_region_for_nation(nation):
    nation_region_map = {
        "ALGERIA": "AFRICA",
        "EGYPT": "AFRICA",
        "ETHIOPIA": "AFRICA",
        "KENYA": "AFRICA",
        "MOROCCO": "AFRICA",
        "MOZAMBIQUE": "AFRICA",
        "ARGENTINA": "AMERICA",
        "BRAZIL": "AMERICA",
        "CANADA": "AMERICA",
        "PERU": "AMERICA",
        "UNITED STATES": "AMERICA",
        "CHINA": "ASIA",
        "INDIA": "ASIA",
        "INDONESIA": "ASIA",
        "JAPAN": "ASIA",
        "JORDAN": "MIDDLE EAST",
        "IRAN": "MIDDLE EAST",
        "IRAQ": "MIDDLE EAST",
        "SAUDI ARABIA": "MIDDLE EAST",
        "FRANCE": "EUROPE",
        "GERMANY": "EUROPE",
        "ROMANIA": "EUROPE",
        "RUSSIA": "EUROPE",
        "UNITED KINGDOM": "EUROPE"
    }
    return nation_region_map.get(nation)

def generate_random_type():
    return choice(TYPES)  # Seleciona um TYPE aleatoriamente

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):
        nation = choice(NATIONS)
        region = generate_region_for_nation(nation)
        type1 = choice(Syllable1)
        type2 = choice(Syllable2)
        type3 = choice(Syllable3)

        start_time = time.time()
        execute(cursor, nation, region, type1, type2, type3)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()