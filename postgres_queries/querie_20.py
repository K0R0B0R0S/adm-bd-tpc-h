if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time


# Lista de cores, assumindo que são definidas na geração de P_NAME
COLORS = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched",
"blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate",
"coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger",
"drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod",
"green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn",
"lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight",
"mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale",
"papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose",
"rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke",
"snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat",
"white", "yellow"]  # Exemplo de cores

# Lista de nações, conforme especificado
NATIONS = [
    'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
]

def execute(cursor, color, date, nation):
    cursor.execute(f"""
        SELECT
            s_name,
            s_address
        FROM
            supplier,
            nation
        WHERE
            s_suppkey IN (
                SELECT
                    ps_suppkey
                FROM
                    partsupp
                WHERE
                    ps_partkey IN (
                        SELECT
                            p_partkey
                        FROM
                            part
                        WHERE
                            p_name LIKE '{color}%'
                    )
                    AND ps_availqty > (
                        SELECT
                            0.5 * SUM(l_quantity)
                        FROM
                            lineitem
                        WHERE
                            l_partkey = ps_partkey
                            AND l_suppkey = ps_suppkey
                            AND l_shipdate >= DATE '{date}'
                            AND l_shipdate < DATE '{date}' + INTERVAL '1 year'
                    )
            )
            AND s_nationkey = n_nationkey
            AND n_name = '{nation}';
    """)

    print(cursor.fetchall()) 

def generate_random_color():
    # Seleciona uma cor aleatória
    return choice(COLORS)

def generate_random_date():
    # Gera a data como o primeiro dia de janeiro de um ano aleatório entre 1993 e 1997
    year = randint(1993, 1997)
    date = f'{year}-01-01'  # Primeiro dia de janeiro do ano selecionado
    return date

def generate_random_nation():
    # Seleciona uma nação aleatória
    return choice(NATIONS)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):

        color = generate_random_color()  # Gera a cor aleatória
        date = generate_random_date()    # Gera a data aleatória
        nation = generate_random_nation()

        start_time = time.time()
        execute(cursor, color, date, nation)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()