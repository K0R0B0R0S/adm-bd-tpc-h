if __name__ == "__main__":
    from postgressql import client_postgres
else:
    from postgres_queries.postgressql import client_postgres
from random import randint, choice
import time

COLORS = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched",
    "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate",
    "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger",
    "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod",
    "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn",
    "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "mid-night", "mint", 
    "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale",
    "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose",
    "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke",
    "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat",
    "white", "yellow"]

def execute(cursor, color):
    cursor.execute(f"""
                SELECT
                    nation,
                    o_year,
                    SUM(amount) AS sum_profit
                FROM
                    (
                        SELECT
                            n_name AS nation,
                            EXTRACT(YEAR FROM o_orderdate) AS o_year,
                            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                        FROM
                            part,
                            supplier,
                            lineitem,
                            partsupp,
                            orders,
                            nation
                        WHERE
                            s_suppkey = l_suppkey
                            AND ps_suppkey = l_suppkey
                            AND ps_partkey = l_partkey
                            AND p_partkey = l_partkey
                            AND o_orderkey = l_orderkey
                            AND s_nationkey = n_nationkey
                            AND p_name LIKE '%{color}%'
                    ) AS profit
                GROUP BY
                    nation,
                    o_year
                ORDER BY
                    nation,
                    o_year DESC;
                """)

def run_benchmark():
    conn = client_postgres
    cursor = conn.cursor()
    cursor.execute("SET search_path TO 'tpc_h';")

    execution_times = []
    for _ in range(0, 100):
        color = choice(COLORS)

        start_time = time.time()
        execute(cursor, color)
        cursor.fetchall()
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()