if __name__ == "__main__":
    from mongodb import client_mongo
else:
    from mongo_queries.mongodb import client_mongo
from datetime import datetime, timedelta
from random import randint
import time

DELTA_START = 60
DELTA_END = 120

db = client_mongo['tpc-h']
collection = db['lineitem']
pipeline = [
    {
        "$match": {
            "l_shipdate": {"$lte": None}
        }
    },
    {
        "$group": {
            "_id": {
                "l_returnflag": "$l_returnflag",
                "l_linestatus": "$l_linestatus"
            },
            "sum_qty": {"$sum": "$l_quantity"},
            "sum_base_price": {"$sum": "$l_extendedprice"},
            "sum_disc_price": {"$sum": {
                "$multiply": [
                    "$l_extendedprice",
                    {"$subtract": [1, "$l_discount"]}
                ]
            }},
            "sum_charge": {"$sum": {
                "$multiply": [
                    "$l_extendedprice",
                    {"$subtract": [1, "$l_discount"]},
                    {"$add": [1, "$l_tax"]}
                ]
            }},
            "avg_qty": {"$avg": "$l_quantity"},
            "avg_price": {"$avg": "$l_extendedprice"},
            "avg_disc": {"$avg": "$l_discount"},
            "count_order": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "_id.l_returnflag": 1,
            "_id.l_linestatus": 1
        }
    }
]

def execute(pipeline):
    collection.aggregate(pipeline)


def run_benchmark():
    execution_times = []
    for _ in range(0, 100):
        delta = randint(DELTA_START, DELTA_END)
        pipeline[0]["$match"]["l_shipdate"]["$lte"] = datetime(1998, 12, 1) - timedelta(days=delta)

        start_time = time.time()
        execute(pipeline)
        end_time = time.time()

        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()