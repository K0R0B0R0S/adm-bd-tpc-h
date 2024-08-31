if __name__ == "__main__":
    from mongodb import client_mongo
else:
    from mongo_queries.mongodb import client_mongo
from datetime import datetime, timedelta
from random import randint, choice
import time


REGIONS = [
    "AFRICA",
    "AMERICA",
    "ASIA",
    "EUROPE",
    "MIDDLE EAST"
]

db = client_mongo['tpc-h']
collection = db['lineitem']
pipeline = [
    {
        "$lookup": {
            "from": "orders",
            "localField": "l_orderkey",
            "foreignField": "o_orderkey",
            "as": "order"
        }
    },
    {
        "$unwind": "$order"
    },
    {
        "$lookup": {
            "from": "customer",
            "localField": "order.o_custkey",
            "foreignField": "c_custkey",
            "as": "customer"
        }
    },
    {
        "$unwind": "$customer"
    },
    {
        "$lookup": {
            "from": "supplier",
            "localField": "l_suppkey",
            "foreignField": "s_suppkey",
            "as": "supplier"
        }
    },
    {
        "$unwind": "$supplier"
    },
    {
        "$lookup": {
            "from": "nation",
            "localField": "supplier.s_nationkey",
            "foreignField": "n_nationkey",
            "as": "nation"
        }
    },
    {
        "$unwind": "$nation"
    },
    {
        "$lookup": {
            "from": "region",
            "localField": "nation.n_regionkey",
            "foreignField": "r_regionkey",
            "as": "region"
        }
    },
    {
        "$unwind": "$region"
    },
    {
        "$match": {
            "region.r_name": "ASIA",
            "order.o_orderdate": {
                "$gte": None,
                "$lt": None
            }
        }
    },
    {
        "$project": {
            "n_name": "$nation.n_name",
            "revenue": {
                "$multiply": [
                    "$l_extendedprice",
                    {
                        "$subtract": [1, "$l_discount"]
                    }
                ]
            }
        }
    },
    {
        "$group": {
            "_id": "$n_name",
            "total_revenue": {
                "$sum": "$revenue"
            }
        }
    },
    {
        "$sort": {
            "total_revenue": -1
        }
    }
]

def execute(pipeline):
    collection.aggregate(pipeline)



def run_benchmark():
    execution_times = []
    for _ in range(0, 5):
        print(_)
        region = choice(REGIONS)
        date = f'{randint(1993, 1998)}-01-01'

        pipeline[10]["$match"]["region.r_name"] = region
        pipeline[10]["$match"]["order.o_orderdate"]["$gte"] = datetime.strptime(date, "%Y-%m-%d")
        pipeline[10]["$match"]["order.o_orderdate"]["$lt"] = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=365)

        start_time = time.time()
        execute(pipeline)
        end_time = time.time()

        print("Tempo de execução: ", end_time - start_time)
        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()