if __name__ == "__main__":
    from mongodb import client_mongo
else:
    from mongo_queries.mongodb import client_mongo
from datetime import datetime, timedelta
from random import randint, choice
import time


SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']

db = client_mongo['tpc-h']
collection = db['lineitem']
pipeline = [
    {
        "$lookup": {
            "from": "orders",
            "localField": "l_orderkey",
            "foreignField": "o_orderkey",
            "as": "orders"
        }
    },
    {
        "$unwind": "$orders"
    },
    {
        "$lookup": {
            "from": "customer",
            "localField": "orders.o_custkey",
            "foreignField": "c_custkey",
            "as": "customers"
        }
    },
    {
        "$unwind": "$customers"
    },
    {
        "$match": {
            "customers.c_mktsegment": "BUILDING",
            "orders.o_orderdate": {
                "$lt": datetime.strptime("1995-03-15", "%Y-%m-%d")
            },
            "l_shipdate": {
                "$gt": datetime.strptime("1995-03-15", "%Y-%m-%d")
            }
        }
    },
    {
        "$project": {
            "l_orderkey": 1,
            "revenue": {
                "$multiply": [
                    "$l_extendedprice",
                    {
                        "$subtract": [1, "$l_discount"]
                    }
                ]
            },
            "o_orderdate": "$orders.o_orderdate",
            "o_shippriority": "$orders.o_shippriority"
        }
    },
    {
        "$group": {
            "_id": {
                "l_orderkey": "$l_orderkey",
                "o_orderdate": "$o_orderdate",
                "o_shippriority": "$o_shippriority"
            },
            "total_revenue": {
                "$sum": "$revenue"
            }
        }
    },
    {
        "$sort": {
            "total_revenue": -1,
            "_id.o_orderdate": 1
        }
    },
    {
        "$project": {
            "_id": 0,
            "l_orderkey": "$_id.l_orderkey",
            "total_revenue": 1,
            "o_orderdate": "$_id.o_orderdate",
            "o_shippriority": "$_id.o_shippriority"
        }
    }
]

def execute(pipeline):
    collection.aggregate(pipeline)



def run_benchmark():
    execution_times = []
    for _ in range(0, 5):
        print(_)
        segment = choice(SEGMENTS)
        date = f'1995-03-{randint(1, 31)}'

        pipeline[4]["$match"]["customers.c_mktsegment"] = segment
        pipeline[4]["$match"]["orders.o_orderdate"]["$lt"] = datetime.strptime(date, "%Y-%m-%d")
        pipeline[4]["$match"]["l_shipdate"]["$gt"] = datetime.strptime(date, "%Y-%m-%d")

        start_time = time.time()
        execute(pipeline)
        end_time = time.time()

        print("Tempo de execução: ", end_time - start_time)
        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()