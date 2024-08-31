if __name__ == "__main__":
    from mongodb import client_mongo
else:
    from mongo_queries.mongodb import client_mongo
from datetime import datetime, timedelta
from random import randint, choice
import time


NATIONS = [
    'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
]

db = client_mongo['tpc-h']
collection = db['lineitem']
pipeline = [
    {
        "$match": {
            "l_shipdate": {
                "$gte": datetime(1995, 1, 1),
                "$lte": datetime(1996, 12, 31)
            }
        }
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
            "as": "customer"
        }
    },
    {
        "$unwind": "$customer"
    },
    {
        "$lookup": {
            "from": "nation",
            "localField": "supplier.s_nationkey",
            "foreignField": "n_nationkey",
            "as": "supp_nation"
        }
    },
    {
        "$unwind": "$supp_nation"
    },
    {
        "$lookup": {
            "from": "nation",
            "localField": "customer.c_nationkey",
            "foreignField": "n_nationkey",
            "as": "cust_nation"
        }
    },
    {
        "$unwind": "$cust_nation"
    },
    {
        "$match": {
            "$or": [
                {
                    "supp_nation.n_name": None,
                    "cust_nation.n_name": None
                },
                {
                    "supp_nation.n_name": None,
                    "cust_nation.n_name": None
                }
            ]
        }
    },
    {
        "$project": {
            "supp_nation": "$supp_nation.n_name",
            "cust_nation": "$cust_nation.n_name",
            "l_year": {
                "$year": "$l_shipdate"
            },
            "volume": {
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
            "_id": {
                "supp_nation": "$supp_nation",
                "cust_nation": "$cust_nation",
                "l_year": "$l_year"
            },
            "revenue": {
                "$sum": "$volume"
            }
        }
    },
    {
        "$sort": {
            "_id.supp_nation": 1,
            "_id.cust_nation": 1,
            "_id.l_year": 1
        }
    }
]

def execute(pipeline):
    collection.aggregate(pipeline)



def run_benchmark():
    execution_times = []
    for _ in range(0, 5):
        print(_)
        nation1 = choice(NATIONS)
        nation2 = choice([nation for nation in NATIONS if nation != nation1])

        pipeline[11]["$match"]["$or"][0]["supp_nation.n_name"] = nation1
        pipeline[11]["$match"]["$or"][0]["cust_nation.n_name"] = nation2

        start_time = time.time()
        execute(pipeline)
        end_time = time.time()

        print("Tempo de execução: ", end_time - start_time)
        execution_times.append(end_time - start_time)
        
    average_execution_time = sum(execution_times) / len(execution_times)
    print(f"Tempo de execução médio: {average_execution_time} segundos")

if __name__ == "__main__":
    run_benchmark()