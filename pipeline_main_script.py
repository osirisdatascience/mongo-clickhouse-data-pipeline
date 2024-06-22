from datetime import timedelta, date
from pymongo import MongoClient
from clickhouse_connect import get_client


#  abstracting a function to return data from the Data Lake MongoDB
def query_mongo_datalake(aggregation, conn):

    client = MongoClient(conn['MONGO_IP']) 
    db = client[conn['DB_NAME']]
    collection = db[conn['COLLECTION']]

    results = collection.aggregate(aggregation)

    return results


#  abstracting a function to insert data into the DW CLICKHOUSE server
def insert_data_clickhouse_DW(command, table_name, db, connx):

    client = get_client(
                host = connx['HOST'], 
                port = connx['PORT'],
                username = connx['USER'],
                password = connx['PASSWORD'],
                database = db )
    
    client.command(command, table_name)


def main():

    # datalake mongo  
    DL_MONGO_CONN = { 'MONGO_IP': '0.0.0.0', 
                    'DB_NAME':'general_sales_from_system',
                    'COLLECTION':'collectionname'}  
    
    # dw clickhouse credentials
    DW_CLICKHOUSE_CONN = {'HOST':'0.0.0.0',
            'PORT' : 'XXXX',
            'USER' : 'default_user',
            'PASSWORD' : 'default_password'}


    today = date.todau()
    yesterday = today - timedelta(days=1)

    #  mongo query to return all the 
    AGGREGATION = [
        {
            "$match": {
                "sale_date": {
                    "$gte": yesterday,
                    "$lt": today
                    },
                "discount": 0,
                "status": "payment_accepted"
            }
        },
        {
        "$project": {
            "sale_date": 1,
            "customer_id": 1,
            "city": "$shipping_address.city",
            "total_amount": {
                "$multiply": ["$quantity", "$amount_value"]
                    }
                }
            }
        ]

    results_from_datalake = query_mongo_datalake(aggregation= AGGREGATION, conn = DL_MONGO_CONN)

    #  loop to insert the sales data per country, in the DW CLICKHOUSE, to use in a possible Data Viz Project

    for i in results_from_datalake:

        sale_date = i['sale_date']
        customer_id = i['customer_id']
        city = i['city']
        total_amount = i['total_amount']

        dw_clickhouse_insert = f"""
                                INSERT INTO  
                                (sale_date, customer_id, city, total_amount)
                                VALUES (
                                    '{sale_date}', 
                                    {customer_id}, 
                                    '{city}', 
                                    {total_amount},  
                                )
                                """

        insert_data_clickhouse_DW(command= dw_clickhouse_insert ,
                                  table_name='', 
                                  db='', 
                                  connx = DW_CLICKHOUSE_CONN)
        


