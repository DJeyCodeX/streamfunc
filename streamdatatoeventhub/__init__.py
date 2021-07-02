import logging

import azure.functions as func
from faker import Faker
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import json
import random
import math
from datetime import datetime

from streamdatatoeventhub.sku import SkuData
from streamdatatoeventhub.customer_id import AddCustomerId
from streamdatatoeventhub.payment_mode import PaymentMode
from streamdatatoeventhub.lat_long import LatLongData

def generatedData():

    fake = Faker()

    #Add Customer
    fake.add_provider(AddCustomerId)
    customer_id_data = fake.customerdata()

    #Add Payment
    fake.add_provider(PaymentMode)
    payments_mode = fake.payment()

    #Add SKU
    fake.add_provider(SkuData)
    sku_id_data = fake.sku_data()
    price = SkuData.sku_price(sku_id_data)

    #Add latlong
    lat_long_index = math.floor(random.random() * 33144)
    lat_long_arr = LatLongData.get_lat_long(lat_long_index)
    lat_long_arr_data = str(lat_long_arr[0]) +","+ str(lat_long_arr[1])

    #Add Orderdate
    current_date = datetime.now()
    order_date = current_date.timestamp()

    #Add RandomNumber
    product_quantity = math.floor(random.random() * 80)

    #Add AmountSpent
    amount_spent = product_quantity*int(float(price))

    #make json data
    quick_data = {
        "customerID":customer_id_data,
        "sku":sku_id_data,
        "orderDate":order_date,
        "randomNumber":product_quantity,
        "amountSpent":amount_spent,
        "latLong":lat_long_arr_data,
        "paymentMode":payments_mode
    }
    return quick_data


def main(req: func.HttpRequest, outputHub: func.Out[str]) -> func.HttpResponse:
    logging.info('Process Started...')
    try:
        req_body = req.get_json()
        logging.info(req_body)
        if req_body['name'] == "senddatatoeventhub":
            logging.info("RequestBody Identified...")
            records = []
            for j in range(0,100):
                json_data = generatedData()
                logging.info("Data Generated...")
                logging.info(json_data)
                records.append(json_data)
                logging.info("Records...")
                logging.info(records)
                if len(records) == 100:
                    logging.info(f"Started Writing to Event Hub...")
                    response = outputHub.set("[" + ",".join([str(i) for i in records]) + "]")
                    logging.info("Successfully written to Event Hub...")
                    break
        return func.HttpResponse("Process Completed...")
    except Exception as e:
        return func.HttpResponse(str(e), status_code=400)
        