#!/usr/bin/env python
import pika
import datetime
import json
import sched
import base64
import time
import sys
import os

def json_serial(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat() # Converts date/time to ISO 8601 string format
    raise TypeError(f"Type {type(obj)} not serializable in JSON")

def to_json(data):
    # Handling of the auction object for sending via RabbitMQ
    json_string = json.dumps(data, default=json_serial)
    json_bytes = json_string.encode('utf-8')
    base64_bytes = base64.b64encode(json_bytes)
    return base64_bytes

def send_auction_end(auction):
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    auction["status"] = "finished"
    channel.basic_publish(exchange='', routing_key='leilao_finalizado', body=to_json(auction))

# Creating the structure for a specific auction
auction_01 = {
    "auction_id": 101,
    "description": "Latest generation gaming notebook",
    "start_date": datetime.datetime(2025, 9, 2, 14), # Year, Month, Day, Hour
    "end_date": datetime.datetime(2025, 9, 3, 16),
    "status": "active"
}

def main():
    # Code snippet to create the queues under the Auction's responsibility
    s = sched.scheduler(time.time, time.sleep)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='leilao_iniciado')
    channel.queue_declare(queue='leilao_finalizado')

    if (datetime.datetime.now()) >= auction_01["start_date"] and (datetime.datetime.now()) <= auction_01["end_date"]:
        channel.basic_publish(exchange='', routing_key='leilao_iniciado', body=to_json(auction_01))
        print(" [x] Sent ")
        connection.close()

        
        # Scheduling the end of the auction
        end_auction = (auction_01["end_date"]).timestamp()
        print(f"Auction will end in {end_auction} seconds.")
        s.enterabs(end_auction, 1, send_auction_end, argument=(
                      auction_01,
                    ))
        s.run()
    
    elif (datetime.datetime.now()) > auction_01["end_date"]:
        auction_01["status"] = "finished"
        channel.basic_publish(exchange='', routing_key='leilao_finalizado', body=to_json(auction_01))
        print(" [x] Sent ")
        connection.close()

    while True:
        pass


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)