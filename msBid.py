import pika, datetime, sys, os, base64, json
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

def json_serial(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable in JSON")

def to_json(data):
    json_string = json.dumps(data, default=json_serial)
    json_bytes = json_string.encode('utf-8')
    return json_bytes


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='lance_realizado')
    auction_open_queue = channel.queue_declare(queue='', exclusive=True)
    queue_name_open = auction_open_queue.method.queue

    channel.queue_bind(exchange='leilao_iniciado',
                    queue=queue_name_open,
                    routing_key='leilao_iniciado')
    
    auction_closed_queue = channel.queue_declare(queue='', exclusive=True)
    queue_name_closed = auction_closed_queue.method.queue

    channel.queue_bind(exchange='leilao_finalizado',
                    queue=queue_name_closed,
                    routing_key='leilao_finalizado')

    auctionList = []
    lastBid = {}

    def callback(ch, method, properties, body):
        print(f" [x] Received raw message from queue '{method.routing_key}'")
        
        try:
            message_str = body.decode('utf-8')
            data = json.loads(message_str)
            print(f" [x] Decoded data: {data}")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f" [!] Could not decode or parse message: {e}")
            return

        queue_origem = method.routing_key
        print("queue origin: ", queue_origem)

        if queue_origem == "leilao_iniciado":
            auctionList.append(data)
            print(f" [+] New auction started and added to list: {data['auction_id']}")

        elif queue_origem == "lance_realizado":
            try:
                bid_data = data["bid"]
                signature_b64 = data["signature"]

                key = RSA.import_key(open('keys/public_0.pem').read())
                hash_obj = SHA256.new(json.dumps(bid_data, sort_keys=True).encode('utf-8'))

                signature_bytes = base64.b64decode(signature_b64)
                
                print(f" [i] Verifying signature of length {len(signature_bytes)} bytes...")
                pkcs1_15.new(key).verify(hash_obj, signature_bytes)
                print(" [+] The signature is valid.")

            except (ValueError, TypeError, KeyError) as e:
                print(f" [!] The signature is NOT valid or message is malformed. Error: {e}")
                return
            
            print("auction id:", auctionList[0]["auction_id"])
            print(data["auction_id"])
            auction = next((a for a in auctionList if a["auction_id"] == data["auction_id"]), None)

            if auction is None:
                print(f" [!] Auction not found.")
                return

            if auction["status"] != "ativo":
                print(f" [!] Auction {data['auction_id']} is already finished.")
                return

            if data["auction_id"] not in lastBid or data["bid_value"] > lastBid[data["auction_id"]]["bid_value"]:
                channel.basic_publish(exchange='', routing_key="lance_validado", body=json.dumps(data).encode('utf-8'))
                lastBid[data["auction_id"]] = data
                print(f" [+] Bid accepted for auction {auction['auction_id']}: {data}")
            else:
                print(f" [-] Bid rejected for auction {auction['auction_id']}: Bid value is not higher.")

    channel.basic_consume(queue=queue_name_open, on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=queue_name_closed, on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='leilao_finalizado', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)