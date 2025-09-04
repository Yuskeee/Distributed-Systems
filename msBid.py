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
    # Handling of the auction object for sending via RabbitMQ
    json_string = json.dumps(data, default=json_serial)
    json_bytes = json_string.encode('utf-8')
    # This function now returns bytes, not base64 encoded bytes. 
    # If the publisher expects base64, keep the b64encode line.
    # For clarity, let's assume the publisher sends plain JSON bytes.
    return json_bytes


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='leilao_iniciado')
    channel.queue_declare(queue='lance_realizado')
    channel.queue_declare(queue='leilao_finalizado')

    auctionList = []
    # FIX 1: Initialize lastBid as a dictionary, not a list
    lastBid = {}

    def callback(ch, method, properties, body):
        print(f" [x] Received raw message from queue '{method.routing_key}'")
        
        # FIX 2: Decode and parse the JSON message ONCE at the beginning.
        try:
            # Assuming the body is a UTF-8 encoded JSON string
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
            # --- Signature Verification Logic ---
            try:
                # Extract the bid data and the signature string from the payload
                bid_data = data["bid"]
                signature_b64 = data["signature"]

                # Prepare the data that was signed (it must be exactly the same as the sender)
                key = RSA.import_key(open('keys/public_0.pem').read())
                # The hash should be over the canonical representation of the bid data
                hash_obj = SHA256.new(json.dumps(bid_data, sort_keys=True).encode('utf-8'))

                # FIX 3: Decode the Base64 signature string into bytes before verifying
                signature_bytes = base64.b64decode(signature_b64)
                
                print(f" [i] Verifying signature of length {len(signature_bytes)} bytes...")
                pkcs1_15.new(key).verify(hash_obj, signature_bytes)
                print(" [+] The signature is valid.")

            except (ValueError, TypeError, KeyError) as e:
                # ValueError for bad signature, KeyError if 'bid' or 'signature' is missing
                print(f" [!] The signature is NOT valid or message is malformed. Error: {e}")
                # Do not process an invalid bid
                return
            
            # --- Bid Processing Logic (only runs if signature is valid) ---
            print("auction id:", auctionList[0]["auction_id"])
            print(data["auction_id"])
            auction = next((a for a in auctionList if a["auction_id"] == data["auction_id"]), None)

            if auction is None:
                print(f" [!] Auction not found.")
                return

            if auction["status"] != "ativo":
                print(f" [!] Auction {data['auction_id']} is already finished.")
                return

            # Check if this is the first bid or a higher bid
            if data["auction_id"] not in lastBid or data["bid_value"] > lastBid[data["auction_id"]]["bid_value"]:
                # The to_json function here should probably just dump the dict to a string and encode it.
                # If you need to send base64, you should rename the function to be more descriptive.
                channel.basic_publish(exchange='', routing_key="lance_validado", body=json.dumps(data).encode('utf-8'))
                lastBid[data["auction_id"]] = data
                print(f" [+] Bid accepted for auction {auction['auction_id']}: {data}")
            else:
                print(f" [-] Bid rejected for auction {auction['auction_id']}: Bid value is not higher.")

    channel.basic_consume(queue='leilao_iniciado', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(queue='lance_realizado', on_message_callback=callback, auto_ack=True)
    # channel.basic_consume(queue='leilao_finalizado', on_message_callback=callback, auto_ack=True)

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