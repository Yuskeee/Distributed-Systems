import pika, datetime, sys, os, base64, json
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

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


def main():
    message = ""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='lance_realizado')
    channel.queue_declare(queue='leilao_iniciado')
    channel.queue_declare(queue='leilao_finalizado')

    auctionList = []
    lastBid = []

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
        queue_origem = method.routing_key

        if queue_origem == "lance_realizado":
            key = RSA.import_key(open('keys/public_0.pem').read())
            h = SHA256.new(body.signature)
            try:
                pkcs1_15.new(key).verify(h, signature)
                print ("The signature is valid.")
            except (ValueError, TypeError):
                print ("The signature is not valid.")

        message = body
        message = base64.b64decode(message)
        message = message.decode('utf-8')
        data = json.loads(message)

        print(f" [x] Decoded {message}")

        if queue_origem == "leilao_iniciado":
            auctionList.append(data)
        elif queue_origem == "lance_realizado":
            auction = next((a for a in auctionList if a["auction_id"] == data["auction_id"]), None)

            if auction is None:
                print(f"[!] Leilão {data['auction_id']} não encontrado.")
                return

            if auction["status"] != "ativo":
                print(f"[!] Leilão {data['auction_id']} já está finalizado.")
                return

            # aqui você trata o lance normalmente
            if data["auction_id"] not in lastBid or data["bid_value"] > lastBid[data["auction_id"]]["bid_value"]:
                channel.basic_publish(exchange='', routing_key="lance_validado", body=to_json(data))
                lastBid[data["auction_id"]] = data
                print(f"[+] Lance aceito para leilão {auction['auction_id']}: {data}")

            

    channel.basic_consume(queue='lance_realizado', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='leilao_iniciado', on_message_callback=callback, auto_ack=True)
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