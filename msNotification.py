import pika, datetime, sys, os, base64, json

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

    channel.queue_declare(queue='lance_validado')
    channel.queue_declare(queue='leilao_vencedor')

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
        message = body
        message = base64.b64decode(message)
        message = message.decode('utf-8')
        data = json.loads(message)

        print(f" [x] Decoded {message}")

        queue_name = "leilao_" + str(data["auction_id"])
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_publish(exchange='', routing_key=queue_name, body=to_json(data))

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