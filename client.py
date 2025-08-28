import pika, sys, os, base64

def main():
    message = ""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='leilao_iniciado')
    channel.queue_declare(queue='leilao_finalizado')

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
        message = body
        message = base64.b64decode(message)
        message = message.decode('utf-8')

        print(f" [x] Decoded {message}")

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