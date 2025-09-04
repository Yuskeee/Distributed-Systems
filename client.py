import pika, sys, os, base64, json, threading
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

class Bid:
    def __init__(self, auction_id, user_id, bid_value):
        self.auction_id = auction_id
        self.user_id = user_id
        self.bid_value = bid_value

    def __to_dictionary(self):
        return {
            "auction_id": self.auction_id,
            "user_id": self.user_id,
            "bid_value": self.bid_value,
        }
    
    def send_bid(self, channel):
        bid_data = self.__to_dictionary()
        message_to_sign = json.dumps(bid_data, sort_keys=True).encode('utf-8')
        try:
            key = RSA.import_key(open('keys/private_0.pem').read())
            h = SHA256.new(message_to_sign)
            signature = pkcs1_15.new(key).sign(h)
            
            signature_b64 = base64.b64encode(signature).decode('utf-8')

            final_message_payload = {
                "bid": bid_data,
                "signature": signature_b64
            }
            
            message_body = json.dumps(final_message_payload)

            channel.basic_publish(
                exchange='',
                routing_key='lance_realizado',
                body=message_body
            )
            print(f" [x] Sent Bid: {message_body}")

        except FileNotFoundError:
            print(" [!] Error: Private key file 'keys/private_0.pem' not found.")
        except Exception as e:
            print(f" [!] An error occurred during message signing or sending: {e}")

def main():
    message = ""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='leilao_iniciado')
    channel.queue_declare(queue='leilao_finalizado')
    channel.queue_declare(queue='lance_realizado')

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
        message = body
        message = base64.b64decode(message)
        message = message.decode('utf-8')
        data = json.loads(message)

        print(f" [x] Decoded {data}")

        if data.get("status") == "active":
            print(" [+] Auction is active. Placing a bid...")
            bid_01 = Bid(data["auction_id"], "user_1", 100.00)
            bid_01.send_bid(ch)
        else:
            print(f" [.] Auction status is '{data.get('status')}'. No action taken.")

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