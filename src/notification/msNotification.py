import threading
import json
import time

from common.rabbitmq import RabbitMQ
from common import config

class MSNotification:
    def __init__(self):
        self.exchange = config.EXCHANGE_NAME
        self.rabbitmq = RabbitMQ()

    def listen(self):
        def _listen():
            self.rabbitmq.declare_exchange(self.exchange, ex_type="direct")
            queue_name = self.rabbitmq.declare_queue("", exclusive=True)

            self.rabbitmq.bind_queue(queue=queue_name, exchange=self.exchange, routing_key="lance_validado")
            self.rabbitmq.bind_queue(queue=queue_name, exchange=self.exchange, routing_key="leilao_vencedor")
            self.rabbitmq.consume(queue=queue_name, callback=self.handle_event)
            
        t = threading.Thread(target=_listen, daemon=True)
        t.start()

    def handle_event(self, ch, method, properties, body):
        try:
            data = json.loads(body.decode())
            event_type = method.routing_key
            payload = data.get("payload", {})
            if event_type == "lance_validado":
                self.notify_auction(payload, "lance_validado")
            elif event_type == "leilao_vencedor":
                self.notify_auction(payload, "leilao_vencedor")
        except Exception as e:
            print(f"[MSNotification] Error processing event: {e}")

    def notify_auction(self, payload, event_type):
        try:
            auction_id = payload.get('auction_id')
            if not auction_id:
                print("[MSNotification] No 'auction_id' in payload, skipping notification.")
                return
            queue_name = f"leilao_{auction_id}"

            message = {
                "event_type": event_type,
                "payload": payload
            }

            self.rabbitmq = RabbitMQ()
            self.rabbitmq.publish(
                exchange=self.exchange,
                routing_key=queue_name,
                body=message
            )

            print(f"[MSNotification] Forwarded event '{event_type}' to queue '{queue_name}'.")

        except Exception as e:
            print(f"[MSNotification] Error forwarding notification: {e}")

if __name__ == "__main__":
    notification_service = MSNotification()
    notification_service.listen()
    print("[MSNotification] Service started. Listening for bid/auction notifications.\nPress Ctrl+C to exit.")
    while True:
        time.sleep(1)
