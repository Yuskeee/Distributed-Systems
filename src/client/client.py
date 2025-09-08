import uuid
import threading
import json

from common.rabbitmq import RabbitMQ
from common.models import Bid, Message
from common import config
from common.crypto_utils import generate_keys, sign, get_public_key_pem

class AuctionClient:
    def __init__(self):
        self.user_id = str(uuid.uuid4())
        self.private_key, self.public_key = generate_keys()
        self.known_auctions = {}

        # Salva a chave pública em um arquivo para o MSBid usar
        with open(f"keys/{self.user_id}_public_key.pem", "wb") as f:
            f.write(get_public_key_pem(self.public_key))

    def listen_for_auctions(self):
        """Listen to 'leilao_iniciado' and 'leilao_finalizado' events in a dedicated thread."""
        def _listen():
            rabbitmq = RabbitMQ()  # NEW instance for this thread
            rabbitmq.declare_exchange(config.EXCHANGE_NAME, ex_type="direct")
            queue_name = rabbitmq.declare_queue("", exclusive=True)

            rabbitmq.bind_queue(
                queue=queue_name,
                exchange=config.EXCHANGE_NAME,
                routing_key="leilao_iniciado"
            )
            rabbitmq.bind_queue(
                queue=queue_name,
                exchange=config.EXCHANGE_NAME,
                routing_key="leilao_finalizado"
            )

            rabbitmq.consume(queue=queue_name, callback=self.on_auction_event)
        t = threading.Thread(target=_listen, daemon=True)
        t.start()

    def on_auction_event(self, ch, method, properties, body):
        """Callback for auction started or finished events."""
        try:
            data_dict = json.loads(body.decode())
        except Exception:
            print("\n[Erro] Falha ao decodificar evento do leilão.")
            return
        event_type = method.routing_key
        payload = data_dict.get('payload', {})
        if event_type == "leilao_iniciado":
            self.known_auctions[payload['id']] = payload
            print("\n--- Novo Leilão Iniciado ---")
            print(f"ID: {payload['id']}, Descrição: {payload['description']}")
            print(f"Início: {payload['start_time']}, Fim: {payload['end_time']}")
            print("---------------------------\n")
        elif event_type == "leilao_finalizado":
            auction_id = payload.get('id')
            if auction_id and auction_id in self.known_auctions:
                del self.known_auctions[auction_id]
                print("\n--- Leilão Finalizado ---")
                print(f"ID: {auction_id}, Status: Encerrado (removido da lista de leilões ativos)")
                print("---------------------------\n")
            else:
                print("\n--- Leilão Finalizado ---")
                print(f"ID: {auction_id if auction_id else '(N/A)'}, Status: Encerrado")
                print("Leilão não constava na lista ativa.")
                print("---------------------------\n")

    def on_notification(self, ch, method, properties, body):
        """Callback for auction-specific notifications."""
        try:
            data_dict = json.loads(body.decode())
        except Exception:
            print("\n[Erro] Falha ao decodificar notificação do leilão.")
            return
        event_type = data_dict.get('event_type')
        payload = data_dict.get('payload', {})
        print("\n--- Notificação Recebida ---")
        if event_type == "lance_validado":
            print(f"Novo lance no leilão {payload['auction_id']}!")
            print(f"Usuário: {payload['user_id'][:8]}..., Valor: {payload['amount']:.2f}")
        elif event_type == "leilao_vencedor":
            if payload['winner_id'] == self.user_id:
                print(f"PARABÉNS! Você venceu o leilão {payload['auction_id']} com o lance de {payload['value']:.2f}!")
            else:
                print(f"O leilão {payload['auction_id']} foi encerrado.")
                print(f"Vencedor: {payload['winner_id'][:8]}..., Valor: {payload['value']:.2f}")
        print("----------------------------\n")

    def place_bid(self, auction_id, amount):
        """Places a bid in an auction."""
        if auction_id not in self.known_auctions:
            print("Erro: Leilão desconhecido.")
            return
        message_to_sign = f"{auction_id}{self.user_id}{amount:.2f}"
        signature = sign(self.private_key, message_to_sign)
        bid = Bid(
            auction_id=auction_id,
            user_id=self.user_id,
            amount=amount,
            signature=signature
        )
        message = Message(event_type="lance_realizado", payload=bid.to_dict())
        RabbitMQ().publish(
            exchange=config.EXCHANGE_NAME,
            routing_key="lance_realizado",
            body=message.to_dict()
        )
        print(f"Lance de {amount:.2f} enviado para o leilão {auction_id}.")

        auction_queue = f"leilao_{auction_id}_{self.user_id}"
    
        def _listen_notifications():
            rabbitmq = RabbitMQ()
            rabbitmq.declare_queue(auction_queue, exclusive=True, durable=False)
            rabbitmq.bind_queue(
                queue=auction_queue,
                exchange=config.EXCHANGE_NAME,
                routing_key=f"leilao_{auction_id}"
            )
            rabbitmq.consume(queue=auction_queue, callback=self.on_notification)
        t = threading.Thread(target=_listen_notifications, daemon=True)
        t.start()

def main():
    client = AuctionClient()
    client.listen_for_auctions()
    print(f"Cliente iniciado. Seu ID de usuário é: {client.user_id}")
    print("Aguardando o início de leilões...")
    while True:
        try:
            print("\nOpções:")
            print("1. Listar leilões ativos")
            print("2. Fazer um lance")
            print("3. Sair")
            choice = input("Escolha uma opção: ")
            if choice == '1':
                if not client.known_auctions:
                    print("Nenhum leilão conhecido no momento.")
                else:
                    print("\n--- Leilões Conhecidos ---")
                    for aid, adata in client.known_auctions.items():
                        print(f"ID: {aid}, Descrição: {adata['description']}, Fim: {adata['end_time']}")
                    print("------------------------")
            elif choice == '2':
                try:
                    auction_id = int(input("Digite o ID do leilão: "))
                    amount = float(input("Digite o valor do lance: "))
                    client.place_bid(auction_id, amount)
                except ValueError:
                    print("Entrada inválida. Por favor, digite números.")
            elif choice == '3':
                break
            else:
                print("Opção inválida.")
        except KeyboardInterrupt:
            break
    print("Cliente encerrado.")

if __name__ == '__main__':
    main()
