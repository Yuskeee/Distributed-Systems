#!/usr/bin/env python
import pika
import datetime
import json
import base64

def json_serial(obj):
    """Serializador JSON para objetos não serializáveis por padrão."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat() # Converte data/hora para string no formato ISO 8601
    raise TypeError(f"O tipo {type(obj)} não é serializável em JSON")

def to_json(data):
        #Tratamento do objeto leilão para envio via RabbitMQ
    json_string = json.dumps(data, default=json_serial)
    json_bytes = json_string.encode('utf-8')
    base64_bytes = base64.b64encode(json_bytes)
    return base64_bytes

# Criando a estrutura para um leilão específico
leilao_01 = {
    "id_leilao": 101,
    "descricao": "Notebook Gamer de última geração",
    "data_inicio": datetime.datetime(2025, 7, 28, 15), # Ano, Mês, Dia, Hora
    "data_fim": datetime.datetime(2025, 7, 29, 18),
    "status": "ativo",
}

def main():
    #Trecho de código para criar as filas de responsabilidade do Leilão
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='leilao_iniciado')
    channel.queue_declare(queue='leilao_finalizado')

   #TODO: while true
    if (datetime.datetime.now()) >= leilao_01["data_inicio"] and (datetime.datetime.now()) <= leilao_01["data_fim"]:
        channel.basic_publish(exchange='', routing_key='leilao_iniciado', body=to_json(leilao_01))
        print(" [x] Sent ")
        connection.close()
    elif (datetime.datetime.now()) > leilao_01["data_fim"]:
        leilao_01["status"] = "finalizado"
        channel.basic_publish(exchange='', routing_key='leilao_finalizado', body=to_json(leilao_01))
        print(" [x] Sent ")
        connection.close()


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)