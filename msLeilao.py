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


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='leilao_iniciado')
channel.queue_declare(queue='leilao_finalizado')

# Criando a estrutura para um leilão específico
leilao_01 = {
    "id_leilao": 101,
    "descricao": "Notebook Gamer de última geração",
    "data_inicio": datetime.datetime(2025, 8, 28, 14), # Ano, Mês, Dia, Hora
    "data_fim": datetime.datetime(2025, 10, 27, 18),
    "status": "ativo"
}

json_string = json.dumps(leilao_01, default=json_serial)
json_bytes = json_string.encode('utf-8')
base64_bytes = base64.b64encode(json_bytes)


channel.basic_publish(exchange='', routing_key='leilao_iniciado', body=base64_bytes)
print(" [x] Sent " + str(base64_bytes))
connection.close()