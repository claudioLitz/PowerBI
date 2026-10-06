import json
import ssl
import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# --- Credenciais e Configurações do HiveMQ Cloud (MQTT) ---
MQTT_BROKER = "696bec4f9bcd435292f5288a90447793.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "Claudio"
MQTT_PASS = "123456789"
MQTT_TOPIC = "senai/claudio/esg/utilidades"

# --- Credenciais e Configurações do Aiven Kafka ---
KAFKA_BOOTSTRAP_SERVER = "kafka-estudante-79bf.a.aivencloud.com:15317"
KAFKA_USER = "avnadmin"
KAFKA_PASSWORD = "AVNS_ugCns2xh0SNGd6qeI3T"
KAFKA_TOPIC = "esg-utilidades-telemetria"
CA_CERT_PATH = "ca.pem"  # Arquivo baixado do painel Aiven

# --- Configuração do Produtor Kafka ---
print("Conectando ao Aiven Kafka...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASSWORD,
    ssl_cafile=CA_CERT_PATH,  # Validação do certificado Aiven
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("Conectado ao Kafka com sucesso!")

# --- Callbacks do MQTT ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conectado ao HiveMQ Cloud com sucesso! Escutando tópico: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Falha ao conectar no HiveMQ Cloud, código de erro: {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        dados = json.loads(payload_str)
        print(f"\n[MQTT ➔ KAFKA] Mensagem recebida: {dados}")
        
        # Envia para o Aiven Kafka
        producer.send(KAFKA_TOPIC, value=dados)
        producer.flush()
        print(f"Enviado para o tópico Kafka: {KAFKA_TOPIC}")
    except Exception as e:
        print(f"Erro ao processar mensagem MQTT: {e}")

# --- Configuração do Cliente MQTT ---
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)

# Habilita suporte a TLS/SSL obrigatório no HiveMQ Cloud
mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

print("Conectando ao HiveMQ Cloud...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_forever()