import json
import sqlite3
from datetime import datetime
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVER = "kafka-estudante-79bf.a.aivencloud.com:15317"
KAFKA_USER = "avnadmin"
KAFKA_PASSWORD = "AVNS_ugCns2xh0SNGd6qeI3T"
KAFKA_TOPIC = "esg-utilidades-telemetria"
CA_CERT_PATH = "ca.pem"
DB_NAME = 'telemetria_esg.db'

def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS telemetria_esg_limpa (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_processamento TEXT,
            sensor_id TEXT,
            linha_producao TEXT,
            kwh_eletricidade REAL,
            m3_agua REAL,
            m3_ar_comprimido REAL,
            unidades_produzidas INTEGER,
            status_maquina TEXT,
            consumo_especifico_kwh_por_unid REAL,
            emissao_co2_kg REAL,
            alerta_consumo_fantasma BOOLEAN,
            is_horario_pico BOOLEAN
        )
    ''')
    conn.commit()
    conn.close()

setup_database()
print("Banco de Dados 'telemetria_esg.db' inicializado!")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASSWORD,
    ssl_cafile=CA_CERT_PATH,
    api_version=(2, 5, 0),
    group_id='esg-etl-group-v5',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Aguardando leituras telemétricas do Kafka '{KAFKA_TOPIC}'...")

for message in consumer:
    raw_data = message.value
    print(f"[KAFKA LEITURA] {raw_data}")

    try:
        kwh = max(0.0, float(raw_data.get('kwh_eletricidade', 0)))
        ar = max(0.0, float(raw_data.get('m3_ar_comprimido', 0)))
        unidades = max(0, int(raw_data.get('unidades_produzidas', 0)))
        is_pico = bool(raw_data.get('is_horario_pico', False))
        sensor_id = raw_data.get('sensor_id', 'GALPAO_01_PONTA_ROUTE')
        linha = raw_data.get('linha_producao', 'LINHA_PRINCIPAL')
        status = raw_data.get('status_maquina', 'operando' if kwh > 5.0 else 'standby')

        emissao_co2 = kwh * 0.085
        consumo_especifico = (kwh / unidades) if unidades > 0 else 0.0
        
        # Regra do Alerta de Desperdício/Anomalia:
        # Corrente de Fuga (> 95 kWh) OR Vazamento de Ar (> 45 m³) OR Consumo sem produção
        alerta_desperdicio = (kwh > 95.0) or (ar > 45.0) or (unidades == 0 and (kwh > 5.0 or ar > 15.0))

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO telemetria_esg_limpa (
                timestamp_processamento, sensor_id, linha_producao,
                kwh_eletricidade, m3_agua, m3_ar_comprimido,
                unidades_produzidas, status_maquina,
                consumo_especifico_kwh_por_unid, emissao_co2_kg,
                alerta_consumo_fantasma, is_horario_pico
            ) VALUES (?, ?, ?, ?, 1.0, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            now_str, sensor_id, linha, kwh, ar,
            unidades, status, consumo_especifico, emissao_co2,
            alerta_desperdicio, is_pico
        ))
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Erro ao salvar leitura: {e}")