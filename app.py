import sqlite3
from flask import Flask, jsonify, render_template

app = Flask(__name__)
DB_NAME = 'telemetria_esg.db'


def get_db_connection():
  conn = sqlite3.connect(DB_NAME)
  conn.row_factory = sqlite3.Row
  return conn


@app.route('/')
def index():
  return render_template('index.html')


@app.route('/favicon.ico')
def favicon():
  return '', 204


@app.route('/api/dados')
def api_dados():
  try:
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. Busca os últimos 30 registros para a janela móvel dos gráficos
    cursor.execute("""
            SELECT 
                id,
                timestamp_processamento,
                sensor_id,
                linha_producao,
                kwh_eletricidade,
                m3_agua,
                m3_ar_comprimido,
                unidades_produzidas,
                status_maquina,
                consumo_especifico_kwh_por_unid,
                emissao_co2_kg,
                alerta_consumo_fantasma,
                is_horario_pico
            FROM telemetria_esg_limpa 
            ORDER BY id DESC 
            LIMIT 30
        """)
    rows = cursor.fetchall()

    # 2. Busca totais e médias de TODO o histórico do banco de dados
    cursor.execute("""
            SELECT 
                COUNT(*) as total_registros,
                SUM(kwh_eletricidade) as total_kwh,
                AVG(kwh_eletricidade) as media_kwh,
                SUM(emissao_co2_kg) as total_co2,
                AVG(emissao_co2_kg) as media_co2,
                SUM(unidades_produzidas) as total_unidades,
                AVG(unidades_produzidas) as media_unidades,
                AVG(consumo_especifico_kwh_por_unid) as media_consumo_especifico
            FROM telemetria_esg_limpa
        """)
    agregados = dict(cursor.fetchone())
    conn.close()

    dados = [dict(row) for row in rows]
    dados.reverse()  # Ordem cronológica

    return jsonify({
        'status': 'success',
        'data': dados,
        'historico_geral': agregados,
    })
  except Exception as e:
    return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
  app.run(debug=True, host='0.0.0.0', port=5000)