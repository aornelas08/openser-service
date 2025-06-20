import os
import logging
import time
import unicodedata
import json
import pandas as pd
import re
import requests
import psycopg2
from flask import Flask, jsonify, request
from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required, JWTManager
from flask_apscheduler import APScheduler
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

load_dotenv()
# Variables
OPENSER_USERNAME = os.getenv("OPENSER_USERNAME")
OPENSER_PASSWORD = os.getenv("OPENSER_PASSWORD")
OPENSER_URL_HOST = os.getenv("OPENSER_URL_HOST")
OPENSER_COMPANY = os.getenv("OPENSER_COMPANY")
OPENSER_PAYLOAD_REPORT = os.getenv("OPENSER_PAYLOAD_REPORT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
PERIOD_OF_DAYS = os.getenv("PERIOD_OF_DAYS")
PATH_LOGGER = os.getenv("PATH_LOGGER")
LOG_DIRECTORY_WINDOWS = os.getenv("LOG_DIRECTORY_WINDOWS")
LOG_FILE_WINDOWS = os.getenv("LOG_FILE_WINDOWS")
LOG_PATH = os.path.join(LOG_DIRECTORY_WINDOWS,LOG_FILE_WINDOWS)
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

DEFAULT_HEADERS = {
    "empresa": OPENSER_COMPANY,
    "Content-Type": "application/json",
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'User-Agent': 'PostmanRuntime/7.40,0',
}

CONNECTION_PARAMS = {
    "host": DB_HOST,
    "port": DB_PORT,
    "user": DB_USERNAME,
    "password": DB_PASSWORD,
    "dbname": DB_DATABASE
}

#conn_string = f"host='localhost' dbname={DB_DATABASE} user={OPENSER_USERNAME} password={OPENSER_PASSWORD}"
psycopg2_url = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@localhost:{DB_PORT}/{DB_DATABASE}"

# Init main vars
app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = "KovalOpenser2025#ITServices"
jwt = JWTManager(app)
scheduler = APScheduler()

# =============== LOGGER =============== #
# Configuración básica para los mensajes de bitácora """
logging.basicConfig(
    filename=  LOG_PATH,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8',
    level=logging.INFO
)
def log_message(level, message):
    """
    Función de tipos de mensajes de log
    :param level: Tipo de log
    :param message: Mensaje del log
    :return: None
    """
    logger = logging.getLogger()
    if level == logging.DEBUG:
        logger.debug(message)
    elif level == logging.INFO:
        logger.info(message)
    elif level == logging.WARNING:
        logger.warning(message)
    elif level == logging.ERROR:
        logger.error(message)
    elif level == logging.CRITICAL:
        logger.critical(message)
    else:
        logger.info(message)
# =============== END LOGGER =============== #

# =============== UTILS =============== #
def remove_accents(str_word):
    """
    Remover tildes de una palabra
    :param str_word: palabra con tilde(s)
    :return txt_without_accents: El string con la palabra sin acentos
    """
    try:
        txt_without_accents = unicodedata.normalize('NFD', str_word)
        txt_without_accents = ''.join(c for c in txt_without_accents if unicodedata.category(c) != 'Mn')
        return txt_without_accents
    except Exception as e:
        print(f"Error {e}")

def create_dataframe(internal_data):
    """
    Crea un dataframe con los datos obtenidos
    :param internal_data: El diccionario de datos a agregar al dataframe
    :return df: El dataframe con los datos
    """
    try:
        decoded_data = json.loads(internal_data['Lista'])
        if isinstance(decoded_data, list):
            df = pd.DataFrame(decoded_data)
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            log_message(logging.INFO, f"DataFrame creado con {len(df)} registros.")
            return df
        else:
            log_message(logging.ERROR, "Los datos decodificados no son una lista.")
            return None
    except Exception as e:
        log_message(logging.ERROR, f"Error en create_dataframe: {e}")
        return None

def rename_columns(df):
    """
    Función encargada de limpiar columnas de un data frame
        - Remplazaar espacios por acentos
        - Convertir column en minusculas
    :param df: El data frame para limpiar sus columnas
    :return: regresa el df limpio y listo para insertar
    """
    try:
        df.columns = [
            remove_accents(re.sub(r'\W+', '_', col.lower().replace(' ', '_')).strip('_'))
            for col in df.columns
        ]
        log_message(logging.INFO, "Columnas renombradas exitosamente.")
    except Exception as e:
        log_message(logging.ERROR, f"Error en rename_columns: {e}")
    return df

def compare_data_frames(df_api, df_db):
    """
    Funcion que nos permite sacar los registros nuevos y los que han sufrido actualización
    :param df_api: Data frame con la información de la API
    :param df_db: Data frame con la información de la DB
    :return: retorna los Data frame de los nuevos y actualizados
    """
    try:
        df_news = df_api[~df_api['id'].isin(df_db['id'])]
        df_merged = df_api.merge(df_db, on='id', suffixes=('_api', '_db'))
        df_changes = df_merged[df_merged['estatus_api'] != df_merged['estatus_db']]
        # Remover sufijo api e ignorar los que tienen db
        df_changes = df_changes[[col for col in df_changes.columns if not col.endswith('_db') and col != 'max_fecha']]
        df_changes.columns = df_changes.columns.str.replace('_api', '')
        return df_news, df_changes
    except Exception as e:
        log_message(logging.ERROR, f"Error en merge compare_data_frames: {e}")

def get_payload_from_file(filename):
    """
    Obtiene el payload del archivo payload.json
    :param filename: Ruta del archivo a consumir
    :return: payload
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, filename)
    try:
        with open(file_path, 'r') as file:
            payload = json.load(file)
        return payload
    except Exception as e:
        print(f"Error loading payload from file: {e}")
        return None

def update_payload_dates(file_path):
    """
    Actualiza los campos 'FechaInicial' y 'FechaFinal' en el archivo payload.json a la fecha actual y la fecha menos
    7 días en un formato de timestamp de Unix
    :param file_path: La ruta para el archivo payload.json
    """
    try:
        # Calcular la fecha inicial y la fecha menos 7 días
        current_date = datetime.now()
        date_x_days_ago = current_date - timedelta(days=int(PERIOD_OF_DAYS))

        # Convertirlas a tiemstamp de Unix
        current_date_unix = int(current_date.timestamp() * 1000)
        date_x_days_ago_unix = int(date_x_days_ago.timestamp() * 1000)
        payload = get_payload_from_file(file_path)

        # Actualiza los campos 'FechaInicial' and 'FechaFinal'
        payload['FechaInicial'] = f"/Date({date_x_days_ago_unix}-0500)/"
        payload['FechaFinal'] = f"/Date({current_date_unix}-0500)/"

        # Salvamos el archivo
        with open(file_path, 'w') as file:
            json.dump(payload, file, indent=4)
            log_message(logging.INFO, "Fechas del payload actualizadas exitosamente.")
    except Exception as e:
        log_message(logging.ERROR, f"Error en update_payload_dates: {e}")
        with open(PATH_LOGGER, 'a') as error_log:
            error_log.write(f"{datetime.now()}: Error en update_payload_dates: {e}\n")
# =============== END UTILS =============== #

# =============== OPENSER OPERATIONS =============== #
def get_web_token(url, username, password, headers):
    """
    Envía una petición POST a la url especificada con las credenciales para obtener un token
    :param url: El endpoint a utilizar
    :param username: Nombre de usuario
    :param password: Contrasena del usuario
    :param headers: Diccionario con las credenciales usadas para obtener un token
    :return: El webtoken a utilizar en string
    """
    try:
        payload = {
            "Password": password,
            "Tipo": 1,
            "Username": username
        }
        response = requests.post(url, headers=DEFAULT_HEADERS, json=payload)
        response_data = response.json()
        web_token = response_data.get('WebToken')
        log_message(logging.INFO, "Token web obtenido exitosamente.")
        return web_token
    except Exception as e:
        log_message(logging.ERROR, f"Error en get_webtoken: {e}")
        return None

def execute_report(url, web_token):
    """
        Envía una petición POST a la url especificada con las credenciales para obtener un token
        :param url: El endpoint a utilizar
        :param webtoken: Token para uso de los API's
        :return: Datos de reporte
    """
    try:
        headers = DEFAULT_HEADERS.copy()
        headers.update({"webtoken": web_token})

        # Cargamos nuestro template desde el archivo de JSON
        payload = get_payload_from_file(OPENSER_PAYLOAD_REPORT)

        if payload is None:
            raise ValueError("No se pudo cargar el payload desde el archivo payload.json")

        logging.basicConfig(level=logging.DEBUG)
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            response_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        log_message(logging.INFO, "Reporte ejecutado exitosamente.")
        return response_data
    except Exception as e:
        log_message(logging.ERROR, f"Error en ejecutar_reporte: {e}")
        return None
# =============== END OPENSER OPERATIONS =============== #


# =============== DATABASE OPERATIONS =============== #
def get_column_type(column):
    """
    Regresa el tipo de dato en SQL para una columna en pandas
    """
    column_type = str(column.dtype)
    if 'int' in column_type:
        return 'INTEGER'
    elif 'float' in column_type:
        return 'FLOAT'
    elif 'datetime' in column_type:
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def ensure_table_and_columns_exist(df, table_name):
    """
    Nos aseguramos que la tabla y las columnas existen en la base de datos PostgreSQL.
    :param df: El dataframe usado para crear las columnas
    :param table_name: El nombre de la tabla que se quiere crear
    """
    try:
        print(psycopg2_url)
        #conn = psycopg2.connect(**CONNECTION_PARAMS)
        conn = psycopg2.connect(psycopg2_url)
        cursor = conn.cursor()

        # Revisa si la tabla existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            );
        """, (table_name,))
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logging.info(f"La tabla {table_name} no existe. Creando tabla...")
            columns = [
                f'"{col}" {get_column_type(df[col])}'
                for col in df.columns
            ]
            create_table_query = f"""
            CREATE TABLE "{table_name}" (
                {', '.join(columns)}
            );
            """
            cursor.execute(create_table_query)
            conn.commit()
            logging.info(f"La tabla {table_name} fue creada exitosamente.")

        # Verifica columnas existentes
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s;
        """, (table_name,))
        existing_columns = [row[0] for row in cursor.fetchall()]

        for column in df.columns:
            if column not in existing_columns:
                sql_column_type = get_column_type(df[column])
                add_column_query = f"""
                ALTER TABLE "{table_name}"
                ADD COLUMN "{column}" {sql_column_type};
                """
                logging.info(f"Agregando columna {column} a la tabla {table_name}")
                cursor.execute(add_column_query)
                conn.commit()

        logging.info(f"Todas las columnas necesarias están presentes en {table_name}.")
    except Exception as e:
        logging.error(f"Error en ensure_table_and_columns_exist: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_tickets_x_days_ago(connection, period_of_days):
    """
    Esta función obtiene todos los tickets desde la fecha máxima registrada hasta X días atrás.
    :param connection: La conexión a la base de datos (SQLAlchemy)
    :param period_of_days: Número de días hacia atrás desde la fecha máxima
    :return: DataFrame con los tickets filtrados
    """

    query_tickets = f"""
    SELECT * FROM ticket 
    WHERE fecha_corta_registro::date >= (
        SELECT MAX(fecha_corta_registro)::date - INTERVAL '{period_of_days} days'
        FROM ticket
    );
    """
    try:
        tickets_result = connection.execute(text(query_tickets))
        tickets = tickets_result.fetchall()
        df_db = pd.DataFrame(tickets, columns=tickets_result.keys())
        logging.info(f"Se han extraído {len(tickets)} registros por intervalo de {period_of_days} días atrás")
        df_db = rename_columns(df_db)
        return df_db
    except Exception as e:
        logging.error(f"Error al extraer registros por intervalo de {period_of_days} días atrás: {e}")
        return pd.DataFrame()

def insert_tickets(df_news, engine, table_name):
    """
    Esta función inserta los registros provenientes de un DataFrame en una tabla PostgreSQL.
    :param df_news: DataFrame con los registros para insertar
    :param engine: Motor SQLAlchemy conectado a PostgreSQL
    :param table_name: Nombre de la tabla donde se insertarán los datos
    """
    try:
        logging.info(f"Insertando {len(df_news)} nuevos registros en {table_name}")
        df_news.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')
        logging.info("Datos insertados exitosamente.")
    except Exception as e:
        logging.error(f"Error en insert_tickets: {e}")


def update_tickets(connection, df_changes):
    """
    Esta función actualiza los registros provenientes de un DataFrame en la tabla 'ticket'.
    :param connection: Conexión SQLAlchemy a la base de datos PostgreSQL
    :param df_changes: DataFrame con los registros para actualizar (debe incluir la columna 'id')
    """
    try:
        for index, row in df_changes.iterrows():
            data_to_update = row.to_dict()
            ticket_id = data_to_update.pop('id', None)

            if ticket_id is None:
                logging.warning(f"Fila sin ID, se omite: {row}")
                continue

            # Convertir NaN a None
            for key, value in data_to_update.items():
                if pd.isna(value):
                    data_to_update[key] = None

            # Preparar la consulta SQL
            set_clause = ", ".join([f"{key} = :{key}" for key in data_to_update.keys()])
            values = {**data_to_update, 'id': ticket_id}

            update_query = text(f"""
                UPDATE ticket
                SET {set_clause}
                WHERE id = :id
            """)

            result = connection.execute(update_query, values)
            connection.commit()

            if result.rowcount > 0:
                logging.info(f"Ticket actualizado: {ticket_id}")
            else:
                logging.info(f"Ticket no encontrado: {ticket_id}")

        logging.info(f"Se actualizaron {len(df_changes)} ticket(s) en la base de datos.")
    except Exception as e:
        logging.error(f"Error en update_tickets: {e}")
# =============== END DATABASE OPERATIONS =============== #


# =============== ORCHESTRATOR OPERATIONS =============== #
def orchestrator_operation(df_api, _table_name):
    """
    Inserta el DataFrame a la tabla en la base de datos PostgreSQL.
    Crea la tabla y las columnas si no existen.

    :param df_api: El DataFrame con los datos provenientes de la API.
    :param _table_name: El nombre de la tabla destino.
    :param connection_params: Diccionario con los parámetros de conexión para PostgreSQL.
    :param period_of_days: Número de días hacia atrás para comparar registros existentes.
    """
    try:
        # Crear motor SQLAlchemy para PostgreSQL
        engine = create_engine(
            f"postgresql+psycopg2://{CONNECTION_PARAMS['user']}:{CONNECTION_PARAMS['password']}@"
            f"{CONNECTION_PARAMS['host']}:{CONNECTION_PARAMS['port']}/{CONNECTION_PARAMS['dbname']}"
        )

        # Asegurar que la tabla y columnas existen
        ensure_table_and_columns_exist(df_api, _table_name)

        with engine.connect() as connection:
            # Obtener registros existentes en el rango de días
            df_db = get_tickets_x_days_ago(connection, PERIOD_OF_DAYS)

            # Comparar DataFrames para detectar nuevos registros y cambios
            df_news, df_changes = compare_data_frames(df_api, df_db)

            # Insertar nuevos registros
            if not df_news.empty:
                insert_tickets(df_news, engine, _table_name)

            # Actualizar registros existentes
            if not df_changes.empty:
                update_tickets(connection, df_changes)

    except Exception as e:
        logging.error(f"Error en orchestrator: {e}")
# =============== END ORCHESTRATOR OPERATIONS =============== #

def run_openser_api():
    # ========== # GET WEB TOKEN PROCESS # ========== #
    log_message(logging.INFO, "Comienza la función get_webtoken")
    start_time = time.time()
    webToken = get_web_token(url=OPENSER_URL_HOST + "Usuario.svc/json/Autenticar", username=OPENSER_USERNAME,
                             password=OPENSER_PASSWORD, headers=DEFAULT_HEADERS)
    end_time = time.time()
    log_message(logging.INFO, f"Total de la ejecución de get_webtoken: {end_time - start_time}")

    # ========== # UPDATE PAYLOAD DATES PROCESS # ========== #
    update_payload_dates(OPENSER_PAYLOAD_REPORT)

    # ========== # EXECUTE REPORT PROCESS # ========== #
    log_message(logging.INFO, "Comienza la función ejecutar_reporte")
    start_time = time.time()
    data = execute_report(OPENSER_URL_HOST + "Reporte.svc/json/ConsultarTodo", webToken)  # JSON data
    end_time = time.time()
    log_message(logging.INFO, f"Total de la ejecución de ejecutar_reporte: {end_time - start_time}")

    # ========== # CREATE DATA FRAME PROCESS # ========== #
    log_message(logging.INFO, "Comienza la funcion create_dataframe")
    start_time = time.time()
    data_frame = None
    if data is not None:
        data_frame = create_dataframe(data)
        data_frame = rename_columns(data_frame)
        data_frame = data_frame.sort_values(by=['id'], ascending=True)
        print(data_frame)
    else:
        log_message(logging.INFO, "No hay datos")
    end_time = time.time()
    log_message(logging.INFO, f"Total de la ejecución de create_dataframe: {end_time - start_time}")

    # # ========== # INSERT DATA IN DATABASE # ========== #
    table_name = "ticket"
    log_message(logging.INFO, "Comienza la funcion orchestrator")
    if data_frame is not None:
        start_time = time.time()
        orchestrator_operation(data_frame, table_name)
    end_time = time.time()
    log_message(logging.INFO, f"Total de la ejecución de orchestrator: {end_time - start_time}")




@app.route("/ondemand", methods=["GET"])
def execute_report_on_demand():
    try:
        run_openser_api()
        logging.info("Reporte Ejecutado")
        return jsonify({"message": "Reporte Ejecutado"}), 200
    except Exception as e:
        logging.error(f"Error en execute_report_on_demand: {e}")
        return jsonify({"error": f"Algo salio mal: {e}"}), 400


@app.route("/login", methods=["POST"])
def login():
    username = request.json.get("username", None)
    password = request.json.get("password", None)
    if username != USERNAME or password != PASSWORD:
        return jsonify({"msg": "Bad username or password"}), 401

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token)

@app.route("/protected", methods=["GET"])
@jwt_required()
def protected():
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user), 200

if __name__ == "__main__":
    scheduler.add_job(func=run_openser_api, trigger='interval', minutes=60, id="run_openser_api")
    scheduler.start()
    app.run(host='0.0.0.0', port=5000)
