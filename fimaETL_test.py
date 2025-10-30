"""
Script para chequear la casilla data@outlier.com.ar en búsqueda de nuevos archivos recibidos de FIMA.
Este script debería ejecutarse, disparado por un cron (entre los horarios de las 19 y las 21, y las 0:30 y las 2:00, o hasta que un nuevo archivo sea recibido)
"""

import time
import os
import imaplib
import email
import re
from datetime import datetime
import pandas as pd
from DataBaseConn import DatabaseConnection
import uuid

# get mail server details from environment variables
MAIL_SERVER = os.getenv("MAIL_SERVER")
MAIL_PORT = os.getenv("MAIL_PORT")
MAIL_USER = os.getenv("MAIL_USER")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
ATTACH_DIR = ATTACH_DIR = "/Users/juan/Google Drive/Mi unidad/dev/temp/"  #os.path.expanduser("~")
FIMA_FROM_ADDRESS = os.getenv("FIMA_FROM_ADDRESS")
destination_folder = 'INBOX.fima_archivados'

def generate_uuid():
    return str(uuid.uuid4())


def check_mail():
    """
    Check the mailbox for new emails and download the attachments
    """
    print(f"Chequeando la casilla de correo {MAIL_USER} en el servidor {MAIL_SERVER} en el puerto {MAIL_PORT} a las {time.ctime()}")
    # connect to the mail server using SSL

    # Regular expression to match the date pattern in the subject
    #date_pattern = re.compile(r'(\d{2}-\d{2}-\d{4})')
    date_pattern = re.compile(r'\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4}')

    mail = imaplib.IMAP4_SSL(MAIL_SERVER, MAIL_PORT)
    mail.login(MAIL_USER, MAIL_PASSWORD)
    mail.select('inbox')

    #result, data = mail.search(None, 'UNSEEN')
    # get only the emails from FIMA and UNSEEN
    #result, data = mail.search(None, 'FROM', FIMA_FROM_ADDRESS)
    #result, data = mail.uid('SEARCH', None, f'(FROM "{FIMA_FROM_ADDRESS}" UNFLAGGED)')
    result, data = mail.search(None, f'(FROM "{FIMA_FROM_ADDRESS}")')
    mail_ids = data[0].split()


    if mail_ids:
        for num in mail_ids:

            _, data = mail.fetch(num, '(RFC822)')
            raw_email = data[0][1]
            email_message = email.message_from_bytes(raw_email)

            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                
                file_name = part.get_filename()
                if file_name and (file_name.endswith('.xls') or file_name.endswith('.xlsx')):
                    
                    # extract the date from the subject
                    print(email_message['Subject'])
                    date_match = date_pattern.search(email_message['Subject'])
                    if date_match:
                        # Extracted date string
                        date_str = date_match.group(0)
                        date_str = re.sub(r'[\/]', '-', date_str)
                    
                        # Convert the date string to a datetime object with hours, minutes and seconds to account for
                        from datetime import datetime

                        try:
                            # Try to parse with a 4-digit year
                            fechaCorrespondeParseada = datetime.strptime(date_str, '%d-%m-%Y')
                        except ValueError:
                            # If it fails, fall back to a 2-digit year
                            fechaCorrespondeParseada = datetime.strptime(date_str, '%d-%m-%y')
 
                        #fechaCorrespondeParseada = datetime.strptime(date_str, '%d-%m-%Y')

                    # assign a file_path adding 
                    email_datetime = email.utils.parsedate_to_datetime(email_message['date'])
                    email_datetime_formatted = email_datetime.strftime('%Y%m%d_%H-%M-%S')
                    print(f"Saving attachment {file_name} received on {email_datetime} to {ATTACH_DIR}")

                    file_name = f"{email_datetime_formatted}_{file_name}_{date_str}"
                    file_path = os.path.join(ATTACH_DIR, file_name)
                    with open(file_path, 'wb') as f:
                        f.write(part.get_payload(decode=True))
                        # pause 1 seconds to prevent having the same timestamp in the file name
                        time.sleep(1)
                    emails_df = pd.DataFrame([{
                        'fechaRecepcion': email_datetime,
                        'descripcion': email_message['Subject'],
                        'fechaCorrespondeParseada': fechaCorrespondeParseada,
                        'fileName': file_name
                    }])
                    emails_df['id'] = emails_df.apply(lambda x: generate_uuid(), axis=1)
                    print("Enviando a almacenar mail en la base de datos")
                    load_mail_to_db(emails_df)
                    print("Enviando a procesar el archivo")
                    process_attachment(emails_df)
                    # Move the email to the destination folder
                    print(f"Moving email {num} to {destination_folder}")
                    result = mail.copy(num, destination_folder)
                    if result[0] == 'OK':
                        print(f"Message {num} copied to {destination_folder} successfully.")
                        # delete the original email
                        mail.store(num, '+FLAGS', '\\Deleted')  
                    else:
                        print(f"Failed to copy message {num}. Server response: {result}. Message not deleted.")
                    break
                    
            #mail.store(num, '+FLAGS', '\\Seen')
    mail.close()
    mail.logout()



def load_mail_to_db(df):

    df.to_sql(name = 'archivosFIMA', con = db.engine, index = False, schema = 'public', if_exists='append')


def process_attachment(df):
    """
    Esta función debe tomar el archivo descargado y parsearlo para obtener la información
    Luego grabar ese df en la base de datos
    """

    # Leer el archivo
    diaria = pd.read_excel(os.path.join(ATTACH_DIR,df.iloc[0,3]), skiprows=4, usecols = "A:K")

    nombresColumna = [
        'tipoFondo',
        'fondo',
        'codBloomberg',
        'vcp',
        'varVcp',
        'varvcpMes',
        'tna',
        'patrimonio',
        'vcpProxHabil',
        'tnaProxHabil',
        'calificacion'
    ]
    diaria.columns = nombresColumna
    # Add a column with the date parsed in the calling function
    diaria['fechaCorrespondeParseada'] = df.iloc[0, 2]
    diaria['id'] = df.iloc[0, 4]
    # La fecha puede venir en K2 (más usual) pero también en H2 o H3 (la he visto en esos dos también). 
    # Esta forma captura las 3 celdas y luego asigna a date_value aquella que no es nula. Si no hay nada, pasará con null.
    date_value1 = pd.read_excel(os.path.join(ATTACH_DIR, df.iloc[0,3]), usecols="K", nrows=2, header=None).iloc[1,0]
    date_value2 = pd.read_excel(os.path.join(ATTACH_DIR, df.iloc[0,3]), usecols="H", nrows=2, header=None).iloc[1,0]
    date_value3 = pd.read_excel(os.path.join(ATTACH_DIR, df.iloc[0,3]), usecols="H", nrows=3, header=None).iloc[1,0]
    date_value = next((val for val in [date_value1, date_value2, date_value3] if pd.notna(val)), None)
    diaria['fechaPlanilla'] = date_value

    # Sanitize data types and remove header-like rows before loading to DB

    # Replace placeholder strings like '-' with NaN for numeric columns, then coerce
    numeric_cols = ['vcp', 'varVcp', 'varvcpMes', 'tna', 'patrimonio', 'tnaProxHabil']
    for col in numeric_cols:
        if col in diaria.columns:
            diaria[col] = diaria[col].replace('-', pd.NA)
            diaria[col] = pd.to_numeric(diaria[col], errors='coerce')

    # Drop rows that look like repeated headers (e.g. Fondo, Bloomberg, Valor Cuota)
    header_like_mask = (
        diaria['fondo'].astype(str).str.strip().str.lower().eq('fondo')
        | diaria['codBloomberg'].astype(str).str.strip().str.lower().eq('bloomberg')
        | diaria['tipoFondo'].astype(str).str.strip().str.lower().eq('tipo fondo')
        | (diaria['vcp'].isna() & diaria['varVcp'].isna() & diaria['patrimonio'].isna())
    )
    diaria = diaria[~header_like_mask]

    # Drop rows without a fund name
    diaria = diaria.dropna(subset=['fondo'])

    # Normalize string columns
    for col in ['tipoFondo', 'fondo', 'codBloomberg', 'calificacion', 'vcpProxHabil']:
        if col in diaria.columns:
            diaria[col] = diaria[col].astype(str).str.strip()
            diaria.loc[diaria[col].isin(['nan', 'None']), col] = pd.NA

    diaria.to_sql(name = 'diariaFIMA', con = db.engine, index = False, schema = 'public', if_exists='append')
    # la fecha máxima era 29-07-2025
    

if __name__ == "__main__":
    import os

    # comentamos estas líneas que estaban agregadas para debug.
    # Me daban todos los valores de las variables de entorno para poder chequear que estén bien seteadas
    # print("Environment variables:")
    # for key in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "MAIL_USER", "MAIL_PASSWORD", "MAIL_SERVER", "MAIL_PORT", "ATTACH_DIR", "FIMA_FROM_ADDRESS"]:
    #     print(f"{key}: {os.environ.get(key)}")
    
    print(f"Iniciando chequeo de mails en la casilla data@outlier.com.ar a las {time.ctime()}")

    db = DatabaseConnection(db_type="postgresql", db_name= os.environ.get('POSTGRES_DB'))
    db.connect()

    #check_mail()
    process_attachment(pd.DataFrame([{
        'fechaRecepcion': datetime.now(),
        'descripcion': 'FIMA - 01-01-2021',
        'fechaCorrespondeParseada': datetime(2021,1,1),
        'fileName': 'Cuota FIMA.xls',
        'id': generate_uuid()
    }]))


    db.disconnect()
