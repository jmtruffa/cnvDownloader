"""
Este script debería correr cada día para descargar los archivos excel que figuren como 
no descargados en la base de datos
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import tempfile
import os
import pandas as pd
#from DataBaseConn import DatabaseConnection
import sqlalchemy


db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_host = os.environ.get('POSTGRES_HOST')
db_port = os.environ.get('POSTGRES_PORT', '5432')  # Default port for PostgreSQL is 5432
db_name = os.environ.get('POSTGRES_DB')
dtypeMap = {'date': sqlalchemy.types.Date}



def download_file(df, db):
    """
    Descarga los archivos excel de los respectivos IDS desde la url
    y los guarda en una carpeta temporal
    """
    # Create a temporary directory to store the downloaded files
    tempDir = tempfile.mkdtemp()
    #tempDir = "/tmp/scrape"
    #print(f"Carpeta temporal: {tempDir}")

    options = Options()
    options.add_argument('--headless')  # Ensure headless mode is enabled
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    # Optional: For better stability in headless mode, you might want to add:
    options.add_argument('--disable-gpu')
    options.add_argument('start-maximized')
    options.add_argument('disable-infobars')
    options.add_argument('--disable-extensions')

    # Set up Chrome options
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": tempDir,  # Set download directory
        "download.prompt_for_download": False,  # Disable "Save As" dialog
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True  # Enable safe browsing
    }
    chrome_options.add_experimental_option("prefs", prefs)
    options.add_experimental_option('prefs', prefs)

    # Set up the Selenium WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # vamos a recorrer el df y visitar las urls en href para bajar los archivos
    for index, row in df.iterrows():

        print(f"Descargando archivo para el ID {row['ID']}, corresponde a fecha {row['fechaCorresponde']}. Row: {index} de {len(df)}")

        # Navigate to the URL
        url = row['href']
        driver.get(url)

        # Give time for the page to load if necessary
        time.sleep(10)

        # Locate the download link using class name and other attributes if needed
        download_link = driver.find_element(By.CSS_SELECTOR, 'a.downloadFile')
        download_link.click()

        # Wait for the download to complete
        time.sleep(20)

        # vamos a buscar el nombre del archivo descargado. Debería descargar uno solo
        downloadedFiles = set(os.listdir(tempDir))
        
        # verificamos que haya descargado un archivo. Si falló, continuamos con el siguiente
        if len(downloadedFiles) == 0 or len(downloadedFiles) > 1:
            print(f"No se descargó ningún archivo de la url {url} o bajaron más de uno. Abortando este archivo")
            continue
        
        downloadedFiles = os.path.join(tempDir, downloadedFiles.pop())

        print(f"Mandando archivo {downloadedFiles} a parsear")

        # se lo mandamos a la función parse_excel_file
        status = parse_excel_file(downloadedFiles, db, row['ID'])

        # si la función fue exitosa, cambiamos el valor de descargado a True
        #acá comenzo el comment
        # if status:
        #     # actualizamos el valor de descargado en la base de datos
        #     print(f"Actualizando el valor descargado en la base de datos para el ID {row['ID']}")
        #     query = f'UPDATE "archivosCAFCI" SET descargado = True, procesado_ok = True WHERE "ID" = \'{row["ID"]}\';'
        #     #db.execute_query(query)
        #     with db.connect() as conn:
        #         conn.execute(sqlalchemy.text(query))
        # else:
        #     print(f"Hubo un error al parsear el archivo {downloadedFiles}. No se actualizó el valor descargado en la base de datos.")
        #acá termino el comment

        if status:
        # actualizamos el valor de descargado en la base de datos
            print(f"Actualizando el valor descargado en la base de datos para el ID {row['ID']}")
            query = f'UPDATE "archivosCAFCI" SET descargado = True, procesado_ok = True WHERE "ID" = \'{row["ID"]}\';'
            # print(f"Executing query: {query}")  # Debugging: print the query
            try:
                with db.connect() as conn:
                    result = conn.execute(sqlalchemy.text(query))
                    conn.commit()  # Commit the transaction
                    print(f"Query executed successfully, {result.rowcount} rows affected.")  # Debugging: print the number of affected rows
            except Exception as e:
                print(f"An error occurred while executing the query: {e}")  # Debugging: print any exceptions
        else:
            print(f"Hubo un error al parsear el archivo {downloadedFiles}. No se actualizó el valor descargado en la base de datos.")


        print(f"Borramos el archivo {downloadedFiles} descargado de la carpeta temporal.")
        # borramos el archivos en tempDir
        os.remove(os.path.join(tempDir, downloadedFiles))

        #print(f"Archivo {downloadedFiles} parseado y guardado en la base de datos. Continuando con el siguiente archivo.")

    # Termine. Cierro el browser y vuelvo
    driver.quit()

  
def parse_excel_file(downloadedFiles, db, ID) -> bool:
    """
    Esta función debe tomar el archivo descargado y parsearlo para obtener la información
    Luego grabar ese df en la base de datos
    """

    # Leer el archivo
    df = pd.read_excel(downloadedFiles, skiprows=9)

    nombresColumna = [
        "fondo",
        "clasMoneda",
        "clasRegion",
        "clasHorizonte",
        "fecha",
        "vcp",
        "vcpAnterior",
        "varVcp",
        "reexPesos",
        "varVcp1",
        "varVcp2",
        "varVcp3",
        "ccp",
        "ccpAnterior",
        "patrimonio",
        "patrimonioAnterior",
        "marketShare",
        "sociedadDepositaria",
        "codigoCNV",
        "calificacion",
        "codigoCAFCI",
        "codigoSocGte",
        "codigoSocDep",
        "sociedadGerente",
        "codigoClasificacion",
        "codigoMoneda",
        "codigoRegion",
        "codigoHorizonte",
        "indiceMM",
        "comisionIngreso",
        "honorariosAdmSG",
        "honorariosAdmSD",
        "gastosOrdGestion",
        "comisionRescate",
        "comisionTransferencia",
        "honorariosExito",
        "monedaFondo",
        "plazoLiq",
        "decreto596",
        "idFondoCAFCIpadre",
        "idFondoCNVpadre",
        "tipoEscision",
        "repatriacion",
        "minimoInversion",
        "regularizacionLey27743",
        "tipodinero"
    ]

    # Si df tiene 44 columnas, es un archivo de los viejos y hay que sacar la última columna de nombresColumna y seguir procesando.
    # Si tiene 45 columnas, es un archivo de los nuevos y hay que seguir procesando con los nombres de columnas asignados
    # Si tiene cualquier otro número de columnas, es un archivo raro y no lo vamos a procesar. Informamos cuantas columnas tiene, ponemos que no se proceso y retornamos False
    if df.shape[1] == 44:
        nombresColumna = nombresColumna[:-1]
        df.columns = nombresColumna
        df['regularizacionLey27743'] = None
    elif df.shape[1] == 46:
        df.columns = nombresColumna
    else:
        print(f"El archivo {downloadedFiles} tiene {df.shape[1]} columnas. No es un archivo común. No se grabará en la base de datos.")
        query = f'UPDATE "archivosCAFCI" SET procesado_ok = False WHERE \"ID\" = \'{ID}\';'
        with db.connect() as conn:
            conn.execute(sqlalchemy.text(query))
        return False # con esto status será False y no se actualizará el valor descargado en la base de datos

    # Eliminamos las filas que tienen el atributo clasMoneda vacío y asi nos quitamos de encima los títulos intermedios
    df = df.dropna(subset=["clasMoneda"])

    # Convertimos la columna fecha a datetime sin la hora
    df.iloc[:, 4] = pd.to_datetime(df.iloc[:, 4], format='%d/%m/%y').dt.date

    # Convertimos la columna 5 a float pero primero le sacamos algunos caracteres inválidos con coerce. pondrá NaN en esos casos
    df.iloc[:,5] = pd.to_numeric(df.iloc[:,5], errors='coerce')

    # Agregamos una columna, ID, que nos indica los datos a qué bajada pertenecen
    df = df.copy()  # Ensure we are working with a copy of the DataFrame
    df.loc[:, 'ID'] = ID

    # Grabar el df en la base de datos
    df.to_sql(name = 'tablaTempFCI', con = db, index = False, schema = 'public', if_exists='append')
    #db.to_sql(df, "tablaTempFCI")

    print(f"Archivo {downloadedFiles} parseado y guardado en la base de datos.")

    return True


def which_IDs(db):
    """
    Devuelve una lista con los IDs de los archivos que no se han descargado.
    Parsea la tabla archivosCAFCI y obtiene los IDS, hrefs que tienen descargado = False y que procesado_ok = NULL (esto último quiere decir que todavía no se intentó procesar)
    """

    # consultamos cuales tienen descargado = False
    query = 'SELECT * FROM "archivosCAFCI" WHERE descargado = False and procesado_ok is NULL;'
    df = pd.read_sql(query, db)

    # retornamos el df
    return df


if __name__ == "__main__":
    # Traer los IDs que no han sido descargados
    # db = DatabaseConnection(db_type="postgresql", db_name= os.environ.get('POSTGRES_DB'))
    # db.connect()
    db = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    
    # Imprimimos hora y día de comienzo
    print(f"Iniciando descarga de archivos el día {pd.Timestamp.now()}")

    # Qué archivos no han sido descargados?
    df = which_IDs(db)

    # si df vuelve vacío, no hay archivos para descargar
    if not df.empty:
        # Descargar los archivos de los IDs
        # vamos a probar solo con el primero
        download_file(df, db)

        print(f"Descarga de archivos finalizada el día {pd.Timestamp.now()}")
        print("-----------------------------------------------------------------")
    else:
        print("No hay archivos para descargar. Finalizando.")
    # db.disconnect()

