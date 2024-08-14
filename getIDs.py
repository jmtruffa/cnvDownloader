"""
Este script debería correr cada día para actualizar la tabla de archivosCAFCI 
con los nuevos registros que se hayan cargado en la página de la CNV.
"""

import time
import pandas as pd
import locale
import os
from io import StringIO
from bs4 import BeautifulSoupº
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from DataBaseConn import DatabaseConnection

def getTablaFromURL():
    """
    Get the table from the URL and return a DataFrame with the data
    """
    locale.setlocale(locale.LC_TIME, 'es_ES')
    try:
        # Set up Selenium WebDriver
        options = Options()
        options.add_argument('--headless')  # Ensure headless mode is enabled
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Optional: For better stability in headless mode, you might want to add:
        options.add_argument('--disable-gpu')
        options.add_argument('start-maximized')
        options.add_argument('disable-infobars')
        options.add_argument('--disable-extensions')

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        # URL of the webpage
        url = 'https://www.cnv.gov.ar/SitioWeb/FondosComunesInversion/CuotaPartes'

        # Load the webpage
        print(f"Cargando la página {url}")
        driver.get(url)
        print(f"Página cargada")

        # Click the "VER MÁS" button
        print("Haciendo click en 'VER MÁS' para expandir la lista de archivos disponibles.")
        while True:
            try:
                ver_mas_button = driver.find_element(By.XPATH, "//span[@class='btn btn-leer-mas']")
                ActionChains(driver).move_to_element(ver_mas_button).click(ver_mas_button).perform()
                print("Boton presionado. Esperando que cargue el contenido...")
                time.sleep(2)  # Wait for more content to load
            except:
                print("Continuando a carga de tabla")
                break

        # Get the page source after loading more content
        page_source = driver.page_source
        driver.quit()

        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(page_source, 'lxml')

        # Find the table in the HTML
        table = soup.find('table')
        if table is None:
            print("No encontré ninguna tabla en la página.")
            return
        print("Recorriendo la tabla para extraer los datos...")
        # Extract data from the table
        rows = table.find_all('tr')[1:]  # Skip the header row
        fechas_documento = []
        fechas_recepcion = []
        descripciones = []
        ids = []
        hrefs = []

        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 4:
                a_tag = cells[0].find('a')
                if a_tag:
                    fechas_documento.append(a_tag.text.strip())
                    hrefs.append(a_tag['href'].strip())
                else:
                    fechas_documento.append(cells[0].text.strip())
                    hrefs.append(None)

                fechas_recepcion.append(cells[1].text.strip())
                descripciones.append(cells[2].text.strip())
                ids.append(cells[3].text.strip())

        # Create DataFrame
        df = pd.DataFrame({
            'fechaCorresponde': fechas_documento,
            'fechaRecepcion': fechas_recepcion,
            'descripcion': descripciones,
            'ID': ids,
            'href': hrefs,
            'descargado': False
        })

        # Convert first and second column of df into datetime. format is 3 jun 2024
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], format='%d %b %Y')
        df.iloc[:, 1] = pd.to_datetime(df.iloc[:, 1], format='%d %b %Y %H:%M')

        # Add a column with the date parsed from the 2nd column of the df
        df['fechaCorrespondeParseada'] = df.iloc[:, 2].apply(lambda x: x.split(" al")[1].strip())
        df['fechaCorrespondeParseada'] = pd.to_datetime(df['fechaCorrespondeParseada'], format='%d %b. %Y')

        print("Tabla obtenida correctamente.")

        return(df)

    except Exception as e:
        print(f"Ocurrió un error: {e}")
        
def grabaTabla(df):
    """
    vamos a grabar la tabla pero verificando primero que no haya registros duplicados
    """
    db = DatabaseConnection(db_type="postgresql", db_name= os.environ.get('POSTGRES_DB'))
    db.connect()
    # chequeamos si hay registros duplicados
    query = 'SELECT * FROM "archivosCAFCI"'
    df_db = pd.read_sql(query, db.engine)
    if df_db.empty:
        print(f"La tabla está vacía, grabando...")
        df.to_sql(name = 'archivosCAFCI', con = db.engine, index = False, schema = 'public')
    else:
        print(f"La tabla almacenada en la base de datos tiene {df_db.shape[0]} registros")
        print(f"La tabla obtenida de la web tiene {df.shape[0]} registros")
        df = df[~df['ID'].isin(df_db['ID'])]
        if df.empty:
            print(f"No hay registros nuevos para grabar. Todos los IDs ya estaban en la base de datos.")
        else:
            print(f"Grabando {df.shape[0]} registros nuevos...")
            df.to_sql(name = 'archivosCAFCI', con = db.engine, index = False, schema = 'public', if_exists='append')

    db.disconnect() 

if __name__ == "__main__":
    print(f"Iniciando...")

    # baja la tabla y la devuelve en un dataframe
    tabla = getTablaFromURL()

    # grabamos la tabla en la base de datos
    print(f"Grabando tabla en la base de datos...")
    grabaTabla(tabla)