import os
import requests
import zipfile

# Lista de URL de los archivos
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # URL inválida
]

# Carpeta donde guardaremos los archivos descargados
DOWNLOAD_DIR = "downloads"

# Función que descarga, descomprime y borra el zipcmdm
def download_and_extract(url: str, folder: str) -> None:
    try:
        # Sacar el nombre del archivo de la URL
        filename = url.split("/")[-1]
        filepath = os.path.join(folder, filename)

        # Printeo que se esta descargando el archivo
        print(f"Descargando {filename} ...")

        # Descargar el archivo desde Internet (tiempo maximo de 15 segundos)
        r = requests.get(url, timeout=15)
        r.raise_for_status()

        # Guardar el archivo en el ordenador
        with open(filepath, "wb") as f:
            f.write(r.content)

        # Abrir el zip y extraer lo que haya en la carpeta
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(folder)

        # Borrar el zip después de extraer
        os.remove(filepath)

        # Printeo que se ha descargado y extraido con exito el archivo
        print(f"{filename} descargado y extraído con éxito.")

    # Si algo falla, mostramos error
    except Exception as e:
        print(f"Error con {url}: {e}")


# Función principal
def main() -> None:
    # Crear la carpeta downloads
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    # Bucle que recorre todas las URLs y descargarlas una a una
    for url in download_uris:
        download_and_extract(url, DOWNLOAD_DIR)

if __name__ == "__main__":
    main()
