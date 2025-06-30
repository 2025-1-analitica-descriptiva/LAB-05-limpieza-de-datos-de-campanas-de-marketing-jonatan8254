"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import glob
from zipfile import ZipFile
import pandas as pd
def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    # Directorios de entrada y salida
    input_dir = os.path.join("files", "input")
    output_dir = os.path.join("files", "output")
    os.makedirs(output_dir, exist_ok=True)

    # Buscar todos los archivos ZIP en el directorio de entrada
    zip_files = glob.glob(os.path.join(input_dir, "*.zip"))
    data_list = []

    for zpath in zip_files:
        with ZipFile(zpath) as archive:
            # Se asume que cada ZIP contiene al menos un archivo CSV
            csv_files = [name for name in archive.namelist() if name.endswith(".csv")]
            if not csv_files:
                continue
            # Leer el primer CSV del archivo ZIP
            with archive.open(csv_files[0]) as f:
                df = pd.read_csv(f)
                # Eliminar columnas sin nombre (como "Unnamed: 0")
                df = df.loc[:, ~df.columns.str.contains("Unnamed")]
                data_list.append(df)

    if not data_list:
        print("No se encontraron archivos con datos.")
        return

    full_df = pd.concat(data_list, ignore_index=True)

    # --- Procesar client.csv ---
    # Columnas requeridas: client_id, age, job, marital, education, credit_default, mortgage
    client = full_df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    # Transformar 'job': quitar puntos y reemplazar guiones por guiones bajos
    client["job"] = (
        client["job"].astype(str)
        .str.strip()
        .str.lower()
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )
    # Transformar 'education': convertir a minúsculas, reemplazar punto por guión bajo y cambiar "unknown" a pd.NA
    client["education"] = (
        client["education"].astype(str)
        .str.strip()
        .str.lower()
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )
    # Convertir credit_default y mortgage a 1 si son "yes", de lo contrario 0
    client["credit_default"] = client["credit_default"].apply(lambda x: 1 if str(x).strip().lower() == "yes" else 0)
    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if str(x).strip().lower() == "yes" else 0)

    # --- Procesar campaign.csv ---
    # Columnas requeridas: client_id, number_contacts, contact_duration, previous_campaign_contacts, previous_outcome, campaign_outcome, last_contact_date
    camp = full_df[[
        "client_id", "number_contacts", "contact_duration", "previous_campaign_contacts",
        "previous_outcome", "campaign_outcome", "month", "day"
    ]].copy()

    # Convertir el mes (abreviatura) a número; se asume que vienen en minúsculas
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    camp["month"] = camp["month"].astype(str).str.strip().str.lower().map(month_map)
    # Asegurar que 'day' tenga dos dígitos
    camp["day"] = camp["day"].astype(str).str.strip().str.zfill(2)
    # Crear la columna last_contact_date con año 2022
    camp["last_contact_date"] = "2022-" + camp["month"] + "-" + camp["day"]
    # Eliminar columnas temporales month y day
    camp.drop(columns=["month", "day"], inplace=True)
    # Convertir indicadores: previous_outcome ("success" -> 1, sino 0) y campaign_outcome ("yes" -> 1, sino 0)
    camp["previous_outcome"] = camp["previous_outcome"].apply(lambda x: 1 if str(x).strip().lower() == "success" else 0)
    camp["campaign_outcome"] = camp["campaign_outcome"].apply(lambda x: 1 if str(x).strip().lower() == "yes" else 0)

    # --- Procesar economics.csv ---
    # Columnas requeridas: client_id, cons_price_idx, euribor_three_months
    economics = full_df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    # Guardar los tres CSV en la carpeta de salida
    client.to_csv(os.path.join(output_dir, "client.csv"), index=False)
    camp.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    economics.to_csv(os.path.join(output_dir, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()



