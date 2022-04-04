import pandas as pd
import numpy as np
import requests
from io import StringIO
import datetime
from typing import Dict, Union
from transmi.pipelines.extraction.utilities import _prepare_output_extraction_troncal
from itertools import product

BASE_VALIDACIONES_REQUEST = "https://storage.googleapis.com/validaciones_tmsa/"


def extraction_validaciones_troncal(
    start_validaciones_troncal: datetime.datetime.date,
    extraction_day: Union[str, datetime.datetime.date],
    validaciones_troncal_log: Dict,
    dic_estacion_linea: Dict,
):
    """Node that etracts transmilenio demand data partitioned by day"""
    # Sets extraction date
    if extraction_day == "next":
        end_validaciones_troncal = (
            datetime.datetime.today().date() - datetime.timedelta(days=2)
        )
        validaciones_range = [
            datetime.datetime.date(x)
            for x in pd.date_range(start_validaciones_troncal, end_validaciones_troncal)
        ]
        first_failed = None
        for day in validaciones_range:
            if not str(day) in validaciones_troncal_log.keys():
                extraction_day = day
                break
            elif (
                "fail" == validaciones_troncal_log[str(day)][:4]
                and first_failed is None
            ):
                first_failed = day
        if extraction_day == "next":
            if first_failed is not None:
                extraction_day = first_failed
            else:
                return (
                    _prepare_output_extraction_troncal(pd.DataFrame()),
                    validaciones_troncal_log,
                    dic_estacion_linea,
                )
    print(f"Extraction day - {extraction_day}")
    response = requests.get(
        f"{BASE_VALIDACIONES_REQUEST}ValidacionTroncal/validacionTroncal{str(extraction_day).replace('-', '')}.csv"
    )
    # Succesfull request
    if response.status_code == 200:
        data_str_io = StringIO(response.text)
        del response
        print("Pandas")
        response_pd = pd.read_csv(data_str_io, sep=",")
        del data_str_io
        dic_estacion_linea_response = (
            response_pd[["Estacion_Parada", "Linea"]]
            .drop_duplicates()
            .set_index("Estacion_Parada")
            .to_dict()["Linea"]
        )
        response_pd.drop(
            [
                "ID_Vehiculo",
                "Ruta",
                "Tipo_Vehiculo",
                "Operador",
                "Hora_Pico_SN",
                "Dispositivo",
                "Day_Group_Type",
                "Fecha_Clearing",
                "Tipo_Tarifa",
                "Sistema",
                "Saldo_Despues_Transaccion",
                "Fase",
                "Linea",
            ],
            axis=1,
            inplace=True,
        )

        def _process_date(date):
            try:
                return date.astimezone(tz="America/Bogota")
            except TypeError:
                return date

        response_pd["Fecha_Transaccion"] = pd.to_datetime(
            response_pd["Fecha_Transaccion"]
        ).apply(_process_date)
        response_pd["day"] = response_pd["Fecha_Transaccion"].apply(lambda x: x.date())
        response_pd["hour"] = response_pd["Fecha_Transaccion"].apply(lambda x: x.hour)
        response_pd["minute"] = response_pd["Fecha_Transaccion"].apply(
            lambda x: x.minute
        )
        response_pd.drop("Fecha_Transaccion", axis=1, inplace=True)
        try:
            assert list(response_pd.columns) == [
                "Acceso_Estacion",
                "Emisor",
                "Estacion_Parada",
                "Nombre_Perfil",
                "Numero_Tarjeta",
                "Saldo_Previo_a_Transaccion",
                "Tipo_Tarjeta",
                "Valor",
                "day",
                "hour",
                "minute",
            ], "Database inconsistency"
        except AssertionError:
            print("Database inconsistency")
            validaciones_troncal_log[
                str(extraction_day)
            ] = "fail - Database inconsistency"
            return (
                _prepare_output_extraction_troncal(pd.DataFrame()),
                validaciones_troncal_log,
                dic_estacion_linea,
            )

        validaciones_troncal_log[str(extraction_day)] = "success"
        dic_estacion_linea.update(dic_estacion_linea_response)
        return (
            _prepare_output_extraction_troncal(response_pd),
            validaciones_troncal_log,
            dic_estacion_linea,
        )

    else:
        reason = response.reason
        print(f"Failed request: {reason}")
        validaciones_troncal_log[str(extraction_day)] = f"fail - {reason}"
        return (
            _prepare_output_extraction_troncal(pd.DataFrame()),
            validaciones_troncal_log,
            dic_estacion_linea,
        )


def extraction_summary_validaciones_troncal(links_data_transmi: pd.DataFrame):
    """Node that download and process summary of troncal validaciones every 15 minutes"""
    result = None
    # Iterate over each link or month to extract from the csv in 01_raw
    sheet_names_list = [
        "Validaciones Consolidado",
        "VALIDACIONES CONSOLIDADO",
        "VALIDACIONES TULLAVE",
        "Validaciones Tullave",
    ]
    for row in links_data_transmi.iterrows():
        link = row[1]["Link"]
        link_type = row[1]["Type"]
        link = link.replace(" ", "%20")
        if link_type == 1:
            print(link)
            xl = pd.ExcelFile(BASE_VALIDACIONES_REQUEST + "ValidacionTroncal/" + link)
            visible_sheets = [
                sh.title for sh in xl.book.worksheets if sh.sheet_state == "visible"
            ]
            sheet = visible_sheets[0]
            for sh in sheet_names_list:
                if sh in visible_sheets:
                    sheet = sh
                    break
            excel = xl.parse(sheet)
            for i, j in product(range(8), range(8)):
                if str(excel.iloc[i, j]).strip() == "Fase":
                    start_coord = i, j
                    break
            cols = excel.iloc[start_coord[0], start_coord[1] :].values
            cols = [
                c.date() if type(c) == datetime.datetime else str(c).strip()
                for c in cols
            ]
            excel = excel.iloc[start_coord[0] + 1 :, start_coord[1] :].copy()
            excel.columns = cols
            excel = excel[excel["Fase"] != "Total general"].copy()
            excel.drop(["Total general", "Fase"], axis=1, inplace=True)
            excel.dropna(how="all", inplace=True)
            excel.dropna(how="all", axis=1, inplace=True)
            excel_summary = (
                excel.drop(["Línea", "Acceso de Estación"], axis=1)
                .groupby(["Estación", "Intervalo"])
                .sum()
            )
            excel_summary = excel_summary.reset_index().melt(
                id_vars=["Estación", "Intervalo"], var_name="date", value_name="demand"
            )
            result = pd.concat([result, excel_summary], ignore_index=True)

    result["Intervalo"] = result["Intervalo"].apply(
        lambda x: datetime.time(int(x.split(":")[0]), int(x.split(":")[1]))
        if type(x) == str
        else x
    )

    return result
