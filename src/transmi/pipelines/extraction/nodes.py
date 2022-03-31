import pandas as pd
import numpy as np
import requests
from io import StringIO
import datetime
from typing import Dict, Union
from transmi.pipelines.extraction.utilities import _prepare_output_extraction_troncal

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
        print("Pandas")
        response_pd = pd.read_csv(data_str_io, sep=",")
        del response, data_str_io
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
