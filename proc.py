import abc
import os
import re
import pandas as pd
import shutil as sh
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from tqdm import tqdm


class Tabla:
    path = ''
    nombre = ''
    df = None

    def __bool__(self):
        return True

    def set_csv_path(self, path):
        self.path = path

    @abc.abstractmethod
    def load_csv_to_dataframe(self):
        pass

    def get_dataframe(self):
        return self.df

    def get_nombre(self):
        return self.nombre


class TablaContratos(Tabla):
    def __init__(self):
        self.nombre = 'entrega_dggi_tarifa'

    def load_csv_to_dataframe(self):
        headers = ['mes', 'año', 'id_empresa', 'id_linea', 'interno', 'ramal', 'tarifa_base_itg', 'debitado', 'contrato',
                   'viaje_integrado', 'descuento_x_integracion', 'cantidad_usos', 'monto', 'total_desc_por_integracion']
        dtypes = {'mes': 'str', 'año': 'str', 'id_empresa': 'str', 'id_linea': 'str', 'interno': 'str', 'ramal': 'str', 'tarifa_base_itg': 'float', 'debitado': 'float',
                  'contrato': 'str', 'viaje_integrado': 'str', 'descuento_x_integracion': 'float', 'cantidad_usos': 'float', 'monto': 'float', 'total_desc_por_integracion': 'float'}
        parse_dates = {'fecha': ['mes', 'año']}
        self.df = pd.read_csv(self.path, skiprows=1, sep=';', decimal=',',
                              header=None, names=headers, dtype=dtypes, parse_dates=parse_dates)


class TablaDistancias(Tabla):
    def __init__(self):
        self.nombre = 'entrega_dist_serv_fechaok'

    def load_csv_to_dataframe(self):
        headers = ['id_empresa', 'id_linea', 'ramal', 'interno', 'fecha_ini',
                   'hora_ini', 'fecha_fin', 'hora_fin', 'distancia_servicio_km']
        dtypes = {'id_empresa': 'str', 'id_linea': 'str', 'ramal': 'str', 'interno': 'str', 'fecha_ini': 'str',
                  'hora_ini': 'str', 'fecha_fin': 'str', 'hora_fin': 'str', 'distancia_servicio_km': 'float'}
        parse_dates = {'fecha_hora_ini': [
            'fecha_ini', 'hora_ini'], 'fecha_hora_fin': ['fecha_fin', 'hora_fin']}
        self.df = pd.read_csv(self.path, skiprows=1, sep=';', decimal=',', header=None,
                              names=headers, dtype=dtypes, parse_dates=parse_dates, dayfirst=True)

        self.df['fecha'] = self.df['fecha_hora_ini'].apply(
            lambda x: pd.to_datetime('{}-{}'.format(x.year, x.month)))

        self.df = self.df.drop(columns=['fecha_hora_ini', 'fecha_hora_fin'])
        self.df = self.df.groupby(
            by=['id_empresa', 'id_linea', 'ramal', 'interno', 'fecha']).sum().reset_index()


class TablaTarifasPlanas(Tabla):
    def __init__(self):
        self.nombre = 'tarifa_plana'

    def load_csv_to_dataframe(self):
        headers = ['fecha', 'valor']
        dtypes = {'fecha': 'str', 'valor': 'float'}
        parse_dates = ['fecha']
        self.df = pd.read_csv(self.path, skiprows=1, sep=';', decimal=',',
                              header=None, names=headers, dtype=dtypes, parse_dates=parse_dates)


class TablaLineas(Tabla):
    def __init__(self):
        self.nombre = 'lineas'

    def load_csv_to_dataframe(self):
        headers = ['id_linea', 'linea']
        dtypes = {'id_linea': 'str', 'linea': 'str'}
        self.df = pd.read_csv(self.path, skiprows=1, sep=';',
                              header=None, names=headers, dtype=dtypes)


class FabricaTabla:
    def get_tabla(self, path):
        tabla = None

        match = re.search('Entrega_dggi_tarifa', path)
        if match:
            tabla = TablaContratos()

        match = re.search('Entrega_Dist_Serv_FechaOk', path)
        if match:
            tabla = TablaDistancias()

        match = re.search('Tarifa_plana', path)
        if match:
            tabla = TablaTarifasPlanas()

        match = re.search('Lineas', path)
        if match:
            tabla = TablaLineas()

        if tabla:
            tabla.set_csv_path(path)

        return tabla


class DataBase:
    engine = None

    def create_session(self, uri, ssl_args):
        engine = create_engine(uri, connect_args=ssl_args)
        self.session = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=engine))

    def save_dataframe(self, tabla):
        tabla.get_dataframe().to_sql(tabla.get_nombre(), con=self.session.get_bind(),
                                     if_exists='append', index=False, chunksize=10000, method='multi')
        self.session.flush()

    def commit(self):
        self.session.commit()


if __name__ == '__main__':
    db = DataBase()
    # db.create_engine('sqlite:///data.db')
    ssl_args = {'sslrootcert': 'server-ca.pem',
                'sslcert': 'client-cert.pem', 'sslkey': 'client-key.pem'}
    db.create_session(
        'postgresql://crud_datos_sube:domino@35.199.112.198:5432/datos_sube', ssl_args)

    fabrica = FabricaTabla()

    data_path = './data'
    proc_path = os.path.join(data_path, 'procesados')
    if not os.path.isdir(proc_path):
        os.mkdir(proc_path)
    files = os.listdir(data_path)
    for f in tqdm(files):
        file_path = os.path.join(data_path, f)
        if os.path.isfile(file_path):
            tabla = fabrica.get_tabla(file_path)
            if (tabla):
                tabla.load_csv_to_dataframe()
                db.save_dataframe(tabla)
                sh.move(file_path, os.path.join(proc_path, f))
    db.commit()
