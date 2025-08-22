# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 16:35:40 2024

@author: allan
"""
print('Importando librerías...')
# Sistema operativo
import os
    
# Google Sheets
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pygsheets
    
# Fechas y tiempo
from datetime import timedelta
import datetime
from pandas.tseries.offsets import DateOffset
    
# Data Base
from sqlalchemy import create_engine
import mysql.connector as sql  
from urllib.parse import quote

# Warnings
import sys
if not sys.warnoptions:
    import warnings
    warnings.simplefilter('ignore')
 
# Hacemos la conexión a la base de datos
geo = 'ESP'
productos = '(3, 13, 14)'
#productos = '(3, 13, 14, 40)'

if geo == 'MX':
    iva_applicated = 0.16
else:
    iva_applicated = 0.21

def database(geography):
    username= 'bi_analyst'
    database= 'usulatie_bbdd'
    
    if geography == 'MX':
        host= 'rds-production-writer.ibancar.mx'
        password= 'WdbA6Z43nju@u46Y!Jhi'
        
    else:
        host= 'rds-production-ibancar-writer.ibanaccess.com'
        password= 'oXDTh4jk@nPzFd86DT9V'
    
    print('Conectando a la base de datos...')
    db = sql.connect(host=host, database=database, user=username, password=password)
    print('¡Conexión a la base de datos hecha!')
    print(' ')
    return db

def round_financial_inplace(df: pd.DataFrame, decimals: int = 2) -> None:
    """
    Redondea in-place todas las columnas numéricas de un DataFrame a 'decimals' cifras.
    Los valores null (NaN) se mantienen como NaN sin error.
    """
    num_cols = df.select_dtypes(include=['float', 'int']).columns
    # Aplica el redondeo directamente sobre el DataFrame original
    df[num_cols] = df[num_cols].round(decimals)
    
def database_engine(geography, data_frame, table_name, if_exists):
    if geography == 'ESP':
        try:
            host= 'ibancar-develop.cluster-c538k0e1eqzj.eu-west-1.rds.amazonaws.com'
            username= 'usulatie_naunet'
            password= '@SKq&5@A96'
            database= 'usulatie_bbdd'
            
            engine = create_engine(f'mysql+mysqlconnector://{username}:%s@{host}:3306/{database}' % quote(password))
            data_frame.to_sql(con= engine, name= table_name, if_exists= if_exists)
            
            print(f'Tabla {table_name} actualizada en el esquema {database}')
            print(f'Rows: {data_frame.shape[0]} , Columns: {data_frame.shape[1]}')
        except:
            raise ValueError('Permisos de CREATE denegados')
    else:
        print('Sólo se tiene permiso de CREATE en la base de datos de España')

def conexion_sheets(nombre_hoja, df_list, pos_x_list, pos_y_list):
    #Lo exportaremos al sheets correspondientes
    ################################################################################
    # En esta parte del código nos dedicaremos a obtener la información de los usuarios del sheets
    # donde se registra todas las transacciones reportadas
    ################################################################################
    # Hacemos la conexión con el google sheets para filtrar la información de las transacciones reportadas que no se han escalado
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # Es importante que el archivo key.json esté en la carpeta que se ha seleccionado para guardar el archivo
    
    os.path.commonprefix(['/usr/lib', '/usr/local/lib'])
    KEY = r'G:\Unidades compartidas\Business Intelligence IBANCAR\Bibliotecas\credenciales_SHEETS.json'
    
    # Escribe aquí el ID de tu documento: el ID del documento que vamos a consultar. Si vemos la URL de nuestra hoja de cálculo,
    # este será lo que está entre /d/ y /edit\n",
    SPREADSHEET_ID = '1oNoNErTARhLVj_xUEPW4xRGKNBA2hEvmvEUlmNkf9BY'
    
    # Cargamos las credenciales y configuramos la hoja de calculo en google
    creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    
    # Escribir en documentos en sheets
    gc = pygsheets.authorize(service_file=KEY)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        sh.add_worksheet(nombre_hoja)
    except:
        pass
       
    #Borramos la data de la hoja para siempre tener info limpia
    erased = sheet.values().clear(spreadsheetId = SPREADSHEET_ID, range = str(nombre_hoja)+'!A:AZ').execute()
    
    wks = sh.worksheet_by_title(nombre_hoja)
    
    # Colocamos en el sheets de collections_principal_capital_gapertura 
    for dt, pos_x, pos_y in list(zip(df_list, pos_x_list, pos_y_list)):
        wks.set_dataframe(dt, (pos_x,pos_y), encoding='utf-8')

    return print('Sheets IUVO_reports actualizado' )

def principal(inicio, corte):
    #Hacer la conexion a la base de datos
    db = database(geo)
    #  Seteamos las variables y llamamos a los queries, el primero será para 
    # traer, mes, año, préstamos emitidos e importe
    
    q1 = f'''SELECT EXTRACT(YEAR_MONTH from fechabanco) AS ym, SUM(importe) AS granted_loans
    FROM usulatie_bbdd.contratos
    WHERE idproductos IN {productos} AND fechabanco IS NOT NULL AND estado != 2 AND (DATE(fechabanco) BETWEEN '{inicio}' AND '{corte}')
    GROUP BY YEAR(fechabanco), MONTHNAME(fechabanco)
    ORDER BY YEAR(fechabanco) ASC, MONTH(fechabanco) ASC;'''
    principal_collections_pt1 = pd.read_sql(q1, db)    
    
    q2 = f'''SELECT EXTRACT(YEAR_MONTH from fechabanco) as ym, a.idcontratos AS loan_id, b.fechabanco, a.fechapagoefectiva, a.fechapago, 
    if( a.estado in (1,4), IFNULL(a.cuotacapital, 0) + IFNULL(a.gastoapertura, 0), 0)  as recov, 12*(YEAR(a.fechapagoefectiva)-YEAR(b.fechabanco)) + (MONTH(a.fechapagoefectiva) - MONTH(b.fechabanco)) as finance_period,
    YEAR(b.fechabanco) as year, MONTH(b.fechabanco) as month
    FROM usulatie_bbdd.cuotas as a
    LEFT JOIN usulatie_bbdd.contratos as b on b.id = a.idcontratos
    WHERE ((b.fechabanco BETWEEN '{inicio}' AND '{corte}') and b.idproductos IN {productos} AND b.fechabanco IS NOT NULL AND
    b.estado != 2 and a.estado in (1, 4, 5) AND (a.fechapagoefectiva BETWEEN '{inicio}' AND '{corte}')) or (b.id = 770);'''
    cash_pt1 = pd.read_sql(q2, db)
        
    q3 = f'''SELECT EXTRACT(YEAR_MONTH from fechabanco) as ym, b.idcontratos AS loan_id, c.fechabanco, fechapagoefectiva, fechapago, 
    if(a.estado in (1,4), IFNULL(a.cuotacapital,0) + 0, 0) as recov, 12*(YEAR(a.fechapagoefectiva) - YEAR(c.fechabanco)) + (MONTH(a.fechapagoefectiva) - MONTH(c.fechabanco)) as finance_period,
    YEAR(c.fechabanco) as year, MONTH(c.fechabanco) as month
    FROM usulatie_bbdd.cuotas_acuerdos AS a  
    INNER JOIN usulatie_bbdd.contratos_acuerdos AS b ON b.id = a.idcontratos
    LEFT JOIN contratos as c on c.id = b.idcontratos
    WHERE (fechapagoefectiva BETWEEN '{inicio}' AND '{corte}') AND b.estado != 2 AND b.idcontratos in (SELECT id FROM usulatie_bbdd.contratos 
    WHERE ((fechabanco BETWEEN '{inicio}' AND '{corte}') and idproductos IN {productos} AND fechabanco IS NOT NULL AND estado != 2 or (id in (770, 108, 430, 546, 277) ) ) );'''
    cash_pt2 = pd.read_sql(q3, db)
    
    q4 = f'''SELECT EXTRACT(YEAR_MONTH from fechabanco) as ym, a.idcontratos as loan_id, b.fechabanco, fechapagoefectiva, fechapago, 
    IFNULL(if(MAX(pagocancelacion) > SUM(IFNULL(cuotacapital, 0) + IFNULL(gastoapertura,0)), SUM(IFNULL(cuotacapital, 0) + IFNULL(gastoapertura,0)), MAX(pagocancelacion)), 0) AS recov,
    12*(YEAR(a.fechapagoefectiva) - YEAR(b.fechabanco)) + (MONTH(a.fechapagoefectiva) - MONTH(b.fechabanco)) as finance_period,
    YEAR(b.fechabanco) as year, MONTH(b.fechabanco) as month
    FROM usulatie_bbdd.cuotas AS a
    LEFT JOIN usulatie_bbdd.contratos AS b ON b.id = a.idcontratos
    WHERE ((b.fechabanco BETWEEN '{inicio}' AND '{corte}') and b.idproductos IN {productos} AND b.fechabanco IS NOT NULL AND
    b.estado != 2 and a.estado in (5) AND (a.fechapagoefectiva BETWEEN '{inicio}' AND '{corte}'))  or (b.id in (770, 108, 430, 546, 277) )
    GROUP BY a.idcontratos;'''
    cancelacion = pd.read_sql(q4, db)
    
    
    # Este query regresa la info de las ventas de los carros
    q5 = f'''SELECT EXTRACT(YEAR_MONTH from fechabanco) as ym, a.idcontratos as loan_id, b.fechabanco, a.fechapagoefectivo as fechapagoefectiva, a.fechapagoefectivo as fechapago,
    if( IFNULL(a.importe, 0) >= IFNULL(a.capitalpendiente, 0), IFNULL(a.capitalpendiente, 0), IFNULL(a.importe, 0)) AS recov,
    12*(YEAR(a.fechapagoefectivo)-YEAR(b.fechabanco)) + (MONTH(a.fechapagoefectivo) - MONTH(b.fechabanco)) as finance_period,
    YEAR(b.fechabanco) as year, MONTH(b.fechabanco) as month
    FROM usulatie_bbdd.coches_vendidos AS a
    LEFT JOIN usulatie_bbdd.contratos AS b ON b.id = a.idcontratos 
    WHERE ((b.fechabanco BETWEEN '{inicio}' AND '{corte}') and b.idproductos IN {productos} AND b.fechabanco IS NOT NULL AND
    b.estado != 2 AND (a.fechapagoefectivo BETWEEN '{inicio}' AND '{corte}'))  or (b.id in (770, 108, 430, 546, 277) );'''
    coches = pd.read_sql(q5, db)
        
    # Dado que traemos de cuotas y cuotas_acuerdos los uniremos con un concat
    cash = pd.concat([cash_pt1, cash_pt2, cancelacion])
    cash_car = pd.concat([cash_pt1, cash_pt2, cancelacion, coches])
    cash['recov'].fillna(value = 0, inplace = True)
    cash_car['recov'].fillna(value = 0, inplace = True)
    
    def pivot_tables(col_recov, col_ym, col_finance_period, df, df1, col_origin):
        #Nos aseguramos de que sólo existan periodos mayores o iguales a cero
        df = df[df[col_finance_period] >= 0]
        
        # Hacemos la tabla pivote donde agruparemos por año y mes el dinero de las tablas cuotas
        # y pondremos como columnas los periodos de finanzas
        df_back = pd.pivot_table(df, values = col_recov, index = [col_ym], columns = [col_finance_period],
                                                           fill_value = 0, aggfunc = 'sum').reset_index()
    
        # Tiraremos la columna de año y mes 
        hold = df_back.drop([col_ym], axis = 1)
        
        #Sacamos el valor de lo que falta por pagar restando a lo originado lo que se recupero en capital principal
        #y si aún así, queda el valor negativo, haremos un appley para corregir esto
        df_back['unpaid'] = hold.sum(axis = 1)
            
        # Hacemos el concat por columnas entre las tablas de collections
        return pd.merge(df1, df_back, on = col_ym, how = 'left')
        
    collections_principal = pivot_tables('recov', 'ym', 'finance_period', cash, principal_collections_pt1, 'granted_loans')
    collections_principal_car = pivot_tables('recov', 'ym', 'finance_period', cash_car, principal_collections_pt1, 'granted_loans')
    
    collections_principal['unpaid'] = collections_principal['granted_loans'] - collections_principal['unpaid']
    collections_principal_car['unpaid'] = collections_principal_car['granted_loans'] - collections_principal_car['unpaid']

    return collections_principal, collections_principal_car

def payoff(inicio, corte):
    #Nos posicionamos en la carpeta IUVO    
    os.chdir(r'C:\Users\Bi_analyst\Desktop\Python')
    
    #Traemos la conexión de la base de datos
    db = database(geo)   

    print(' ')
    print('Iniciando la creación del reporte payoff')
    
    collections_principal, collections_principal_car = principal(inicio, corte)
    print('Se ha importdo el módulo de principal collections')
    
    #Sacamos el día y hora actual
    q0 = 'SELECT CURDATE(), TIME(NOW());'
    hora_fecha = pd.read_sql(q0, db)
    
    # Importaremos de la base de datos la información de los pagos por contratos
    # y por acuerdo. Además, traemos la info de los coches vendidos
    q1 = f'''SELECT EXTRACT(YEAR_MONTH from b.fechabanco) as ym, a.estado, a.idcontratos AS loan_id, 
    IFNULL(12*(YEAR(a.fechapagoefectiva)-YEAR(b.fechabanco)) + (MONTH(a.fechapagoefectiva) - MONTH(b.fechabanco)), 0) as period,
    b.importe, a.fechapagoefectiva, a.fechapago, 
    IFNULL(if(a.estado in (1,4) and a.fechapagoefectiva <= '{corte}', a.importepago, 0), 0) as importepago, 
    IFNULL(if(a.estado in (1,4) and a.fechapagoefectiva <= '{corte}', a.cuota_intereses_iva, 0), 0) as cuota_intereses_iva, 
    IFNULL(if(a.estado in (1,4) and a.fechapagoefectiva <= '{corte}', a.cuotaserviciosiva, 0), 0) as cuotaserviciosiva, 
    IFNULL(if(a.estado in (1,4) and a.fechapagoefectiva <= '{corte}', a.cuota_gastoapertura_iva, 0), 0) as cuota_gastoapertura_iva, 
    0 as is_acuerdo, if(a.fechapagoefectiva <= '{corte}', IFNULL(a.pagocancelacion, 0), 0) as pagocancelacion, YEAR(b.fechabanco) as year, MONTH(b.fechabanco) as month
    FROM cuotas as a
    LEFT JOIN contratos as b on b.id = a.idcontratos
    WHERE ((b.fechabanco BETWEEN '{inicio}' and '{corte}') and b.idproductos IN {productos} AND b.fechabanco IS NOT NULL AND b.estado != 2
    and a.estado in (1, 4, 5) AND (a.fechapagoefectiva BETWEEN '{inicio}' AND '{corte}'))  or (b.id in (770, 108, 430, 546, 277) );'''
    
    q2 = f'''SELECT EXTRACT(YEAR_MONTH from c.fechabanco) as ym, a.estado, b.idcontratos AS loan_id, 
    IFNULL(12*(YEAR(a.fechapagoefectiva)-YEAR(c.fechabanco)) + (MONTH(a.fechapagoefectiva) - MONTH(c.fechabanco)), 0) as period,
    b.importe, fechapagoefectiva, fechapago,
    IFNULL(if(a.fechapagoefectiva <= '{corte}', cuotainteres, 0), 0) + IFNULL(if(a.fechapagoefectiva <= '{corte}', cuotacapital, 0),0) as importepago, 
    0 as cuota_intereses_iva, 0 as cuotaserviciosiva, 0 as cuota_gastoapertura_iva, 1 AS is_acuerdo,
    0 as pagocancelacion, YEAR(c.fechabanco) as year, MONTH(c.fechabanco) as month
    FROM usulatie_bbdd.cuotas_acuerdos AS a  
    INNER JOIN usulatie_bbdd.contratos_acuerdos AS b ON b.id = a.idcontratos
    LEFT JOIN usulatie_bbdd.contratos as c on c.id = b.idcontratos
    WHERE a.estado in (1, 4, 5) AND (a.fechapagoefectiva BETWEEN '{inicio}' AND '{corte}') AND b.idcontratos in (SELECT id FROM usulatie_bbdd.contratos 
    WHERE (fechabanco BETWEEN '{inicio}' AND '{corte}') and idproductos IN {productos} AND fechabanco IS NOT NULL AND estado != 2  or (id in (770, 108, 430, 546, 277) ));'''
    
    payoff_cash_pt1 = pd.read_sql(q1, db)
    payoff_cash_pt2 = pd.read_sql(q2, db)

    #Se extrae la información de los pagos registrados de la tabla contratos asegurándose de traer sólo las cancelaciones
    print('Extrayendo la información de pagos anticipados (pagos_cancelacion) de la tabla de cuotas')
    print(' ')
    q4 = f'''SELECT EXTRACT(YEAR_MONTH from b.fechabanco) as ym, a.estado, a.idcontratos as loan_id,
    12*(YEAR(a.fechapagoefectiva) - YEAR(b.fechabanco)) + (MONTH(a.fechapagoefectiva) - MONTH(b.fechabanco)) as period,
    b.importe, fechapagoefectiva, fechapago, IFNULL(pagocancelacion, 0) AS importepago, 0 as cuota_intereses_iva, 0 as cuotaserviciosiva, 0 as cuota_gastoapertura_iva,
    0 as is_acuerdo, 0 as pagocancelacion, YEAR(b.fechabanco) as year, MONTH(b.fechabanco) as month
    /*IFNULL(if(MAX(IFNULL(pagocancelacion, 0)) > SUM(IFNULL(cuotacapital, 0) + IFNULL(gastoapertura,0)), SUM(IFNULL(cuotacapital, 0) + IFNULL(gastoapertura,0)), MAX(IFNULL(pagocancelacion, 0))), 0)*/
    FROM usulatie_bbdd.cuotas AS a
    LEFT JOIN usulatie_bbdd.contratos AS b ON b.id = a.idcontratos
    WHERE ((b.fechabanco BETWEEN '{inicio}' AND '{corte}') and b.idproductos IN {productos} AND b.fechabanco IS NOT NULL AND
    b.estado != 2 and pagocancelacion IS NOT NULL AND (a.fechapagoefectiva BETWEEN '{inicio}' AND '{corte}'))  or (b.id in (770, 108, 430, 546, 277) );'''
    cancelacion = pd.read_sql(q4, db)
    
    # Este query regresa la info de las ventas de los carros
    print('Extrayendo la información de la venta de vehículos, restando el exceso devuelto en caso de que el importe vendido sea mayor al capital pendiente')
    print(' ')
    q5 = f'''SELECT EXTRACT(YEAR_MONTH from b.fechabanco) as ym, 1 as estado, a.idcontratos as loan_id, 
    12*(YEAR(a.fechapagoefectivo)-YEAR(b.fechabanco)) + (MONTH(a.fechapagoefectivo) - MONTH(b.fechabanco)) as period,
    0 as importe, a.fechapagoefectivo as fechapagoefectiva, b.fechabanco, IFNULL(a.importe, 0) - IFNULL(a.devolucion_exceso_venta, 0) as importepago,
    0 as cuota_intereses_iva, 0 as cuotaserviciosiva, 0 as cuota_gastoapertura_iva, 0 as is_acuerdo, 0 as pagocancelacion,
    YEAR(b.fechabanco) as year, MONTH(b.fechabanco) as month
    FROM usulatie_bbdd.coches_vendidos AS a
    LEFT JOIN usulatie_bbdd.contratos AS b ON b.id = a.idcontratos 
    WHERE (b.fechabanco BETWEEN '{inicio}' AND '{corte}') and b.idproductos IN {productos} AND b.fechabanco IS NOT NULL AND
    b.estado != 2 AND (a.fechapagoefectivo BETWEEN '{inicio}' AND '{corte}')  or (b.id in (770, 108, 430, 546, 277) );'''
    coches = pd.read_sql(q5, db)
    
    # Dado que traemos de cuotas y cuotas_acuerdos los uniremos con un concat
    payoff_cash = pd.concat([payoff_cash_pt1, payoff_cash_pt2, cancelacion]).reset_index(drop=True)
    payoff_cash_car = pd.concat([payoff_cash, coches]).reset_index(drop=True)
    
    payoff_cash['tot_amount'] = payoff_cash['importepago'] - payoff_cash[['cuota_intereses_iva', 'cuotaserviciosiva', 'cuota_gastoapertura_iva']].sum(axis = 1)
    payoff_cash_car['tot_amount'] = payoff_cash_car['importepago'] - payoff_cash_car[['cuota_intereses_iva', 'cuotaserviciosiva', 'cuota_gastoapertura_iva']].sum(axis = 1)
    payoff_cash['tot_amount'].fillna(value = 0, inplace = True)
    payoff_cash_car['tot_amount'].fillna(value = 0, inplace = True)
    
    def pivote(df, df1):
        cash_second = pd.pivot_table(df, values = 'tot_amount', index = 'ym', columns = 'period', aggfunc = 'sum', fill_value = 0)
        columnas = cash_second.columns
        cash_second['Total_received'] = cash_second[columnas].sum(axis = 1)
        cash_second.reset_index(inplace =  True)
        collections_principal = pd.merge(df1[['ym', 'granted_loans', 'unpaid']], cash_second, left_on = 'ym', right_on = 'ym', how = 'left')
        return collections_principal
    
    collections = pivote(payoff_cash, collections_principal)
    collections_car = pivote(payoff_cash_car, collections_principal_car)
    
    def arregla(df):
        df.rename(columns = {'ym': 'EUR'}, inplace = True)
        cols = df.columns.to_list()
        cols =  cols[0:3] + cols[-1:] + cols[3:-1]
        df = df[cols]
        df[cols[4:-1]] = df[cols[4:-1]].cumsum(axis = 1)
        return df
    collections = arregla(collections)
    collections_car = arregla(collections_car)
    
    conexion_sheets('payoff_report', [hora_fecha, collections], [1, 4], [1,1]) 
    conexion_sheets('payoff_plus_sold_car_report', [hora_fecha, collections_car], [1, 4], [1,1])
    print('payoff_report updated')

def portfolio(year, fecha_corte):
    #Nos posicionamos en la carpeta IUVO    
    os.chdir(r'C:\Users\Bi_analyst\Desktop\Python')
    
    #Traemos la conexión de la base de datos
    db = database(geo)
    
    #Creamos el bloque de funciones que usaremos a lo largo de función principal 
    def ventana(x):
        if x < 6:
            etiqueta = 'Current'
        elif 6 <= x <= 30:
            etiqueta = '1-30d'
        elif 31 <= x <= 60:
            etiqueta = '31-60d'
        elif 61 <= x <= 90:
            etiqueta = '61-90d'
        elif 91 <= x <= 120:
            etiqueta = '91-120d'
        elif 121 <= x <= 150:
            etiqueta = '121-150d'
        elif 151 <= x <= 180:
            etiqueta = '151-180d'
        else:
            etiqueta = '>180d'
        return etiqueta
    
    #Hacemos una función para vectorizar y quedarnos con la fecha única si es estado 1 o 5 y
    #Sacar el ym de la fecha límite
    def fechas(fecha_1, fecha_5, estado):
        if estado == '1':
            return fecha_1
        else:
            return fecha_5
        
    def ym(fecha):
        try:
            a = float(str(fecha.year)+str(fecha.strftime("%m")))
            return a
        except:
            return np.nan
        
    #Creamos la función que nos ayudará a vectorizar
    def updatecapital(col1, col2):
        if col1 - col2 <= 0:
            return 0
        else:
            return col1 - col2
        
    #Si fecha_limite <= fecha: ym_fecha | Si (fecha_limite > fecha) & (fecha_limite < max_fecha): ym_fecha
    #| Si (fecha_limite > fecha) & (fecha_limite >= max_fecha): ym_limite
    def union_fechas(fecham, fechal, fechaf, ymf, yml):
        if fechal <= fechaf:
            return ymf
        elif (fechal > fechaf) & (fechal < fecham):
            return ymf
        elif (fechal > fechaf) & (fechal >= fecham):
            return yml
        
    #Vamos a vectorizar y preguntar si las columnas de ym son iguales colocaremos el valor actual
    #caso contrario, colocaremos el primer valor distinto de null
    def bandera(col_com1, col_com2, col_valor, col_valno):
        try:
            if col_com1 == col_com2:
                return col_valor
            else:
                return col_valno
        except:
            return col_valno
        
    def banderadias(col_com1, col_com2, col_valor, col_valno):
        try:
            if col_com1 <= col_com2:
                return col_valor
            else:
                return col_valno
        except:
            return col_valno
        
    #Construiremos una función que nos ayudará ha hacer check donde el renglón i+1 debe ser menor o igual al i-ésimo
    def check_lower(col1, col2):
        if col2 <= col1:
            return 1
        else:
            return 0
        
    def cambio_etiquetas(etiqueta):
        if etiqueta == 'Amortized':
            return 'Amortizado'
        elif etiqueta == 'IMP':
            return 'Impairments'
        else:
            return etiqueta
        
    #Construiremos la función que nos ayudará a comparar el renglón actual con el siguiente por usuario para
    #obtener los renglones donde no se está cumpliendo que el principla_outstanding se mantenga o decrezca
    def check_principal(df):
        df['menor_igual_bandera'] = df['principal_outstanding'] - df['next_principal_outstanding']
        return df
        
    #Nos posicionasmos en la carpeta de IUVO
    os.chdir(r'C:\Users\Bi_analyst\Desktop\Python')
    
    print('Extraemos la información de los préstamos por mes')
    #Sacamos todas las fechasbanco de los usuarios y la info de sus cuotas
    #Sacamos cuantos usuarios por año mes se emitieron y se guardarán en una lista
    q0 =  f'''SELECT id as 'Loan Id', importe, fechabanco, fechabanco as fechabanco2, EXTRACT(YEAR_MONTH from fechabanco) as ym_fb
    FROM usulatie_bbdd.contratos
    WHERE idproductos IN {productos} AND fechabanco IS NOT NULL AND estado != 2 and id not in (322) and YEAR(fechabanco) >= {year};'''
    lstaid = pd.read_sql(q0, db) 
    lid = '(' + str(lstaid['Loan Id'].to_list())[1:-1] + ')'

        
    #Sacamos el día y hora actual
    q = 'SELECT CURDATE(), TIME(NOW());'
    hora_fecha = pd.read_sql(q, db)
    
    print('Traemos la información de las cuotas de los contratos originales')   
    cierre = fecha_corte
    #Ahora traeremos la info de todas las cuotas y sacaremos su diferencia de días 
    #Vamos a sacar los pagos que hicieron los usuarios
    q1 = f'''SELECT a.idclientes, a.id AS idcontratos, -1 as idcontratos_acuerdos, a.fechabanco, a.importe, a.comisionapertura, a.importe - a.comisionapertura as Amount_wo_of,a.numcuotas, b.fecha, b.numdelacuota, b.estado, b.fechapago, b.fechapagoefectiva, IFNULL(b.importecuota, 0) as importecuota, if(b.importepago < 0, 'Reversal', 'Payment') as "Transaction Type", IFNULL(b.importepago, 0) as importepago, IFNULL(b.cuotainteres, 0) as cuotainteres, IFNULL(b.cuota_intereses_iva, 0) as cuota_intereses_iva, 
    IFNULL(b.cuotacapital, 0) as cuotacapital, if(capitalpendiente_sga IS NOT NULL AND importepago IS NOT NULL, capitalpendiente - capitalpendiente_sga, NULL) AS "Pending Origination Fee", b.capitalpendiente as capitalpendiente, IFNULL(b.cuotaservicios, 0) as cuotaservicios, IFNULL(b.cuotaserviciosiva, 0) as cuotaserviciosiva, IFNULL(b.cuotaseguro, 0) as cuotaseguro, IFNULL(b.cuotasegurocapital, 0) as cuotasegurocapital,
    IFNULL(b.gastoapertura, 0) as gastoapertura, IFNULL(b.cuota_gastoapertura_iva, 0) as cuota_gastoapertura_iva, IFNULL(b.pago_pendiente, 0) as pago_pendiente, IFNULL(b.pagocancelacion, 0) as pagocancelacion,
    0 as acuerdo, if(DATEDIFF(b.fechapago, b.fecha) > 5 AND b.importepago > 0, b.importepago-b.importecuota, 0) as "Late fee & Prepayment fee", NULL as "Late fee & Prepayment fee VAT", CASE
    WHEN (IFNULL(b.importepago, 0) >= IFNULL(b.importecuota, 0)) AND (IFNULL(b.pagocancelacion, 0) = 0) THEN IFNULL(b.cuotacapital, 0) + IFNULL(b.gastoapertura, 0)
    WHEN (IFNULL(b.importepago, 0) < IFNULL(b.importecuota, 0)) AND (IFNULL(b.pagocancelacion, 0) = 0) THEN IFNULL(b.importepago, 0)
    WHEN IFNULL(b.pagocancelacion, 0) > 0 THEN IFNULL(b.pagocancelacion, 0)
    END as principal_recov_1, CASE
    WHEN b.estado = 5 and b.pagocancelacion is not NULL THEN DATEDIFF(b.fechapagoefectiva, b.fecha)
    WHEN b.estado = 5 and b.pagocancelacion is NULL THEN 0
    WHEN b.estado = 1 THEN DATEDIFF(b.fechapagoefectiva, fecha)
    ELSE NULL END as Days, FLOOR(DATEDIFF('{cierre}', def_fechanacimiento)/360 ) as "Age of customer", sueldo_neto as "Salary of Customer", "Individual" as "Prodcut Type",
    coche_tasacion as "Collateral Amount", importe/coche_tasacion as "Initial LTV", if(importepago > importecuota AND DATEDIFF(fechapago, fecha) <= 5 AND importecuota != 0, importepago - importecuota, 0) as plusfee,
    a.fechafin as 'Maturity Date',  d.tipo_interes/100 AS tipo_interes, e.nombrepq
    FROM usulatie_bbdd.contratos AS a
    LEFT JOIN usulatie_bbdd.cuotas AS b ON a.id = b.idcontratos
    LEFT JOIN usulatie_bbdd.clientes AS c on c.id = a.idclientes
    LEFT JOIN usulatie_bbdd.tipo_contratos AS d ON a.id_tipo_contrato = d.id
    LEFT JOIN usulatie_bbdd.def_p2p AS e ON e.id = a.idp2p
    WHERE a.id in {lid}
    GROUP BY a.id, b.fecha, b.id
    ORDER BY a.id, b.fecha;'''
    
    cuotas1 = pd.read_sql(q1, db)
    cuotas1['num_renglon'] = cuotas1.groupby(['idcontratos']).cumcount()+1
    
    #A la tabla de cuotas vamos a agregarle un renglón al inició de cada idcontratos donde la cuota sea cero,
    #la fechapago sea la fechabanco, estado = 1, acuerdo = 0, Days = 0, principal_recov_1 = 0 y el capital pendiente es el importe    
    columnas = ['importecuota', 'importepago', 'Days', 'acuerdo', 'principal_recov_1', 'num_renglon', 'cuotainteres', 'cuota_intereses_iva',
           'cuotaseguro', 'cuotasegurocapital', 'cuotacapital', 'cuotaservicios', 'cuotaserviciosiva', 'cuota_gastoapertura_iva',
           'pagocancelacion', 'pago_pendiente', 'gastoapertura']
    
    #Creamos una columna temporal que contendrá los ids para localizar los renglones
    cuotas1['idx'] = cuotas1.index
    
    #Sacamos los ínides para hacer un loc con esa lista de índices
    indices = cuotas1.groupby('idcontratos')[['fechabanco', 'idx']].first()
    indices = indices['idx'].to_list()
    
    #Concatenamos los dos dataframes, el original y el primer registro que tendrá la fechabanco
    cuotas = pd.concat([cuotas1, (cuotas1.loc[indices, ['fecha', 'idcontratos', 'fechabanco']].assign(fecha = lambda x: x['fechabanco'] ))]
                    ).sort_values(by=["idcontratos", "fecha"], ignore_index=True)
    
    #Dropeamos la columna auxiliar y rellenamos los otros valores importantes
    cuotas.drop('idx', axis = 1, inplace = True)
    idx = cuotas[cuotas['idcontratos_acuerdos'].isna()].index.to_list()
    
    cuotas.loc[idx, columnas] = 0
    cuotas.loc[idx, ['estado']] = '1'
    cuotas.loc[idx, 'fechapagoefectiva'] = cuotas.loc[idx, 'fechabanco']
    cuotas.loc[idx, ['Pending Origination Fee']] = cuotas.loc[list(np.array(idx) + 1), ['comisionapertura']].values
    cuotas.loc[idx, ['nombrepq']] = cuotas.loc[list(np.array(idx) + 1), ['nombrepq']].values
    cuotas.loc[idx, 'fechapago'] = cuotas.loc[idx, 'fechabanco']  
    cuotas.loc[idx, ['importe']] = cuotas.loc[list(np.array(idx) + 1), ['importe']].values
    cuotas.loc[idx, ['Maturity Date']] = cuotas.loc[list(np.array(idx) + 1), ['Maturity Date']].values
    cuotas.loc[idx, ['capitalpendiente']] = cuotas.loc[list(np.array(idx) + 1), ['importe']].values
    cuotas.loc[idx, ['idclientes']] = cuotas.loc[list(np.array(idx) + 1), ['idclientes']].values
    cuotas.loc[idx, ['comisionapertura']] = cuotas.loc[list(np.array(idx) + 1), ['comisionapertura']].values
    cuotas.loc[idx, ['Amount_wo_of']] = cuotas.loc[list(np.array(idx) + 1), ['Amount_wo_of']].values
    cuotas.loc[idx, ['numcuotas']] = cuotas.loc[list(np.array(idx) + 1), ['numcuotas']].values
    cuotas.loc[idx, ['Age of customer']] = cuotas.loc[list(np.array(idx) + 1), ['Age of customer']].values
    cuotas.loc[idx, ['Salary of Customer']] = cuotas.loc[list(np.array(idx) + 1), ['Salary of Customer']].values
    cuotas.loc[idx, ['Prodcut Type']] = cuotas.loc[list(np.array(idx) + 1), ['Prodcut Type']].values
    cuotas.loc[idx, ['Collateral Amount']] = cuotas.loc[list(np.array(idx) + 1), ['Collateral Amount']].values
    cuotas.loc[idx, ['Initial LTV']] = cuotas.loc[list(np.array(idx) + 1), ['Initial LTV']].values
    cuotas.loc[idx, ['idcontratos_acuerdos']] = -1
    
    print('Traemos la información de las cuotas de los acuerdos')
    #Vamos a sacar los pagos que hicieron los usuarios con acuerdos
    q2 = f'''SELECT c.idclientes, b.idcontratos, b.id as idcontratos_acuerdos, b.fechaalta AS fechabanco, b.importe, 0 as comisionapertura,  b.importe - 0 as Amount_wo_of, b.numcuotas, a.fecha, a.numdelacuota, a.estado, a.fechapago, a.fechapagoefectiva, IFNULL(a.importecuota, 0) as importecuota, if(a.importepago < 0, 'Reversal', 'Payment') as "Transaction Type", IFNULL(a.importepago, 0) as importepago, IFNULL(a.cuotainteres, 0) as cuotainteres, 0 AS cuota_intereses_iva, 
    IFNULL(a.cuotacapital, 0) as cuotacapital, NULL AS "Pending Origination Fee", IF(a.fechapago IS NOT NULL AND a.capitalpendiente IS NULL, 0, a.capitalpendiente) as capitalpendiente, 0 AS cuotaservicios, 0 AS cuotaserviciosiva, 0 as cuotaseguro, 0 AS cuotasegurocapital, 0 AS gastoapertura, 0 AS cuota_gastoapertura_iva, IFNULL(a.pago_pendiente, 0) as pago_pendiente, 
    IFNULL(a.pagocancelacion, 0) as pagocancelacion, 1 as acuerdo, CASE
    WHEN (IFNULL(a.importepago, 0) >= IFNULL(a.importecuota, 0)) AND (IFNULL(a.pagocancelacion, 0) = 0) THEN IFNULL(a.cuotacapital, 0)
    WHEN (IFNULL(a.importepago, 0) < IFNULL(a.importecuota, 0)) AND (IFNULL(a.pagocancelacion, 0) = 0) THEN IFNULL(a.importepago, 0)
    WHEN IFNULL(a.pagocancelacion, 0) > 0 THEN IFNULL(a.pagocancelacion, 0)
    END as principal_recov_1, if(DATEDIFF(a.fechapago, a.fecha) > 5 AND a.importepago > 0, a.importepago-a.importecuota, 0) as "Late fee & Prepayment fee", NULL as "Late fee & Prepayment fee VAT", CASE
    WHEN a.estado = 5 and a.pagocancelacion is not NULL THEN DATEDIFF(a.fechapagoefectiva, a.fecha)
    WHEN a.estado = 5 and pagocancelacion is NULL THEN 0
    WHEN a.estado = 1 THEN DATEDIFF(a.fechapagoefectiva, a.fecha)
    ELSE NULL END as Days, FLOOR(DATEDIFF('{cierre}', def_fechanacimiento)/360 ) as "Age of customer", sueldo_neto as "Salary of Customer", "Individual" as "Prodcut Type",
    coche_tasacion as "Collateral Amount", c.importe/coche_tasacion as "Initial LTV", if(importepago > importecuota AND DATEDIFF(fechapago, fecha) <= 5 AND importecuota != 0, importepago - importecuota, 0) as plusfee,
    b.fechafin as 'Maturity Date', e.tipo_interes/100 AS tipo_interes
    FROM usulatie_bbdd.cuotas_acuerdos AS a  
    INNER JOIN usulatie_bbdd.contratos_acuerdos AS b ON b.id = a.idcontratos
    LEFT JOIN contratos as c on c.id = b.idcontratos
    LEFT JOIN usulatie_bbdd.clientes AS d on d.id = c.idclientes
    LEFT JOIN usulatie_bbdd.tipo_contratos AS e ON c.id_tipo_contrato = e.id
    WHERE b.idcontratos in {lid} AND b.estado != 2;'''
    
    cuotas_acuerdos = pd.read_sql(q2, db)
    cuotas_acuerdos['num_renglon'] = cuotas_acuerdos.groupby(['idcontratos_acuerdos']).cumcount()+1
    
    print('Traemos la información de la venta del vehículo')
    q3 = f'''SELECT b.idclientes, a.idcontratos, NULL as idcontratos_acuerdos, NULL as fechabanco, NULL as importe, NULL as comisionapertura, NULL as Amount_wo_of, NULL as numcuotas, a.fechapagoefectivo as fecha, NULL as numdelacuota, '0' as estado,
    a.fechapagoefectivo as fechapago, a.fechapagoefectivo as fechapagoefectiva, IFNULL(a.importe, 0) - IFNULL(a.devolucion_exceso_venta, 0) as importecuota, 'Payment' as "Transaction Type", IFNULL(a.importe, 0) - IFNULL(a.devolucion_exceso_venta, 0) as importepago, 0 as cuotainteres, 0 as cuota_intereses_iva, 0  as cuotacapital,  
    NULL AS "Pending Origination Fee", NULL as capitalpendiente, 0 as cuotaservicios, 0 as cuotaserviciosiva, 0 as cuotaseguro, 0 as cuotasegurocapital, 0 as gastoapertura, 0 as cuota_gastoapertura_iva, 0 as pago_pendiente, 0 as pagocancelacion, 2 as acuerdo, NULL as principal_recov_1, NULL as "Late fee & Prepayment fee", NULL as "Late fee & Prepayment fee VAT", 0 as Days, NULL as num_renglon,
    FLOOR(DATEDIFF('{cierre}', def_fechanacimiento)/360 ) as "Age of customer", sueldo_neto as "Salary of Customer", "Individual" as "Prodcut Type", coche_tasacion as "Collateral Amount", b.importe/coche_tasacion as "Initial LTV", 0 as plusfee, b.fechafin as 'Maturity Date',  d.tipo_interes/100 AS tipo_interes
    FROM usulatie_bbdd.coches_vendidos AS a
    LEFT JOIN usulatie_bbdd.contratos AS b ON b.id = a.idcontratos
    LEFT JOIN usulatie_bbdd.clientes AS c on c.id = b.idclientes
    LEFT JOIN usulatie_bbdd.tipo_contratos AS d ON b.id_tipo_contrato = d.id
    WHERE a.idcontratos in {lid};'''
    cuotas_venta = pd.read_sql(q3, db)
    
    print('Traemos la información de los cambios de estados de cada contrato')
    q4 = f'''SELECT c.id_contrato AS idcontratos, EXTRACT(YEAR_MONTH FROM c.fecha) as ym_status, d.statusEN, if(c.nuevo_estadov2 = 'C33', 'Extension', NULL) AS Extension
    FROM usulatie_bbdd.logs_contratos AS c
    LEFT JOIN usulatie_bbdd.contratosestados as d on c.nuevo_estadov2 = d.codigo_estado
    WHERE c.id_contrato in {lid}
    ORDER BY c.id_contrato, c.fecha_completa;'''
    estados1 = pd.read_sql(q4, db)
    
    print('Traemos la información de los RIM')
    q5 = f'''SELECT id AS idcontratos, EXTRACT(YEAR_MONTH FROM fechafinreal) as ym_status, 'Amortized' as statusEN, NULL AS Extension
    FROM usulatie_bbdd.contratos
    WHERE id in {lid} AND fechafinreal <= {fecha_corte};'''
    estados2 = pd.read_sql(q5, db)
    
    estados = pd.concat([estados1, estados2])
    estados.reset_index(drop=True, inplace=True)
    estados['Ocurrence'] = estados.groupby(['idcontratos', 'statusEN']).cumcount()+1
    
    estados.sort_values(by =['idcontratos', 'ym_status'], ascending=True, inplace=True)
    
    #Agrupamos por contrato y yearmonth y nos quedamos sólo con el último registro
    estados = estados.groupby(['idcontratos', 'ym_status']).last().reset_index()
    
    print('Concatenamos ambas tablas y ordenamos por contrato, fecha de pago y por id de acuerdos')
    #Concatenamos las tablas de pagos y ordenamos los valores por fecha de ocurrencia
    pagos = pd.concat([cuotas, cuotas_acuerdos], ignore_index = True)
           
    pagos.sort_values(by=["idcontratos", "fecha", "idcontratos_acuerdos"], ignore_index=True, inplace = True)
    
    #Resetamos los índices para tener una llave única para localizar los renglones, dadoq ue se usó un concat, el índice 
    #pagos.reset_index(inplace = True, drop = True)
    pagos.reset_index(inplace = True)
    
    #Filtramos los renglones de la info de los usuarios que nunca tuvieron acuerdos
    good_u = pagos.groupby('idcontratos')['acuerdo'].sum().reset_index()
    good_u = good_u[good_u['acuerdo'] == 0]['idcontratos'].to_list()
    
    loan_t1 = pagos[pagos['idcontratos'].isin(good_u)]
    
    print('Obteniendo la ventana de tiempo en donde es válido cada contrato')
    #Vamos a sacar los renglones que tienen el indice mínimo y máximo sólo para los usuarios que tienen acuerdos
    diccionario1 = pagos[~pagos['idcontratos'].isin(good_u)].groupby(['idcontratos', 'idcontratos_acuerdos']).agg(index_max = ('index', np.max), index_min = ('index', np.min)).reset_index()
    diccionario1['num_row'] = diccionario1.groupby(['idcontratos']).cumcount()+1
    
    #Contamos, en total, cuantos acuerdos tiene cada contrato y hacemos un merge con el conteo de renglones anterior
    diccionario2 = diccionario1.groupby(['idcontratos'])['idcontratos_acuerdos'].count().reset_index().rename(columns = {'idcontratos_acuerdos': 'count'})
    
    diccionario2_1 = pd.merge(diccionario1, diccionario2, on = 'idcontratos', how = 'left')
    
    #Además, sacamos los contratos que, en lugar de registrar los pagos al contrato más nuevo, lo pueden adjudicar a otro disitinto
    diccionario3 = pagos[(~pagos['idcontratos'].isin(good_u)) & (~pagos['fechapagoefectiva'].isna())].groupby(['idcontratos', 'idcontratos_acuerdos']).agg(efectiva_max = ('index', np.max)).reset_index()
    
    diccionario = pd.merge(diccionario2_1, diccionario3, on = ['idcontratos', 'idcontratos_acuerdos'], how = 'left')
    diccionario.fillna(0, inplace = True)
    
    #Crearemos un ciclo for para ir renglón por renglón y concatenar mediante un loc, esto con el fin de quedarnos sólo 
    #con el contracto activo hasta que llegué un acuerdo nuevo que lo sustituya, esto nos dará el acuerdo/contrato vigente
    #en cada ventana de tiempo específica
    loan_t2 = pd.DataFrame()
    
    # Esta sección se comenta debido a que todos los contratos están en su respectiva ventana de tiempo
    print('Nos quedamos con los contratos que tienen pagos en cada ventana de tiempo respectiva')
    for c, i, l, k in list(zip(diccionario['idcontratos'], diccionario['num_row'], diccionario['count'], diccionario['idcontratos_acuerdos'])):
        list_min = diccionario[diccionario['idcontratos'] == c]['index_min'].to_list()
        list_max = diccionario[diccionario['idcontratos'] == c]['index_max'].to_list()
        list_max_efectiva = diccionario[diccionario['idcontratos'] == c]['efectiva_max'].to_list()
        
        if i != l:
            if  list_max_efectiva[i-1] < list_min[i-1+1]:
                guarda_1 = None
                hold = pagos[(pagos['idcontratos'] == c) & (pagos['idcontratos_acuerdos'] == k)]
                hold = hold[(list_min[i-1] <= hold['index']) & (hold['index'] <= list_min[i-1+1])]
                loan_t2 = pd.concat([loan_t2, hold])
            else:
                if guarda_1 == None:
                    hold = pagos[(pagos['idcontratos'] == c) & (pagos['idcontratos_acuerdos'] == k)]
                    hold = hold[(list_min[i-1] <= hold['index']) & (hold['index'] <= list_max_efectiva[i-1])]
                    guarda_1 = list_max_efectiva[i-1]
                    loan_t2 = pd.concat([loan_t2, hold]) 
                    print(1,c,k)
                else:
                    hold = pagos[(pagos['idcontratos'] == c) & (pagos['idcontratos_acuerdos'] == k)]
                    hold = hold[((guarda_1+1) <= hold['index']) & (hold['index'] <= list_min[i-1+1])]
                    guarda_1 = list_max_efectiva[i-1]
                    loan_t2 = pd.concat([loan_t2, hold]) 
                    print(2,c,k)
                          
        else:
            if  list_max_efectiva[i-1-1] < list_min[i-1]:
                guarda_1 = None
                hold = pagos[(pagos['idcontratos'] == c) & (pagos['idcontratos_acuerdos'] == k)]
                hold = hold[(list_min[i-1] <= hold['index']) & (hold['index'] <= list_max[i-1])]
                loan_t2 = pd.concat([loan_t2, hold])
            else:
                hold = pagos[(pagos['idcontratos'] == c) & (pagos['idcontratos_acuerdos'] == k)]
                hold = hold[(list_max_efectiva[i-1-1] <= hold['index']) & (hold['index'] <= list_max[i-1])]
                loan_t2 = pd.concat([loan_t2, hold])
                print(3,c,k) 

    print('Uniendo la información de la venta del carro al final de cada contrato respectivo')
    #Vamos a unir ambos tipos de usuarios, los corrientes y los que tuvimos que seccionar
    loan_tape_nocarsold = pd.concat([loan_t1, loan_t2])
    #Concatenamos la información de la venta de carros y ordenamos para que la venta del vehículo quede hasta el final
    loan_tape = pd.concat([loan_tape_nocarsold, cuotas_venta])
    loan_tape.sort_values(by=["idcontratos", 'acuerdo', "fecha"], ignore_index=True, inplace = True)
    
    #Rellenamos los valores que nos faltaron por el de la venta de vehículo
    cols = ['idcontratos_acuerdos', 'fechabanco', 'importe', 'comisionapertura', 'Amount_wo_of', 'numcuotas', 
            'numdelacuota', 'capitalpendiente', 'principal_recov_1', 'Late fee & Prepayment fee', 'Late fee & Prepayment fee VAT', 'num_renglon', 'Pending Origination Fee']
    for i in cols:
        loan_tape[i] = loan_tape.groupby('idcontratos')[i].transform(lambda v: v.ffill())
    
    print('Obteniendo valores de year_month de las diferentes fechas que tenemos en la tabla')
    idx_cs = loan_tape[(loan_tape['index'].isna())].index.to_list()
    
    #Actualizamos el valor de importecuota de estos renglones con el valor de capitalpendiente
    loan_tape.loc[idx_cs, 'importecuota'] = loan_tape.loc[idx_cs, 'capitalpendiente']
    
    #Vamos a restar el importepago de la venta del vehículo al valor capitalpendiente y lo guardaremos en la columna de capitalpendiente
    #sólo a los renglones donde index sea vacío
    try:
        loan_tape.loc[idx_cs, 'capitalpendiente'] = np.vectorize(updatecapital)(loan_tape.loc[idx_cs, 'capitalpendiente'], loan_tape.loc[idx_cs, 'importepago'])
    except:
        print('No hay ventas de vehículo')
    
    #Vamos a suponer que el valor de la columna fecha, cuando se vende el automovil, será el mismo que la columna fechapagoefectiva
    loan_tape.loc[idx_cs, 'fecha'] = loan_tape.loc[idx_cs, 'fechapagoefectiva']
    
    #Ahora, crearemos una columna que se llame fehca_limite y otra llamada ym que será la llave para unir con el
    #arrange de fechas más adelante
    loan_tape['fecha_limite'] = np.vectorize(fechas)(loan_tape['fechapagoefectiva'], loan_tape['fecha'], loan_tape['estado'])
    loan_tape['ym_limite'] = np.vectorize(ym)(loan_tape['fecha_limite'])
    
    #Ahora, crearemos una columna ym para vectorizar con esta columna
    loan_tape['ym_efectiva'] = np.vectorize(ym)(loan_tape['fechapagoefectiva'])
    
    #Ahora, crearemos una columna que se llame ym_fecha
    loan_tape['ym_fecha'] = np.vectorize(ym)(loan_tape['fecha'])
    
    #También crearemos una columna que tenga el máximo valor de la columna fecha 
    aux_lt = loan_tape.groupby('idcontratos').agg( max_fecha = ('fecha', np.max)).reset_index()   
    loan_tape = pd.merge(loan_tape, aux_lt, on = 'idcontratos', how = 'left')
    
    #Vamos a crear una tabla guía donde para cada contrato tendremos desde la fechabanco hasta la fecha hoy
    rangos = loan_tape.groupby('idcontratos').agg({'fechabanco':'min'}).reset_index()
    rangos['max_date'] = fecha_corte
    tipos = rangos['fechabanco'].apply(type).value_counts()
    print(tipos)
    
    #Restamos un mes a la fecha banco para que aparezca en el explode
    rangos['fechabanco'] = rangos['fechabanco']- DateOffset(months=1) 
    #rangos['fechabanco'] = rangos['fechabanco'].apply(lambda x: x if int(x.day) == 1 else x - DateOffset(months=1) )
    
    #Creamos un array de fecha con pd.date_range y lo explotamos con explode
    rangos['array'] = rangos.apply(lambda x: pd.date_range(start=x['fechabanco'], end=x['max_date'], freq='MS'), axis=1)
    rangos = rangos.explode('array').reset_index(drop=True)
    
    #Sacamos el ym de la columna array, dropeamos fechabanco para que no se dupliquela columna cuando hagamos el merge
    rangos['ym'] = np.vectorize(ym)(rangos['array'])
    rangos.drop('fechabanco', axis = 1, inplace = True)
    
    #Ahora, uniremos los renglones de loan tape a  esta tabla guía. Dropeamos y creamos la columna índice
    loan_tape['ym'] = np.vectorize(union_fechas)(loan_tape['max_fecha'], loan_tape['fecha_limite'], loan_tape['fecha'], loan_tape['ym_fecha'], loan_tape['ym_limite'])
    loan_tape.drop(['index'], axis = 1, inplace = True)
    loan_tape.reset_index(inplace = True)
    
    loan_tape_copy = loan_tape.copy()
    ##########################################################################################
    ######################loan_tape_copy.to_excel('loan_tape_copy.xlsx')######################
    ##########################################################################################
    #Ponemos todos los valores de 'Late fee & Prepayment fee' igual a cero si y solamente sí importepago == 0
    loan_tape_copy.loc[loan_tape_copy[loan_tape_copy['importepago']==0].index, 'Late fee & Prepayment fee'] =  0
    
    if geo == 'MX': 
        #Cuadramos el IVA del interes normal
        loan_tape_copy['cuota_intereses_iva'] = (iva_applicated*loan_tape_copy['plusfee']/(1+iva_applicated) + loan_tape_copy['cuota_intereses_iva']).round(2)
        loan_tape_copy['plusfee'] = (loan_tape_copy['plusfee']/(1+iva_applicated)).round(2)
        
        #Cuadramos el IVA del interes por moratorio
        loan_tape_copy['Late fee & Prepayment fee VAT'] = (iva_applicated*loan_tape_copy['Late fee & Prepayment fee']/(1+iva_applicated)).round(2)
        loan_tape_copy['Late fee & Prepayment fee'] = (loan_tape_copy['Late fee & Prepayment fee']/(1+iva_applicated)).round(2)
    else:
        pass
    
    #################################
    #loan_tape_copy.to_excel('test_loan_tape_copy.xlsx')
    #################################

    loan_tape_union = pd.merge(rangos, loan_tape, on = ['idcontratos', 'ym'], how = 'left')
    loan_tape_union.drop('max_fecha', axis = 1, inplace = True)

    print('Uniendo cada contrato con su cambio de estado por ym')
    #Uniremos la tabla loan_tape_union con estados para pegarla por la izquierda 
    loan_tape_union = pd.merge(loan_tape_union, estados, left_on = ['idcontratos', 'ym'], right_on = ['idcontratos', 'ym_status'], how = 'left')    
    loan_tape_union.drop(['index'], axis = 1, inplace = True)
    loan_tape_union.reset_index(inplace = True)
    
#############################################
    print('Cambiando los estados por lógica del negocio')
    #Vamos a setear algunas reglas por defecto del negocio
    #1. Todos los usuarios con idcontratos_acuerdos igual a -1 y estado 1, debe ir el valor pl
    pl_idx1 = loan_tape_union[(loan_tape_union['idcontratos_acuerdos'] == -1) & (loan_tape_union['estado'] == '1') & (loan_tape_union['statusEN'].isna()) & (loan_tape_union['ym_efectiva'] == loan_tape_union['ym_fecha'])].index
    loan_tape_union.loc[pl_idx1, 'statusEN'] = 'PL'

    #2. Todos los usuarios con estado 5 debe ir el valor Amortized
    amz_idx1 = loan_tape_union[(loan_tape_union['estado'] == '5') & (loan_tape_union['statusEN'].isna()) & (loan_tape_union['ym_efectiva'] <= loan_tape_union['ym_fecha'])].index
    loan_tape_union.loc[amz_idx1, 'statusEN'] = 'Amortized'

    #3. Todos los usuarios cuya último valor tenga estado = 1 debe ser Amortized. Sacamos el valor máximo de cada renglon
    max_idx = loan_tape_union[~loan_tape_union['ym_fecha'].isna()].groupby('idcontratos')['index'].max().reset_index()  
    hold_amz = loan_tape_union[~loan_tape_union['ym_efectiva'].isna()].groupby('idcontratos')['index'].max().reset_index()
    hold_amz = pd.merge(hold_amz, max_idx, left_on = ['idcontratos', 'index'], right_on = ['idcontratos', 'index'], how = 'inner')
    
    last_index = loan_tape_union.groupby('idcontratos')['index'].max().reset_index()
    amz_idx2 = loan_tape_union[(loan_tape_union['index'].isin(last_index['index'].to_list())) & (loan_tape_union['index'].isin(hold_amz['index'].to_list())) & (loan_tape_union['estado'] == '1') & (loan_tape_union['statusEN'] == 'PL') & (loan_tape_union['capitalpendiente'] == 0) & (loan_tape_union['importepago'] != 0)].index
    loan_tape_union.loc[amz_idx2, 'statusEN'] = 'Amortized'
    
    #4.-Actualizamos los valores donde principal_outstanding sea 0, statusEN no sea Amortized y haya pagado todas sus cuotas, para ello vamos a hacer un query
    #en el cual nos traiga todos los contratos que han pagado todas sus cuotas para convertir los ids a lista y usar un isin
    q6 = f'''SELECT c.id AS idcontratos, MAX(a.numdelacuota) AS maxima_cuota, IFNULL(MAX(if(a.fechapagoefectiva IS NOT NULL and a.fechapagoefectiva <= {fecha_corte}, a.numdelacuota, NULL)), 0) AS maxima_cuota_pagada, b.numcuotas 
    FROM usulatie_bbdd.cuotas_acuerdos AS a  
    INNER JOIN usulatie_bbdd.contratos_acuerdos AS b ON b.id = a.idcontratos
    LEFT JOIN usulatie_bbdd.contratos as c on c.id = b.idcontratos
    WHERE b.estado != 2
    GROUP BY c.id, a.idcontratos
    HAVING maxima_cuota = maxima_cuota_pagada
    UNION
    SELECT b.id AS idcontratos, MAX(a.numdelacuota) AS maxima_cuota, IFNULL(MAX(if(a.fechapagoefectiva IS NOT NULL and a.fechapagoefectiva <= {fecha_corte}, a.numdelacuota, NULL)), 0) AS maxima_cuota_pagada, b.numcuotas 
    FROM usulatie_bbdd.cuotas AS a  
    LEFT JOIN usulatie_bbdd.contratos as b on b.id = a.idcontratos
    GROUP BY b.id
    HAVING maxima_cuota = maxima_cuota_pagada'''
    c_pagados = pd.read_sql(q6, db)
    id_c_pagados = c_pagados['idcontratos'].to_list()
        
    idx_2Amortized = loan_tape_union[(loan_tape_union['statusEN'] != 'Amortized') & (loan_tape_union['idcontratos'].isin(id_c_pagados))].index
    loan_tape_union.loc[idx_2Amortized, 'statusEN'] = 'Amortized'
    
    #5. Rellenamos los valores restantes con el anterior
    #loan_tape_union['statusEN'] = loan_tape_union.groupby(['idcontratos'])['statusEN'].ffill()
    loan_tape_union['statusEN'] = loan_tape_union.groupby('idcontratos')['statusEN'].transform(lambda v: v.ffill())
    
 #   #6. Buscamos los contratos que deben tener RPL en lugar de cpl basándonos con la tabla contratos_rpl. Primero sacaremos a aquellos usuarios que tiene
 #   #el valor de la columna coche_cambio_trafico como NULL y les pondremos RPL donde tengan CPL
 #   contratos_rpl1 = contratos_rpl[contratos_rpl['coche_cambio_trafico'].isna()]['idcontratos'].unique()    
 #   idx_rplp1 = loan_tape_union[(loan_tape_union['idcontratos'].isin(contratos_rpl1)) & (loan_tape_union['statusEN'].isin(['CPL']))].index
 #   loan_tape_union.loc[idx_rplp1, 'statusEN'] = 'RPL'
    
 #   #Sacaremos los valores de los contratos con coche_cambio_trafico distinta de NULL, haremos un merge con la tabla loan_tape_union y sacaremos aquellos
 #   #valores en donde coche_cambio_trafico sea menor estricto a ym_efectiva
 #   contratos_rpl2 = contratos_rpl[~contratos_rpl['coche_cambio_trafico'].isna()].copy()
 #   contratos_rpl2.rename(columns = {'coche_cambio_trafico':'aux_coche_cambio_trafico'}, inplace = True)  
 #   loan_tape_union = pd.merge(loan_tape_union, contratos_rpl2, on = 'idcontratos', how = 'left')
 #   idx_rplp2 = loan_tape_union[(loan_tape_union['idcontratos'].isin( contratos_rpl2['idcontratos'].unique() )) & (loan_tape_union['statusEN'].isin(['CPL'])) & (loan_tape_union['aux_coche_cambio_trafico'] < loan_tape_union['ym_efectiva'])].index
 #   loan_tape_union.loc[idx_rplp2, 'statusEN'] = 'RPL'
    
 #   #Dropeamos la columna que hicimos el merge
 #   loan_tape_union.drop('aux_coche_cambio_trafico', axis = 1, inplace = True)
#############################################
   
    #Sacaremos el verdadero capital pendiente de la suma de las cuotas, primero haremos la suma acumulada de principal_recov1
    #y se lo restaremos al importe prestado, si resulta negativa lo setearemos a cero
    loan_tape_union['principal_cumsum'] = loan_tape_union.groupby(['idcontratos', 'idcontratos_acuerdos'])['principal_recov_1'].transform(pd.Series.cumsum)

    print('Modificamos renglón por renglón la columna importe de acuerdo al valor del principal outstanding del contrato anterior')
    #Creamos una agenda donde guardaremos el idcontratos, idcontratos_acuerdo e index e iremos uno por uno
    #modificando el valor de importe sólo para los usuarios de acuerdo = 1. Usamos la variable good_u antes declarada
    agenda1 = loan_tape_union[(~loan_tape_union['idcontratos'].isin(good_u)) & (~loan_tape_union['estado'].isna())].groupby(['idcontratos', 'idcontratos_acuerdos'])['capitalpendiente'].last().reset_index().rename(columns = {'index': 'ultimo_pay'})

    #Ahora, enumeraremos el número de contratos/acuerdos que tiene cada idcontratos y sacamos el número total
    hold = agenda1.groupby(['idcontratos'])['idcontratos_acuerdos'].count().reset_index().rename(columns = {'idcontratos_acuerdos': 'count'})
    agenda1 = pd.merge(agenda1, hold, on = 'idcontratos', how = 'left')

    agenda2 = loan_tape_union.groupby(['idcontratos', 'idcontratos_acuerdos'])['index'].first().reset_index().rename(columns = {'index': 'primer_ren'})
    agenda = pd.merge(agenda1, agenda2, on = ['idcontratos', 'idcontratos_acuerdos'], how = 'left')
    agenda['num_row'] = agenda.groupby(['idcontratos']).cumcount()+1
    agenda['capitalpendiente'] = agenda['capitalpendiente'].shift(1)

    #Dropeamos el registro que tenga el valor de idcontratos_acuerdos == -1
    agenda.drop(agenda[agenda['idcontratos_acuerdos'] == -1].index, axis = 'index', inplace = True)

    #Ahora, vamos a cambiar el valor de la columna importe de todos los acuerdos
    for i, j, k in list(zip(agenda['capitalpendiente'], agenda['idcontratos'], agenda['idcontratos_acuerdos'])):
        indices = loan_tape_union[(loan_tape_union['idcontratos'] == j) & (loan_tape_union['idcontratos_acuerdos'] == k)].index.to_list()
        loan_tape_union.loc[indices, 'importe'] = i

    print('Calculamos el principal outstanding de todos los contratos, los que van al corriente, de los que se adelantan o atrasan')
    loan_tape_union['principal_recov'] = loan_tape_union['importe'] - loan_tape_union['principal_cumsum']
    loan_tape_union['principal_recov'] = loan_tape_union['principal_recov'].apply(lambda x: x if x > 0 else 0)

    #Vamos a rellenar los valores de capitalpendiente, idcontratos_acuerdos, fechabanco, importe, acuerdo, principal_cumsum
    #nulos con el anterior valor mediante la función .ffil
    cols = ['idclientes', 'idcontratos_acuerdos', 'fechabanco', 'importe', 'comisionapertura', 'Amount_wo_of', 'numcuotas', 
                'Age of customer', 'Salary of Customer', 'Prodcut Type', 'Collateral Amount', 'Initial LTV', 'nombrepq']
    for i in cols:
        loan_tape_union[i] = loan_tape_union.groupby('idcontratos')[i].transform(lambda v: v.ffill())

    #Los importecuota e importepago cuyos índices sean nulos con ceros
    loan_tape_union.loc[loan_tape_union[loan_tape_union['index'].isna()].index.to_list(), ['importecuota', 'importepago']] = 0

    #Ordenamos por ['idcontratos', 'fecha', 'capitalpendiente'] ascending = [True, True, False]
    loan_tape_union.ym_fecha = pd.to_numeric(loan_tape_union.ym_fecha, errors='coerce')
    loan_tape_union = loan_tape_union.sort_values(by=['idcontratos', 'ym_fecha', 'capitalpendiente'], ascending = [True, True, False], ignore_index=True)

    #Vamos a traer el índice de los valores que tienen la columna fecha distintio de nulo, ya que a ellos les aplicaremos vectorize
    fecha_null_idx = loan_tape_union[~loan_tape_union['fecha'].isna()].index.to_list()

    loan_tape_union.loc[fecha_null_idx, 'principal_outstanding'] = np.vectorize(bandera)(loan_tape_union.loc[fecha_null_idx, 'ym_efectiva'], loan_tape_union.loc[fecha_null_idx, 'ym_fecha'], loan_tape_union.loc[fecha_null_idx, 'capitalpendiente'], np.nan)
    loan_tape_union.loc[fecha_null_idx, 'principal_days'] = np.vectorize(banderadias)(loan_tape_union.loc[fecha_null_idx, 'ym_efectiva'], loan_tape_union.loc[fecha_null_idx, 'ym_fecha'], loan_tape_union.loc[fecha_null_idx, 'Days'], np.nan)

    #El idcontratos 1427 paga en meses previos sus cuotas, por ejemplo, debe pagar en enero, pero paga en diciembre.
    #Entonces, al cortede diciembre debería estar la cuota que se pagó en diciembre pero que debía ser pagada en enero y estos
    #casos no los ve el bloque de código anterior. Para esto, después de que la tabal sea llenada, vamos a hacer lo siguiente
    
    #Traemos todos los registros donde ym_efectiva < ym_fecha. Sacamos los índices donde fechapagoefectiva no sea nula
    efectiva_nonull = loan_tape[ (~loan_tape['fechapagoefectiva'].isnull()) ].index.to_list()
    pagos_previos = loan_tape.iloc[efectiva_nonull].copy()

    #Para asegurarnos que nos quedaremos con el último registro en caso de que en un mismo year_month haya cubierto más de
    #una cuota posterior y de los renglones que ym_efectiva sea menor a ym_fecha.
    pagos_previos = pagos_previos[(pagos_previos['ym_efectiva'].astype('float') <= pagos_previos['ym_fecha'].astype('float')) ]
    pagos_previos.sort_values(by=['idcontratos', 'fecha'], ignore_index=True, inplace = True)

    #Sacaremos el mínimo valor de la columna principal_outstanding agrupando por 'idcontratos' y 'ym_efectiva'.
    pagos_previos = pagos_previos.groupby(['idcontratos', 'ym_efectiva']).agg(capitalpendiente = ('capitalpendiente', np.min)).reset_index()
    pagos_previos = pagos_previos[['idcontratos', 'ym_efectiva', 'capitalpendiente']].rename(columns = {'capitalpendiente':'prin_out_prev', 'ym_efectiva':'ym_fecha'})
    
    #Hacemos un merge con la tabla loan_tape_union, sacamos los índices donde la columna prin_out_prev no es nula y 
    #hacemos un update de los valores uniendo primero por ym_efectiva
    try:
        loan_tape_union.drop('prin_out_prev', axis = 1, inplace = True)
    except:
        pass
    
    loan_tape_union = pd.merge(loan_tape_union, pagos_previos, on = ['idcontratos', 'ym_fecha'], how = 'left')

    idx_principal = loan_tape_union[ (~loan_tape_union['prin_out_prev'].isna()) & (~loan_tape_union['ym_fecha'].isna()) & (loan_tape_union['ym_efectiva'] <= loan_tape_union['ym_fecha']) ].index.to_list()
    loan_tape_union.loc[idx_principal, 'principal_outstanding'] = loan_tape_union.loc[idx_principal, 'prin_out_prev']
    loan_tape_union.drop('prin_out_prev', axis = 1, inplace = True)

    #Hacemos exactamente lo mismo de antes, sólo que ahora unimos por ym_efectiva con ym, en caso de que el usuario no tenga
    #pagos en los siguientes meses
    pagos_previos.rename(columns = {'ym_fecha':'ym'}, inplace = True)
    #pagos_previos.drop('index', axis = 1, inplace = True)
    #Hacemos un merge con la tabla loan_tape_union, sacamos los índices donde la columna prin_out_prev no es nula y 
    #hacemos un update de los valores uniendo primero por ym
    loan_tape_union = pd.merge(loan_tape_union, pagos_previos, on = ['idcontratos', 'ym'], how = 'left')
    idx_principal = loan_tape_union[(~loan_tape_union['prin_out_prev'].isna()) & (loan_tape_union['ym_fecha'].isna()) & (loan_tape_union['principal_outstanding'].isna())].index.to_list()
    loan_tape_union.loc[idx_principal, 'principal_outstanding'] = loan_tape_union.loc[idx_principal, 'prin_out_prev']
    loan_tape_union.drop('prin_out_prev', axis = 1, inplace = True)

    #Sacaremos una lógica para los valores nulos donde las cuotas se atrasan, es decir, un usuario cuyo pago era en septiembre
    #pero pagó en Octubre, con la función de arriba arroja el valor col_valno. Para el cierre de septiembre, este debería sel el
    #capital_pendiente anterior (Agosto), pero para el corte de Octubre el capital pendiente debe ser el de Septiembre, porque este
    #usuario sí pago septiembre, pero hasta Octubre, mes donde estamos haciendo el corte

    #Sacaremos el valor de los índices que tienen fechapagoefectiva distinto de nulo y principal_outstanding nulo
    prin_outs_null1 = loan_tape[(~loan_tape['fechapagoefectiva'].isna()) & (loan_tape['ym_fecha'] < loan_tape['ym_efectiva'])][['idcontratos', 'ym_efectiva', 'capitalpendiente']]
    prin_outs_null1.sort_values(['idcontratos', 'ym_efectiva'], inplace = True)
    prin_outs_null1 = prin_outs_null1.groupby(['idcontratos', 'ym_efectiva']).last().reset_index()

    #El caso de arriba sólo funciona sí y solamente sí hay una cuota por mes. Sin embargo, si el usuario tiene más de una cuota y 
    #al menos una está pagada, por tanto, sólo con las otras sin pagar hace el merge. Por eso, haremos una tabla correctiva
    prin_outs_correct = loan_tape_union[(~loan_tape_union['fechapagoefectiva'].isna()) & (~loan_tape_union['principal_outstanding'].isna())][['idcontratos', 'ym_efectiva', 'capitalpendiente']]
    prin_outs_correct.sort_values(['idcontratos', 'ym_efectiva'], inplace = True)
    prin_outs_correct = prin_outs_correct.groupby(['idcontratos', 'ym_efectiva'])['capitalpendiente'].sum().reset_index().rename(columns = {'capitalpendiente':'capitalpendiente_correct'})

    #La columna de capitalpendiente_correct será la llave de que nos indique si pasa el caso descrito anteriormente
    prin_outs_null = pd.merge(prin_outs_null1, prin_outs_correct, on = ['idcontratos', 'ym_efectiva'], how = 'left')
    prin_outs_null.drop(prin_outs_null[~prin_outs_null['capitalpendiente_correct'].isna()].index, axis = 'index', inplace = True)
    
    #Cambiamos el nombre de las columnas para poder hacer un join  (Se cambia ym_fecha por ym)
    prin_outs_null.rename(columns = {'capitalpendiente': 'capitalpendiente_aux', 'ym_efectiva': 'ym'}, inplace = True)
    #prin_outs_null.drop('index', axis = 1, inplace = True)
    loan_tape_union = pd.merge(loan_tape_union, prin_outs_null, on = ['idcontratos', 'ym'], how = 'left') 

    #Sacamos los índices de la columna capitalpendiente_aux no nulos, aplicamos loc principal_outstanding y dropeamos capitalpendiente_aux
    loan_tape_union_index = loan_tape_union[ (~loan_tape_union['capitalpendiente_aux'].isna()) & (loan_tape_union['principal_outstanding'].isna()) ].index.to_list()
    loan_tape_union.loc[loan_tape_union_index, 'principal_outstanding'] = loan_tape_union.loc[loan_tape_union_index, 'capitalpendiente_aux']
    loan_tape_union.drop(['capitalpendiente_aux', 'capitalpendiente_correct'], axis = 1, inplace = True)

    loan_tape_union.sort_values(by=["idcontratos", "array", "index"], ignore_index=False, inplace = True)
    
    #Llenaremos los valores faltantes mediante un .ffill agrupado por idcontratos
    loan_tape_union["principal_outstanding"] = loan_tape_union.groupby('idcontratos')['principal_outstanding'].transform(lambda v: v.ffill())
    
    print('Hacemos el update sobre los renglones de carsold')
    #Debemos pasar la columna de principal outstanding del renglón i-1 a la columna cuotacapital del renglón i
    #Además, importecuota menos cuotacapital en la columna Late fee
    car_sold_idx = loan_tape_union[(loan_tape_union['acuerdo'] == 2) & (loan_tape_union['importepago'] > 0)].groupby('idcontratos').agg({'index':'min', 'principal_outstanding':'max'}).reset_index()
    cs_idx = loan_tape_union[loan_tape_union['index'].isin(car_sold_idx['index'].to_list())].index
    loan_tape_union.loc[cs_idx, 'Late fee & Prepayment fee'] = (loan_tape_union.loc[cs_idx, 'importepago'].fillna(0)).values - (loan_tape_union.loc[list(np.array(cs_idx) - 1), 'principal_outstanding'].fillna(0)).values 
    loan_tape_union.loc[cs_idx, 'Late fee & Prepayment fee']  = loan_tape_union.loc[cs_idx, 'Late fee & Prepayment fee'].apply(lambda x: x if x >= 0 else 0)
    
    #Dropeamos los renglones fantasmas
    loan_tape_union.drop(loan_tape_union[loan_tape_union['principal_outstanding'].isna()].index, axis = 0, inplace = True)
    
    #loan_tape_union.to_excel('before_loan_tape.xlsx', index=False)
    #Finalmente, agrupamos por idcontratos, ym_fecha
    final = loan_tape_union.groupby(['idcontratos', 'ym']).last().reset_index()
    final['period'] = final.groupby(['idcontratos']).cumcount()

    #Para eliminar el caso en que haya más registros en el reporte de excel que de la info de la base de datos, se realizará
    # un .ffill para principal_outstanding y se updeiteara la columna array con base en min_date
    final['principal_outstanding'] = final.groupby(['idcontratos'])['principal_outstanding'].ffill()
    
    #Dado que existe el estado RIM, donde si fechafinreal es menor o igual a curdate() el usuario ha amortizado, pero no aparece en la tabla de cuotas
    #Pondremos en 0 todos los valores que tengan amortizado y principal outstanding distinto de cero
    idx_out0 = final[(final['statusEN'].isin( ['Amortized', 'CS'])) & (final['principal_outstanding'] != 0)].index
    final.loc[idx_out0, 'principal_outstanding'] = 0

    #Para asegurarnos que el programa está leyendo bien la información, haremos la siguiente transformación de datos
    final['ym'] = final['ym'].astype('Int64')
    
    ##Vamos a asegurarnos que todo renglón sea mayor o igual al próximo renglón, para eso corremos un valor hacia atrás la columna
    #principal_outstanding y rellenamos el último, que será nulo (por la naturaleza del shift) con el valor de principal_outstanding con .loc
    final['next_principal_outstanding'] = final.groupby(['idcontratos'])['principal_outstanding'].shift(1)
    idx_shift_po = final[final['next_principal_outstanding'].isna()].index
    final.loc[idx_shift_po, 'next_principal_outstanding'] = final.loc[idx_shift_po, 'principal_outstanding']
        
    #Ponemos una bandera para comparar el principal_outstanding actual con el del siguiente renglón y este debe ser menor o igual
    #df['menor_igual_bandera'] = df.groupby('idcontratos').apply(lambda x: check_principal(df['principal_outstanding'], df['next_principal_outstanding']) )
    final = final.groupby('idcontratos').apply(check_principal).reset_index(drop=True)
    
    #Hacemos la corrección para que sólo nos quedemos con los valores que hagan que cumplan que el renglón actual
    #debe ser mayor que el siguiente
    idx_actual_mayor = final[final['menor_igual_bandera'] > 0].index
    final.loc[idx_actual_mayor, 'principal_outstanding'] = final.loc[idx_actual_mayor, 'next_principal_outstanding']
    
    final.drop(['next_principal_outstanding', 'menor_igual_bandera'], inplace = True, axis = 1)
    
    #Hacemos un último check de estados donde, si el principal_outstanding == 0 y statusEN == 'PL' cambiar esos valores a 'Amortized'
    idx_pl2Amor = final[(final['principal_outstanding'] == 0) & (final['statusEN'] == 'PL')].index
    final.loc[idx_pl2Amor, 'statusEN'] = 'Amortized'
    
    #Actualizamos las etiquetas a como aparecen en el reporte de checks para que podamos cuadrar estados
    final['nuevo_estado'] = np.vectorize(cambio_etiquetas)(final['statusEN'])
    
    if geo == 'MX': 
        #Cuadramos el IVA del interes normal
        final['cuota_intereses_iva'] = (iva_applicated*final['plusfee']/(1+iva_applicated) + final['cuota_intereses_iva']).round(2)
        final['plusfee'] = (final['plusfee']/(1+iva_applicated)).round(2)
        
        #Cuadramos el IVA del interes por moratorio
        final['Late fee & Prepayment fee VAT'] = (iva_applicated*final['Late fee & Prepayment fee']/(1+iva_applicated)).round(2)
        final['Late fee & Prepayment fee'] = (final['Late fee & Prepayment fee']/(1+iva_applicated)).round(2)
    else:
        pass

    #Sacamos la lista de las fechas a donde tenemos que hacer el corte
    asign = pd.DataFrame(index= ['Current', '1-30d', '31-60d', '61-90d', '91-120d','121-150d', '151-180d', '>180d', 'Total', 'New loans', 'Write offs', 'Amz', 'CS'])
        
    rest = [   '2017-1-31', '2017-2-28', '2017-3-31', '2017-4-30', '2017-5-31', '2017-6-30', '2017-7-31', '2017-8-31', 
               '2017-9-30', '2017-10-31', '2017-11-30', '2017-12-31', '2018-1-31', '2018-2-28', '2018-3-31', '2018-4-30',
               '2018-5-31', '2018-6-30', '2018-7-31', '2018-8-31', '2018-9-30', '2018-10-31', '2018-11-30', '2018-12-31',
               '2019-1-31', '2019-2-28', '2019-3-31', '2019-4-30', '2019-5-31', '2019-6-30', '2019-7-31', '2019-8-31', 
               '2019-9-30', '2019-10-31', '2019-11-30', '2019-12-31', '2020-1-31', '2020-2-29', '2020-3-31', '2020-4-30',
               '2020-5-31', '2020-6-30', '2020-7-31', '2020-8-31', '2020-9-30', '2020-10-31', '2020-11-30', '2020-12-31',
               '2021-1-31', '2021-2-28', '2021-3-31', '2021-4-30', '2021-5-31', '2021-6-30', '2021-7-31', '2021-8-31', 
               '2021-9-30', '2021-10-31', '2021-11-30', '2021-12-31', '2022-1-31', '2022-2-28', '2022-3-31', '2022-4-30',
               '2022-5-31', '2022-6-30', '2022-7-31', '2022-8-31', '2022-9-30', '2022-10-31', '2022-11-30', '2022-12-31',
               '2023-1-31', '2023-2-28', '2023-3-31', '2023-4-30', '2023-5-31', '2023-6-30', '2023-7-31', '2023-8-31', 
               '2023-9-30', '2023-10-31', '2023-11-30', '2023-12-31', '2024-1-31', '2024-2-29', '2024-3-31', '2024-4-30',
               '2024-5-31', '2024-6-30',  '2024-7-31', '2024-8-31', '2024-9-30', '2024-10-31', '2024-11-30', '2024-12-31',
               '2025-1-31', '2025-2-28', '2025-3-31', '2025-4-30', '2025-5-31', '2025-6-30',  '2025-7-31', '2025-8-31',
               '2025-9-30', '2025-10-31', '2025-11-30', '2025-12-31']
        
    ym_hh = [  201701, 201702, 201703, 201704, 201705, 201706, 201707, 201708, 
               201709, 201710, 201711, 201712, 201801, 201802, 201803, 201804,
               201805, 201806, 201807, 201808, 201809, 201810, 201811, 201812,
               201901, 201902, 201903, 201904, 201905, 201906, 201907, 201908, 
               201909, 201910, 201911, 201912, 202001, 202002, 202003, 202004,
               202005, 202006, 202007, 202008, 202009, 202010, 202011, 202012,
               202101, 202102, 202103, 202104, 202105, 202106, 202107, 202108, 
               202109, 202110, 202111, 202112, 202201, 202202, 202203, 202204,
               202205, 202206, 202207, 202208, 202209, 202210, 202211, 202212,
               202301, 202302, 202303, 202304, 202305, 202306, 202307, 202308, 
               202309, 202310, 202311, 202312, 202401, 202402, 202403, 202404, 
               202405, 202406, 202407, 202408, 202409, 202410, 202411, 202412,
               202501, 202502, 202503, 202504, 202505, 202506, 202507, 202508,
               202509, 202510, 202511, 202512]
        
    print('Comenzando la conciliación a cierre de cada mes')
        
    for k, l in list(zip(rest, ym_hh)):
        try:
            print(k, l)
            i = k
            i = datetime.datetime(int(i[:4]), int(i[5:i.find('-',5,8)]), int(i[i.find('-',5,8)+1:]) ).date()
            #Cortamos el dataframe hasta el ym de corte, ordenamos y reseteamos los índices
            hold0 = final[final['ym'] <=  l].copy()
            hold0.sort_values(by=["idcontratos", "array"], ignore_index=True, inplace = True)   
            try:
                hold0.drop('index', axis = 1, inplace = True)
                hold0.reset_index(inplace = True)
            except:
                hold0.reset_index(inplace = True)
            
            #Creamos una columna auxiliar para calcular a fecha de corte y calculamos la diferencia de días
            hold0['auxiliar'] = datetime.datetime.strptime(k, '%Y-%m-%d')
                
            #Como el excel tenía más datos de los que se tenían en la base de datos, los renglonees faltantes de la columna fecha
            #serán llenados con el valor de array
            ids = hold0[hold0['fecha'].isnull()].index
            hold0.loc[ids, 'fecha'] = hold0.loc[ids, 'array'] 
            hold0['fecha'] = hold0['fecha'].astype('datetime64[ns]')
                  
            #Colocaremos la etiqueta que debe de tener por diferencia de fechas
            #Sacaremos la eitqueta de los valores que tienen días (corrientes) y los que no tienen días (restarlos)
            currents = hold0[~hold0['principal_days'].isna()].index
            hold0.loc[currents, 'etiqueta'] = 'Current'
            ############hold0.to_excel('testeo_full.xlsx', index=False)   
            nulos = hold0[hold0['principal_days'].isna()].index
            print(hold0.shape)
                
            #Seteamos valores por default
            hold0.loc[nulos, 'principal_days'] = (hold0.loc[nulos, 'auxiliar'] - hold0.loc[nulos, 'fecha']).dt.days.astype('Int64')
            try:
                hold0.loc[nulos, 'etiqueta'] = np.vectorize(ventana)(hold0.loc[nulos, 'principal_days'])
                
            except:
                pass
                
            #Vamos a tomar el último registro de hold0 por idcontratos
            dt = hold0[hold0['ym'] == l].copy()
                
            #Calcularemos la suma de los valores en imp para posteriormente agregarlos
            impairment = dt[dt['statusEN'].isin(['Impairment', 'IMP'])]['principal_outstanding'].sum()
            cs = dt[dt['statusEN'].isin(['CS'])]['principal_outstanding'].sum()
            amz = dt[dt['statusEN'].isin(['Amortized'])]['principal_outstanding'].sum()
                
            #Dropearemos los estados finales CS, IMP, Amortized
            dt.drop(dt[dt['statusEN'].isin(['Amortized', 'Impairment', 'CS', 'IMP'])].index, axis = 'index', inplace = True)
                
            #Sacaremos a los usuarios que están corrientes
            good_id = dt[dt['etiqueta'] == "Current"].index
            f_loan_tape1 = dt.loc[good_id, :]
            print(f_loan_tape1.shape)
                
            #Ahora, sacaremos el último registro pagado de los que no estaban corriente. Para esto haremos casos
            bad_id = dt[dt['etiqueta'] != "Current"]['idcontratos'].to_list()
            #bad_id = dt[~dt.index.isin(good_id)]['Loan Id'].to_list()
                
            #Caso 1: Por defecto, el periodo debería ser fechabanco. Sin embargo, en el supuesto de que su primer cuota
            #fuera en el mismo ym del ym_fechabanco y no la haya pagado, en el momento de hacer el groupby y quedarnos con el 
            #último registro, este sería nulo y en teoría, si nunca nos pagan, jamás tendría renglones pagados. Sacamos estos usuarios
            id_all_null = hold0[(hold0['fechapagoefectiva'].isnull()) & (hold0['period'].isin([0]))]['idcontratos'].to_list()
            #Sacamos los índices que se usarán para posicionar la antigüedad de estos usuarios
            f_loan_tape2_idx1 = hold0[(hold0['idcontratos'].isin(id_all_null)) & (hold0['period'].isin([0]))].index
                
            #Caso 2: Los usuarios que tienen fechapagoefectiva distinta de nulo y el renglón donde obtenemos su último pago es
            #disitnto al renglón máximo, para esto agruparemos y sacaremos de la columna index el máximo y haremos un merge
            group_max_idx = hold0.groupby('idcontratos')['index'].max().reset_index().rename(columns = {'index': 'max_index'})
            hold0 = pd.merge(hold0, group_max_idx, on = ['idcontratos'], how = 'left')
            f_loan_tape2_idx2 = hold0[(hold0['idcontratos'].isin(bad_id)) & (~hold0['idcontratos'].isin(id_all_null)) & (~hold0['fechapagoefectiva'].isnull())].groupby("idcontratos").last().reset_index()
                
            #Caso 2_1: Sacaremos a los que les podremos sumar un renglón
            f_loan_tape2_idx2_1 = f_loan_tape2_idx2[f_loan_tape2_idx2['index'] != f_loan_tape2_idx2['max_index']]['index'].to_list()
            ar = np.array(f_loan_tape2_idx2_1)
            ar_index = ar + 1
            ar_index_1 = ar_index.tolist()
                
            #Caso 2_2: Sacaremos a los que NO les podremos sumar un renglón
            ar_index_2 = f_loan_tape2_idx2[f_loan_tape2_idx2['index'] == f_loan_tape2_idx2['max_index']]['index'].to_list()
                
            #Unimos las tres listas
            f_loan_tape2_idx = f_loan_tape2_idx1.tolist() + ar_index_1 + ar_index_2
                     
            #Sacaremos los renglones correspondientes de los usuarios que no estaban al corriente y haremos un merge con ambas tablas
            f_loan_tape2 = hold0.loc[f_loan_tape2_idx, :]
            f_loan_tape = pd.concat([f_loan_tape1, f_loan_tape2])
                
            #Ordenamos por fecha y nos quedamos con el último
            f_loan_tape.sort_values(by=["idcontratos", "fecha"], ignore_index=True, inplace = True)
            f_loan_tape.drop_duplicates(subset="idcontratos", keep='last', inplace=True)
                
            #Crearemos la tabla para ir guardando uno a uno
            df = f_loan_tape.groupby('etiqueta').agg({'principal_outstanding':'sum'}).rename(columns = {'principal_outstanding':str(i)})
            asign = pd.concat([asign, df], axis = 1)
            asign.loc['Total', str(i)] = asign.loc[['Current', '1-30d', '31-60d', '61-90d', '91-120d','121-150d', '151-180d', '>180d'], f'{i}'].sum()
            asign.loc['New loans', str(i)] = lstaid[lstaid['ym_fb'] == int(l)]['importe'].sum()    
            asign.loc['Write offs', str(i)] = impairment
            asign.loc['Amz', str(i)] = amz
            asign.loc['CS', str(i)] = cs
            print(l)
        except:
            print(f'No hay data para la originación del año {l}')
        
    asign.fillna(0, inplace = True)
    if geo == 'ESP':
        conexion_sheets('asign_stats_report', [hora_fecha, asign.reset_index()], [1, 4], [1, 1])
    else: 
        pass
    return final, loan_tape_copy

def vintage_pi(final, date, ym_date, inicio):
    #Traemos la conexión de la base de datos
    db = database(geo)
    
    #Nos posicionamos en la carpeta IUVO    
    os.chdir(r'C:\Users\Bi_analyst\Desktop\Python')   
    
    print('Iniciando la creación del reporte p2p Report')
    #Creamos el bloque de funciones que usaremos a lo largo de función principal 
    def ventana(x):
        if x < 6:
            etiqueta = 'Current'
        elif 6 <= x <= 30:
            etiqueta = '1-30d'
        elif 31 <= x <= 60:
            etiqueta = '31-60d'
        elif 61 <= x <= 90:
            etiqueta = '61-90d'
        elif 91 <= x <= 120:
            etiqueta = '91-120d'
        elif 121 <= x <= 150:
            etiqueta = '121-150d'
        elif 151 <= x <= 180:
            etiqueta = '151-180d'
        else:
            etiqueta = '>180d'
        return etiqueta
    
    def ym(fecha):
        try:
            a = float(str(fecha.year)+str(fecha.strftime("%m")))
            return a
        except:
            return np.nan
        
    #Sacamos el día y hora actual
    q0 = 'SELECT CURDATE(), TIME(NOW());'
    hora_fecha = pd.read_sql(q0, db)
    
    #Sacamos todas las fechasbanco de los usuarios y la info de sus cuotas
    #Sacamos cuantos usuarios por año mes se emitieron y se guardarán en una lista
    q1 =  f'''SELECT id as 'Loan Id', importe, fechabanco, fechabanco as fechabanco2, EXTRACT(YEAR_MONTH from fechabanco) as ym_fb
    FROM usulatie_bbdd.contratos
    WHERE idproductos IN {productos} AND fechabanco IS NOT NULL AND estado != 2 
    AND EXTRACT(YEAR_MONTH FROM fechabanco) >= {inicio} AND EXTRACT(YEAR_MONTH FROM fechabanco) <= {ym_date}
    AND id not in (322)'''
    lstaid = pd.read_sql(q1, db) 
    
    print('Se ha cargado la información de la tabla dek reporte Aging_stats')
    #Es importante que, para correr este reporte, se deba correr primero aging_astas module, ya que final es procesado
    #y limpiado en ese modulo
    
    #Creamos una tabla auxiliar donde agrupemos por idcontratos y saquemos fechabanco
    aux_banco = final.groupby('idcontratos')['fechabanco'].first().reset_index()
    aux_banco['ym_banco'] = np.vectorize(ym)(aux_banco['fechabanco'])
    
    final = pd.merge(final, aux_banco[['idcontratos', 'ym_banco']], on = 'idcontratos', how = 'left')
    
    for k, l in list(zip([date], [ym_date])):
        print(k, l)
        i = k
        i = datetime.datetime(int(i[:4]), int(i[5:i.find('-',5,8)]), int(i[i.find('-',5,8)+1:]) ).date()
        #Cortamos el dataframe hasta el ym de corte, ordenamos y reseteamos los índices
        hold0 = final[final['ym'] <=  l].copy()
        
        hold0.sort_values(by=["idcontratos", "array"], ignore_index=True, inplace = True)   
        try:
            hold0.drop('index', axis = 1, inplace = True)
            hold0.reset_index(inplace = True)
        except:
            hold0.reset_index(inplace = True)
    
        #Creamos una columna auxiliar para calcular a fecha de corte y calculamos la diferencia de días
        hold0['auxiliar'] = datetime.datetime.strptime(k, '%Y-%m-%d')
        
        #Como el excel tenía más datos de los que se tenían en la base de datos, los renglonees faltantes de la columna fecha
        #serán llenados con el valor de array
        ids = hold0[hold0['fecha'].isnull()].index
        hold0.loc[ids, 'fecha'] = hold0.loc[ids, 'array'] 
        hold0['fecha'] = hold0['fecha'].astype('datetime64[ns]')
        
        
        #Colocaremos la etiqueta que debe de tener por diferencia de fechas
        #Sacaremos la eitqueta de los valores que tienen días (corrientes) y los que no tienen días (restarlos)
        currents = hold0[~hold0['principal_days'].isna()].index
        hold0.loc[currents, 'etiqueta'] = 'Current'
        
        nulos = hold0[hold0['principal_days'].isna()].index
        #Seteamos valores por default
        hold0.loc[nulos, 'principal_days'] = (hold0.loc[nulos, 'auxiliar'] - hold0.loc[nulos, 'fecha']).dt.days.astype('Int64')
        hold0.loc[nulos, 'etiqueta'] = np.vectorize(ventana)(hold0.loc[nulos, 'principal_days'])
        
        #Vamos a tomar el último registro de hold0 por idcontratos
        dt = hold0[hold0['ym'] == l].copy()
        #pivote_check = pd.pivot_table(dt, values='principal_outstanding', index='ym_banco', columns= 'Status Change', aggfunc='sum', fill_value=0)
        wt_down = dt[dt['statusEN'].isin(['Impairment', 'IMP'])]
               
        #Dropearemos los estados finales CS, IMP, Amortized
        dt.drop(dt[dt['statusEN'].isin(['Amortized', 'Impairment', 'CS', 'IMP'])].index, axis = 'index', inplace = True)
        
        #Sacaremos a los usuarios que están corrientes
        good_id = dt[dt['etiqueta'] == "Current"].index
        f_loan_tape1 = dt.loc[good_id, :]
        
        #Ahora, sacaremos el último registro pagado de los que no estaban corriente. Para esto haremos casos
        bad_id = dt[dt['etiqueta'] != "Current"]['idcontratos'].to_list()
        #bad_id = dt[~dt.index.isin(good_id)]['Loan Id'].to_list()
        
        #Caso 1: Por defecto, el primer periodo debería ser fechabanco. Sin embargo, en el supuesto de que su primer cuota
        #fuera en el mismo ym del ym_fechabanco y no la haya pagado, en el momento de hacer el groupby y quedarnos con el 
        #último registro, este sería nulo y en teoría, si nunca nos pagan, jamás tendría renglones pagados. Sacamos estos usuarios
        id_all_null = hold0[(hold0['fechapagoefectiva'].isnull()) & (hold0['period'].isin([0]))]['idcontratos'].to_list()
        #Sacamos los índices que se usarán para posicionar la antigüedad de estos usuarios
        f_loan_tape2_idx1 = hold0[(hold0['idcontratos'].isin(id_all_null)) & (hold0['period'].isin([0]))].index
        
        #Caso 2: Los usuarios que tienen fechapagoefectiva distinta de nulo y el renglón donde obtenemos su último pago es
        #disitnto al renglón máximo, para esto agruparemos y sacaremos de la columna index el máximo y haremos un merge
        group_max_idx = hold0.groupby('idcontratos')['index'].max().reset_index().rename(columns = {'index': 'max_index'})
        hold0 = pd.merge(hold0, group_max_idx, on = ['idcontratos'], how = 'left')
        f_loan_tape2_idx2 = hold0[(hold0['idcontratos'].isin(bad_id)) & (~hold0['idcontratos'].isin(id_all_null)) & (~hold0['fechapagoefectiva'].isnull())].groupby("idcontratos").last().reset_index()

        #Caso 2_1: Sacaremos a los que les podremos sumar un renglón
        f_loan_tape2_idx2_1 = f_loan_tape2_idx2[f_loan_tape2_idx2['index'] != f_loan_tape2_idx2['max_index']]['index'].to_list()
        ar = np.array(f_loan_tape2_idx2_1)
        ar_index = ar + 1
        ar_index_1 = ar_index.tolist()
        
        #Caso 2_2: Sacaremos a los que NO les podremos sumar un renglón
        ar_index_2 = f_loan_tape2_idx2[f_loan_tape2_idx2['index'] == f_loan_tape2_idx2['max_index']]['index'].to_list()
        
        #Unimos las tres listas
        f_loan_tape2_idx = f_loan_tape2_idx1.tolist() + ar_index_1 + ar_index_2
             
        #Sacaremos los renglones correspondientes de los usuarios que no estaban al corriente y haremos un merge con ambas tablas
        f_loan_tape2 = hold0.loc[f_loan_tape2_idx, :]
    
        f_loan_tape = pd.concat([f_loan_tape1, f_loan_tape2])
        
        #Ordenamos por fecha y nos quedamos con el último
        f_loan_tape.sort_values(by=["idcontratos", "fecha"], ignore_index=True, inplace = True)
        f_loan_tape.drop_duplicates(subset="idcontratos", keep='last', inplace=True)

    print('Corte de la base de datos realizado')
    
    ############################ Genera el P+I en dinero ######################
    pivote = pd.pivot_table(f_loan_tape, values='principal_outstanding', index='ym_banco', columns='etiqueta', aggfunc='sum', fill_value=0)
    pivote.reset_index(inplace = True)
    
    #Value of grated loans - principal
    grated_loans = lstaid.groupby('ym_fb')['importe'].sum().reset_index().rename(columns = {'importe' : 'value_grated_loans'})
    grated_loans['ym_fb'] = grated_loans['ym_fb'].astype('float')
    #Concatenamos por la izquierda con pivote
    pivote = pd.merge(grated_loans, pivote, left_on = 'ym_fb', right_on = 'ym_banco', how = 'left')
    pivote.drop('ym_banco', axis = 1, inplace = True)

    #Creamos la columna de written down principal y llenamos los valores faltantes con 0
    wt_down_pricipal = wt_down.groupby('ym_banco')['principal_outstanding'].sum().reset_index().rename(columns = {'principal_outstanding' : 'written_down_principal'})
    
    pivote = pd.merge(pivote, wt_down_pricipal, left_on = 'ym_fb', right_on= 'ym_banco', how = 'left')
    pivote.fillna(0, inplace = True)
    
    pivote['principal_recovery'] = pivote['value_grated_loans'] -pivote['Current'] -pivote['written_down_principal'] -pivote['1-30d'] -pivote['31-60d'] -pivote['61-90d'] -pivote['91-120d'] -pivote['121-150d'] -pivote['151-180d'] -pivote['>180d'] 

    pivote.drop('ym_banco', axis = 'columns', inplace = True)
    pivote = pivote.reindex(['ym_fb', 'value_grated_loans', 'principal_recovery', 'Current', '1-30d', '31-60d', '61-90d', '91-120d', '121-150d', '151-180d', '>180d', 'written_down_principal'], axis=1)
    pivote = pivote[(pivote['ym_fb'] <= ym_date)]
    
    ########################### Genera el P+I en usuarios #####################
    pivote_usuarios = pd.pivot_table(f_loan_tape, values='idcontratos', index='ym_banco', columns='etiqueta', aggfunc='count', fill_value=0)
    pivote_usuarios.reset_index(inplace = True)
    
    #Value of grated loans - principal
    grated_loans_usuarios = lstaid.groupby('ym_fb')['importe'].count().reset_index().rename(columns = {'importe' : 'number_grated_loans'})
    grated_loans_usuarios['ym_fb'] = grated_loans_usuarios['ym_fb'].astype('float')
    #Concatenamos por la izquierda con pivote
    pivote_usuarios = pd.merge(grated_loans_usuarios, pivote_usuarios, left_on = 'ym_fb', right_on = 'ym_banco', how = 'left')
    pivote_usuarios.drop('ym_banco', axis = 1, inplace = True)

    #Creamos la columna de written down principal y llenamos los valores faltantes con 0
    wt_down_pricipal_usuarios = wt_down.groupby('ym_banco')['idcontratos'].count().reset_index().rename(columns = {'idcontratos' : 'written_down_users'})
    
    pivote_usuarios = pd.merge(pivote_usuarios, wt_down_pricipal_usuarios, left_on = 'ym_fb', right_on= 'ym_banco', how = 'left')
    pivote_usuarios.fillna(0, inplace = True)
    
    pivote_usuarios.drop('ym_banco', axis = 'columns', inplace = True)
    pivote_usuarios = pivote_usuarios.reindex(['ym_fb', 'number_grated_loans',  'Current', '1-30d', '31-60d', '61-90d', '91-120d', '121-150d', '151-180d', '>180d', 'written_down_users'], axis=1)
    pivote_usuarios = pivote_usuarios[(pivote_usuarios['ym_fb'] <= ym_date)]
    
    
    conexion_sheets('vintage_pi_report', [hora_fecha, pivote], [1, 4], [1, 1]) 
    conexion_sheets('vintage_pi_report_users', [hora_fecha, pivote_usuarios], [1, 4], [1, 1]) 

def loan_tape_stratification_collection(loan_tape_copy, final, year, destiny, table_name, if_exists):
    #Nos posicionamos en la carpeta IUVO    
    os.chdir(r'C:\Users\Bi_analyst\Desktop\Python')
    aux_df = final.copy()
    df1 = loan_tape_copy.copy()
    
    #Traemos la conexión de la base de datos
    db = database(geo)
    
    print('Creamos la primer tabla de los pagos distintos a los de cancelacion')
    #Primero sacaremos la prelación de pagos de los usuarios que pagaron distinto a una cancelacion
    colss = ['importecuota', 'importepago', 'Late fee & Prepayment fee', 'Late fee & Prepayment fee VAT', 'cuotainteres', 'cuota_intereses_iva', 'cuotacapital', 'cuotaservicios', 'cuotaserviciosiva',
             'cuotaseguro', 'cuotasegurocapital', 'gastoapertura', 'cuota_gastoapertura_iva', 'pagocancelacion', 'plusfee']
    
    buckets1 = df1[(df1['estado'].isin([ '0', '1'])) & (df1['importepago'] != 0)]
    j = 0
    for i in colss:
        if j == 0:
            computed_values1 = buckets1.groupby(['idcontratos', 'fechapagoefectiva']).agg({'index':'min', i: 'sum'}).reset_index().rename(columns = {i:str(i)+'_comp'})
        else:
            hold = buckets1.groupby(['idcontratos', 'fechapagoefectiva']).agg({ i: 'sum'}).reset_index().rename(columns = {i:str(i)+'_comp'})
            computed_values1 = pd.merge(computed_values1, hold, on = ['idcontratos', 'fechapagoefectiva'], how = 'left')
        j += 1
        
    computed_values1['cuotainteres_comp'] = computed_values1['cuotainteres_comp'] + computed_values1['plusfee_comp']
    computed_values1['comision_cancelacion_comp'] = 0
    
    print('Creando la segunda tabla que contiene los pagos cancelaciones')
    #Vamos a crear una función que nos ayudarpa a ver si hay servicios o servicios e intereses
    def current(prevfecha, fecha, fechapago):
        if fechapago <= prevfecha:
            return 1
        else:
            return 0  
        
    #Esta función nos ayudará a saber si un contrato tiene etiqueta Not Restructured, Restructured, Car Sold, Restuctured & Car Sold
    def estado(col):
        if col == 0:
            return 'Not Restructured'
        elif col == 1:
            return 'Restructured'
        elif col == 2:
            return 'Car Sold'
        else:
            return 'Restuctured & Car Sold'
        
    def mes_fecha(col):
        try:
            a = col.strftime('%Y%m')
        except:
            a = ''
        return a
    
    #Para el pago cancelación, veremos aquellos que tengan acuerdo == 2 desde la base de datos sus intereses de cancelacion y sus pagos calculados en loan_tape
    buckets2 = df1[(~df1['pagocancelacion'].isna()) & (df1['pagocancelacion'] > 0)]
    ids_cancelacion = buckets2['idcontratos'].to_list()
    lid_cancelacion = '(' + str(ids_cancelacion)[1:-1] + ')'
    
    #Sacamos una copia del valor calculado y sacar el valor pasado de fecha y estado
    hold = df1[df1['idcontratos'].isin(ids_cancelacion)].copy()
    hold['prev_fecha'] = hold.groupby('idcontratos')['fecha'].shift(1)
    
    #Nos quedaremos sólo con los renglones donde pagocancelacion es mayor a cero
    hold = hold[hold['pagocancelacion'] > 0]
    
    #Aplicaremos una bandera para saber si sólo hay que cobrar el capital pendiente y ga o también intereses y después servicios+iva
    hold['flag'] = np.vectorize(current, otypes=[int])( pd.to_datetime(hold['prev_fecha']).astype('datetime64[ns]') + pd.Timedelta(days = 5), pd.to_datetime(hold['fecha']), pd.to_datetime(hold['fechapago']) )
    
    hold['diferencia_dias'] = pd.to_datetime(hold['fechapago']) - pd.to_datetime(hold['prev_fecha'])
    hold['diferencia_dias'] = hold['diferencia_dias'].apply(lambda x: abs(x.days))
    
    hold = hold[['idcontratos', 'prev_fecha', 'flag', 'diferencia_dias', 'index']]
    
    print('Extrayendo la información de la base de datos')
    #Traeremos de la base de datos estos contratos con sus respectivos valores de intereses de cancelacion
    q6 = f'''SELECT a.id AS idcontratos, formapago, IFNULL(a.importe, 0) AS importe, IFNULL(a.comisionapertura, 0) AS comisionapertura, 
    b.tipo_interes/100 AS tipo_interes, comision_amortizacion/100 as comision_amortizacion, IFNULL(porcentaje_servicios/100, 0) AS porcentaje_servicios, {iva_applicated} AS iva,
    c.fecha, c.fechapago, c.fechapagoefectiva, c.estado, c.pagocancelacion AS pagocancelacion_comp, c.capitalpendiente, c.capitalpendiente_sga,
    if(gastoapertura IS NOT NULL, gastoapertura, if(c.capitalpendiente_sga IS NOT NULL, c.capitalpendiente - capitalpendiente_sga, 0)) AS gastoapertura,
    c.importecuota as importecuota_comp, IFNULL(cuota_gastoapertura_iva, 0) as cuota_gastoapertura_iva_comp,
    if(a.id in {lid_cancelacion}, 1, 0) as cancelacion_flag
    FROM usulatie_bbdd.contratos AS a
    LEFT JOIN usulatie_bbdd.tipo_contratos AS b ON a.id_tipo_contrato = b.id
    LEFT JOIN usulatie_bbdd.cuotas AS c ON a.id = c.idcontratos
    WHERE (a.idproductos IN {productos} AND a.fechabanco IS NOT NULL AND a.estado != 2 and a.id not in (322) and YEAR(a.fechabanco) >= {year})  or (a.id in (770, 108, 430, 546, 277) )
    ORDER BY idcontratos ASC, c.fecha ASC;'''

    if lid_cancelacion != '()':
        cancelacion = pd.read_sql(q6, db)
    else:
        cancelacion = pd.DataFrame(columns = ['formapago'])
    
    #No podemos cambiar el order by fecha, ya que nos meteríamos con los pagos por correo y podríamos meternos en más problemas
    #Por eso dejaremos el script como está y modificaremos los casos donde haya solo forma de pago 1 y el renglón siguiente sea
    #mayor al renglón actual
    idx_sn_formapago6 = [0]
    usuarios_formapago6 = cancelacion[cancelacion['formapago'].astype('str') == '6']['idcontratos'].unique()
    
    
    while len(idx_sn_formapago6) > 0:    
        #Haremos un prev_capitalpendiente agrupado por idcontratros para asegurarnos de que el siguiente renglón sea menor o igual al actual
        cancelacion['prev_capitalpendiente'] = cancelacion.groupby('idcontratos')['capitalpendiente'].shift(1)   
        idx_sn_formapago6 = cancelacion[(~cancelacion['idcontratos'].isin(usuarios_formapago6)) & (cancelacion['capitalpendiente'] > cancelacion['prev_capitalpendiente'])].index
        prev, cur = cancelacion.iloc[np.array(idx_sn_formapago6)-1], cancelacion.iloc[idx_sn_formapago6]
        cancelacion.iloc[np.array(idx_sn_formapago6)-1], cancelacion.iloc[idx_sn_formapago6] = cur, prev
    
    cancelacion.drop('prev_capitalpendiente', axis = 1, inplace = True)    
    
    #Vamos a rellenar la columna de capitalpendiente_sga de tal manera que hagamos un ffill para los que tuvieron minicuotas 
    #y después haremos un .loc poniendo el valor de capitalpendiente para los que todavia no ponian esa columna   
    cancelacion['capitalpendiente_sga'] = cancelacion.groupby('idcontratos')['capitalpendiente_sga'].transform(lambda v: v.ffill())
    idx_capital_sga = cancelacion[cancelacion['capitalpendiente_sga'].isna()].index
    cancelacion.loc[idx_capital_sga, 'capitalpendiente_sga'] = cancelacion.loc[idx_capital_sga, 'capitalpendiente']
    
    
    cancelaciones = cancelacion[cancelacion['cancelacion_flag'] == 1].copy()

    #Traemos de la base de datos a los contratos que han sido impariment para hacer un join pos idcontratos
    q7 = '''SELECT c.id_contrato AS idcontratos, min(c.fecha) as 'Deafult Date'
    FROM usulatie_bbdd.logs_contratos AS c
    LEFT JOIN usulatie_bbdd.contratosestados as d on c.nuevo_estadov2 = d.codigo_estado
    WHERE d.statusEN IN ('IMP')
	GROUP BY c.id_contrato;'''
    defaulters = pd.read_sql(q7, db)
    
    q8 = f'''SELECT a.id AS idcontratos, 1 AS 'NPL Once'
    FROM usulatie_bbdd.contratos as a
    LEFT JOIN usulatie_bbdd.logs_contratos AS b on b.id_contrato = a.id
    LEFT JOIN usulatie_bbdd.contratosestados as c on b.nuevo_estadov2 = c.codigo_estado
    WHERE (a.idproductos IN {productos} AND a.fechabanco IS NOT NULL AND a.estado != 2 and a.id not in (322)
    and YEAR(a.fechabanco) >= {year} AND c.statusEN IN ('NPL'))  or (a.id in (770, 108, 430, 546, 277) AND c.statusEN IN ('NPL'))
    GROUP BY a.id;'''
    npl_once =pd.read_sql(q8, db)
    
    #Sacamos el valor pasado de capitalpendiente, capitalpendiente_sga y gastoapertura
    cancelaciones['prev_capitalpendiente'] = cancelaciones.groupby('idcontratos')['capitalpendiente'].shift(1)
    cancelaciones['prev_capitalpendiente_sga'] = cancelaciones.groupby('idcontratos')['capitalpendiente_sga'].shift(1)
    cancelaciones['prev_gastoapertura'] = cancelaciones.groupby('idcontratos')['gastoapertura'].shift(1)
    
    #Nos quedamos sólo con el renglon que tiene el pago cancelacion
    cancelaciones = cancelaciones[cancelaciones['pagocancelacion_comp'] >0] 
    
    #Rellenamos los valores previos que pueden salir nulos, en el caso donde el usuario cancela desde el primer momento
    idx_ga = cancelaciones[cancelaciones['prev_gastoapertura'].isna()].index
    idx_cp = cancelaciones[cancelaciones['prev_capitalpendiente'].isna()].index
    idx_cpga = cancelaciones[cancelaciones['prev_capitalpendiente_sga'].isna()].index
    cancelaciones.loc[idx_cp, 'prev_capitalpendiente'] = cancelaciones.loc[idx_cp, 'importe']
    cancelaciones.loc[idx_ga, 'prev_gastoapertura'] = cancelaciones.loc[idx_ga, 'comisionapertura']
    cancelaciones.loc[idx_cpga, 'prev_capitalpendiente_sga'] = cancelaciones.loc[idx_cpga, 'importe'] - cancelaciones.loc[idx_cpga, 'comisionapertura']
    
    hold_cancelaciones = pd.merge(hold, cancelaciones, on = 'idcontratos', how = 'left')

    #Para acompletar las columnas y ser consistentes con el anterior dataframe, vamos a declarar unas columnas
    hold_cancelaciones['importepago_comp'] = 0
    hold_cancelaciones['Late fee & Prepayment fee_comp'] = 0
    hold_cancelaciones['Late fee & Prepayment fee VAT_comp'] = 0
    hold_cancelaciones['cuota_intereses_iva_comp'] = 0
    hold_cancelaciones['cuotaseguro_comp'] = 0
    hold_cancelaciones['cuotasegurocapital_comp'] = 0
    hold_cancelaciones['plusfee_comp'] = 0
    
    #Comnezaremos a hacer los cálculos del importe de pago cancelación
    hold_cancelaciones['cuotacapital_comp'] = hold_cancelaciones['prev_capitalpendiente_sga']
    hold_cancelaciones['gastoapertura_comp'] = hold_cancelaciones['prev_gastoapertura']
    
    hold_cancelaciones['comision_cancelacion_comp'] = hold_cancelaciones['prev_capitalpendiente_sga']*hold_cancelaciones['comision_amortizacion']
    hold_cancelaciones['cuotainteres_comp'] = 0
    hold_cancelaciones['cuotaservicios_comp'] = 0
    hold_cancelaciones['cuotaserviciosiva_comp'] = 0
    
        
    computed_values2 = hold_cancelaciones[['idcontratos', 'fechapagoefectiva', 'index', 'importecuota_comp',
           'importepago_comp', 'Late fee & Prepayment fee_comp', 'Late fee & Prepayment fee VAT_comp', #'tipo_interes',
           'cuotainteres_comp', 'cuota_intereses_iva_comp', 'cuotacapital_comp',
           'cuotaservicios_comp', 'cuotaserviciosiva_comp', 'cuotaseguro_comp',
           'cuotasegurocapital_comp', 'gastoapertura_comp',
           'cuota_gastoapertura_iva_comp', 'pagocancelacion_comp', 'plusfee_comp',
           'comision_cancelacion_comp']]
    
    print('Uniendo ambas tablas con la tabla de toda la cartera')
    #Haremos un concat y sumaremos por un jopin si tiene pagos en fechas iguales, cada id en tablas diferentes
    computed_values = pd.concat([computed_values1, computed_values2])
    computed_values['idcontratos'] = computed_values['idcontratos'].astype('Int64')
    
    loan_tape_copy = loan_tape_copy[['index', 'idclientes', 'idcontratos', 'idcontratos_acuerdos', 'fechabanco', 'importe', 'comisionapertura',
                        'Amount_wo_of', 'numcuotas', 'Age of customer', 'Salary of Customer', 'Prodcut Type', 'Collateral Amount', 'Initial LTV',
                       'acuerdo', 'estado', 'tipo_interes', 'Days', 'fecha', 'fechapago', 'fechapagoefectiva', 'Transaction Type', 'tipo_interes', 'Maturity Date', 'ym_efectiva', 'ym_fecha', 'ym',
                       'importecuota', 'cuotainteres', 'cuota_intereses_iva', 'cuotacapital', 'Pending Origination Fee','capitalpendiente', 'cuotaservicios',
                       'cuotaserviciosiva', 'gastoapertura', 'cuota_gastoapertura_iva', 'pagocancelacion']]
    
    #Dropeamos las columnas de idcontratos y fechapagoefectiva
    computed_values.drop(['idcontratos', 'fechapagoefectiva'], axis = 1, inplace =True)
    
    prelacion = pd.merge(loan_tape_copy, computed_values, on = 'index', how = 'left')

    aux_df['ym'] = aux_df['ym'].astype('float')
    prelacion = pd.merge(prelacion, aux_df[['idcontratos', 'ym', 'principal_outstanding', 'Extension']], left_on = ['idcontratos', 'ym'], right_on = ['idcontratos', 'ym'], how = 'left')
    
    #Actualizaremos algunas columnas, idcontratos_acuerdos pondremos nulo cuando sea los renglones de los contratos originales
    idx_idca = prelacion[prelacion['idcontratos_acuerdos'] == -1].index
    prelacion.loc[idx_idca, 'idcontratos_acuerdos'] == np.nan
    
    #Cambiaremos la nomenclatura de la columna acuerdo para que sea Not Restructured, Restructured, Car Sold, Restuctured & Car Sold. 
    #filtramos a todos aquellos acuerdos que tengan 1 en la columna acuerdo
    estado_group = prelacion.groupby(['idcontratos', 'acuerdo'])['ym'].count().reset_index().drop('ym',  axis = 1)
    label = estado_group.groupby('idcontratos')['acuerdo'].sum().reset_index()
    label['etiqueta'] = np.vectorize(estado)(label['acuerdo'])
    label.drop('acuerdo', axis = 1, inplace = True)
    
    #Unimos con la tabla original y cambiamos el nombre de la columna
    #prelacion.drop('acuerdo', axis = 1, inplace = True)
    prelacion = pd.merge(prelacion, label, on = 'idcontratos', how = 'left')
    prelacion.rename(columns = {'etiqueta':'acuerdo_type'}, inplace = True)
    
    #Cambiamos la columna de Period por un groupby en una tabla externa y los unimos con prelacion por idcontratos y ym
    dataf = prelacion.groupby(['idcontratos', 'ym'])['index'].min().reset_index()
    dataf.drop('index', axis = 1, inplace = True)
    dataf['Period'] = dataf.groupby(['idcontratos']).cumcount()
    prelacion = pd.merge(prelacion, dataf, left_on = ['idcontratos', 'ym'], right_on = ['idcontratos', 'ym'], how = 'left')
    
    #Initial LTV no puede ser mayor a 75%, pondremos una regla para limitarlo a este monto, por lo cual esta parte del
    #código debe ser atendida desde UW
    prelacion['Initial LTV'] = prelacion['Initial LTV'].apply(lambda x: x if x <=0.75 else 0.75 )
    
    #Hacemos un merge con defaulters para sacar la fecha de defaulter
    prelacion = pd.merge(prelacion, defaulters, left_on = ['idcontratos'], right_on = ['idcontratos'], how = 'left')
    prelacion.reset_index(drop=True, inplace=True)
    
    #Hacemos un merge con los npl_once
    prelacion = pd.merge(prelacion, npl_once, on = 'idcontratos', how = 'left')
    prelacion.fillna(value={'NPL Once':0}, inplace = True)
    
    #Vamos a traer la informaicón de los coches vendidos para ponerlos en la tabla
    q9 = '''SELECT idcontratos, a.importe - IFNULL(iva_rebu, 0) - IFNULL(devolucion_exceso_venta,0) AS principal,
    a.fechapagoefectivo AS 'fechapagoefectiva', a.importe AS "Car Sale €"
    FROM usulatie_bbdd.coches_vendidos AS a
    LEFT JOIN (SELECT idcoches_vendidos, importe AS iva_rebu FROM usulatie_bbdd.coches_vendidos_costes 
    WHERE idtipo = 10) AS b ON a.id = b.idcoches_vendidos;'''
    IVA_REBU_FULL = pd.read_sql(q9, db)
    
    IVA_REBU = IVA_REBU_FULL[['idcontratos', 'principal']].copy()
    prelacion = pd.merge(prelacion, IVA_REBU_FULL, on = ['idcontratos', 'fechapagoefectiva'], how = 'left')
    
    prelacion = prelacion[['index', 'idclientes', 'idcontratos', 'idcontratos_acuerdos', 'fechabanco', 'importe', 'comisionapertura', 
                        'Amount_wo_of', 'numcuotas', 'Maturity Date', 'Age of customer', 'Salary of Customer', 'Prodcut Type', 'Collateral Amount', 'Initial LTV', 'acuerdo',
                       'acuerdo_type', 'Car Sale €', 'estado', 'tipo_interes', 'Days', 'NPL Once', 'fecha', 'ym_fecha', 'capitalpendiente', 'importecuota', 'cuotacapital', 'gastoapertura', 
                       'cuota_gastoapertura_iva', 'cuotainteres', 'cuota_intereses_iva', 'cuotaservicios', 'cuotaserviciosiva', 'fechapago', 'fechapagoefectiva', 'ym_efectiva', 'Transaction Type', 
                       'Pending Origination Fee', 'importepago_comp', 'cuotacapital_comp', 'gastoapertura_comp', 'cuota_gastoapertura_iva_comp', 'cuotainteres_comp', 'cuota_intereses_iva_comp', 'cuotaservicios_comp', 
                       'cuotaserviciosiva_comp', 'Late fee & Prepayment fee_comp', 'Late fee & Prepayment fee VAT_comp', 'plusfee_comp', 'pagocancelacion',  'comision_cancelacion_comp', 'pagocancelacion_comp', 'Period']].copy()
    
    prelacion.rename(columns = {'acuerdo':'Acuerdo', 'index':'Index', 'idclientes':'Customer ID', 'idcontratos':'Loan ID', 'idcontratos_acuerdos':'Restructured Loan ID',
                        'fechabanco': 'Loan Origination Date', 'importe': 'Loan Amount', 'comisionapertura': 'Origination Fee', 'Amount_wo_of': 'Loan Amount w/Origination Fee',
                        'numcuotas': 'Term', 'acuerdo_type': 'Restuctured/CS  Flag', 'estado': 'Payment Type', 'tipo_interes': 'Interest Rate',
                        'fecha':'Scheduled Payment Date', 'ym_fecha':'Scheduled Payment YM', 'capitalpendiente':'Scheduled Oustanding Balance', 'importecuota':'Scheduled Installment', 
                        'cuotacapital': 'Scheduled Principal', 'gastoapertura': 'Scheduled Origination Fee', 'cuota_gastoapertura_iva': 'Scheduled Origination Fee VAT',
                        'cuotainteres': 'Scheduled Interest', 'cuota_intereses_iva':'Scheduled Interest_VAT', 'cuotaservicios': 'Scheduled Service Fee', 'cuotaserviciosiva': 'Scheduled Service Fee VAT',
                        'fechapago':'AC Payment Date', 'fechapagoefectiva':'AC Effective Date', 'ym_efectiva':'AC Effective Date YM',
                        'importepago_comp': 'AC Payment Amount', 
                        'cuotacapital_comp': 'AC Principal', 'gastoapertura_comp': 'AC Origination Fee', 'cuota_gastoapertura_iva_comp':'AC Origination Fee VAT', 'cuotainteres_comp': 'AC Interest', 
                        'cuota_intereses_iva_comp': 'AC Interest VAT', 'cuotaservicios_comp': 'AC Service Fee', 'cuotaserviciosiva_comp': 'AC Service Fee VAT',
                        'Late fee & Prepayment fee_comp': 'AC Late Fees & Prepayment Fees', 'Late fee & Prepayment fee VAT_comp': 'AC Late Fees & Prepayment Fees VAT', 'plusfee_comp':'AC Other Fees', 'pagocancelacion':'AC Early Prepayment',
                        'comision_cancelacion_comp':'AC Early Prepayment Fee', 'pagocancelacion_comp': 'AC Early Prepayment (Ref)'}, inplace = True)
    #'principal_outstanding':'AC Principal Oustanding'
    #Convertimos a Porcentaje y rellenamos la columna de Interest Rate por Loan ID
    prelacion['Initial LTV'] = prelacion[['Initial LTV']].applymap(lambda x: "{0:.2f}%".format(x*100) if isinstance(x, float) else ' ') 
    
    prelacion['Interest Rate'] = prelacion.groupby('Loan ID')['Interest Rate'].transform(lambda v: v.ffill())
    prelacion['Interest Rate'] = prelacion.groupby('Loan ID')['Interest Rate'].transform(lambda v: v.bfill())
    prelacion['Interest Rate'] = prelacion[['Interest Rate']].applymap(lambda x: "{0:.2f}%".format(x*100) if isinstance(x, float) else ' ') 
    
    for i in ['AC Payment Amount', 'AC Interest', 'AC Interest VAT', 'AC Principal', 'AC Service Fee', 'AC Service Fee VAT', 'AC Origination Fee' ]:
        prelacion[i] = prelacion[i].abs()
    
    #Cambiamos el signo sólo de los valores que enrealidad fueron un reversal
    idx_reversal = prelacion[(prelacion['Transaction Type'] == 'Reversal') & (~prelacion['AC Payment Amount'].isna())].index
    
    col_cambiosigno = ['AC Payment Amount', 'AC Principal', 'AC Origination Fee', 'AC Origination Fee VAT', 'AC Interest', 'AC Interest VAT', 'AC Service Fee', 
                       'AC Service Fee VAT', 'AC Late Fees & Prepayment Fees', 'AC Late Fees & Prepayment Fees VAT', 'AC Other Fees']
    prelacion.loc[idx_reversal, col_cambiosigno] = -prelacion.loc[idx_reversal, col_cambiosigno]
    prelacion['AC Late Fees & Prepayment Fees'] = prelacion['AC Late Fees & Prepayment Fees'].apply(lambda x: x if x > 0 else 0)
        
    print('Combinando ambos reportes...')
    principal_portfolio = final.copy()
    
    #Por Loan ID vamos a sacar una lista de Period y de index de prelacion
    period_index1 = prelacion[['Loan ID', 'Scheduled Payment YM', 'Index']].copy()
    #Tomaremos de principal_portfolio la columna statusEN, principal_outstanding, period y idcontratos y cambiaremos los nombres
    principal_portfolio.rename(columns = {'idcontratos':'Loan ID', 'ym':'Scheduled Payment YM', 'period':'Period'}, inplace = True)
    hold = principal_portfolio[['Loan ID', 'Scheduled Payment YM']].copy()
    hold['Index'] = None
    
    #Vamos a guardar en una tupla las columnas 'Loan ID', 'Period' de prelacion para hacer el filtro
    loan_period = list(zip(period_index1['Loan ID'], period_index1['Scheduled Payment YM']))
    period_index2 = hold[~(hold[['Loan ID', 'Scheduled Payment YM']].apply(tuple, 1).isin(loan_period))]
    
    print('Concatenamos y reordenamos')
    #Ahora concatenamos tanto period_index1 y period_index2 y ordenaremos por 'Scheduled Payment YM', 'Index'
    period_index = pd.concat([period_index1, period_index2])
    period_index.sort_values(by=['Loan ID', 'Scheduled Payment YM'], inplace =True)
    
    #Posteriormente uniremos por Loan ID, Period, Index con prelacion
    period_index = pd.merge(period_index, prelacion, on = ['Loan ID', 'Scheduled Payment YM', 'Index'], how = 'left')
    period_index.drop(['Period'], axis = 'columns', inplace = True)
    
    #Posteriormente uniremos por Loan ID, Period, con principal_portfolio para sacar las columnas restantes que tienen que
    #ver con los estatus, P2P
    period_index = pd.merge(period_index, principal_portfolio[['Loan ID', 'Scheduled Payment YM', 'Period', 'array', 'principal_outstanding', 'statusEN', 'Extension', 'ym_status', 'Ocurrence']], on = ['Loan ID', 'Scheduled Payment YM'], how = 'left')   
    
    #Hay ocasiones en las que el estado de amortización lo ponen antes de registrar el pago, por lo que corregiremos eso
    loans_with_extension = period_index[~period_index['Extension'].isna()]['Loan ID'].unique()
    idx_extension = period_index[(period_index['Loan ID'].isin(loans_with_extension)) & (period_index['AC Early Prepayment'] > 0) & (period_index['Extension'].isna())].index
    period_index.loc[idx_extension, 'Extension'] = 'Extension'
    
    #Y quitamos los registros que no dberían llevar 'Extension'
    idx_non_extension= period_index[(period_index['Extension'] == 'Extension') & (period_index['AC Early Prepayment'] == 0)].index
    period_index.loc[idx_non_extension, 'Extension'] = np.nan
    
    
    print('Rellenamos las columnas estadísticas')
    #Vamos a rellenar todos los valores hacia bajo las siguientes columnas
    cols = ['Customer ID', 'Restructured Loan ID', 'Loan Origination Date', 'Loan Amount', 'Origination Fee', 'Loan Amount w/Origination Fee',
           'Age of customer', 'Salary of Customer', 'Prodcut Type', 'Collateral Amount', 'Initial LTV', 'Restuctured/CS  Flag', 'Maturity Date',
           'Payment Type', 'Interest Rate', 'NPL Once', 'Term']
    for i in cols:
        period_index[i] = period_index.groupby('Loan ID')[i].transform(lambda v: v.ffill())
    
    print('Dropeamos y creamos index')
    #Dropearemos la columna Indice y la volveremos a obtener
    period_index.drop('Index', inplace = True, axis = 1)
    period_index.reset_index(inplace = True)

    #Ahora, quitaremos los renglones  que estén repetidos
    repetidos = period_index.groupby(['Loan ID', 'array']).last()
    repetidos = repetidos.reset_index()
    #Los valores de la columna index que no estén en repetidos los tomaremos y rellenaremos con nulos lo valores de
    # 'array', 'principal_outstanding', 'statusEN', 'ym_status'
    idx_blank = period_index[~period_index['index'].isin( repetidos['index'].to_list() )].index
    period_index.loc[idx_blank, ['array', 'principal_outstanding', 'statusEN', 'ym_status']] = None

    
    #Sacaremos el valor de los índices que 'Acuerdo' == 2 y le restaremos uno al valor para ver si el valor 
    #de 'Principal Oustanding' == 0 lo setearemos por nan y llenaremos con un ffill agrupado por usuario
    #idx_prev_carsold = np.array(period_index[period_index['Acuerdo'] == 2].index)-1
    #idx_prev_carsold = period_index[(period_index['index'].isin(list(idx_prev_carsold))) & (period_index['AC Principal Oustanding'] == 0)].index
    #period_index.loc[idx_prev_carsold, 'AC Principal Oustanding'] = np.NaN
    #period_index[['AC Principal Oustanding']] = period_index.groupby('Loan ID')[['AC Principal Oustanding']].transform(lambda v: v.ffill())
    
    #Si PrincipalOutstanding difiere de principal_outstanding, nos quedaremos con el valor de principal_outstanding sólo en estos casos
    #idx_mismatch = period_index[(period_index['principal_outstanding'] != period_index['AC Principal Oustanding']) & (~period_index['principal_outstanding'].isna())].index
    #period_index.loc[idx_mismatch, 'AC Principal Oustanding'] = period_index.loc[idx_mismatch, 'principal_outstanding']
    
    #Modificamos los valores de la venta de carros
    #Haremos el un shift de una columna auxiliar de 'Principal Oustanding' y modificaremos el valor de la columna Principal y Other Fees
    ids_ivarebu_list = IVA_REBU['idcontratos'].to_list()
    
    #idx_2 = period_index[period_index['Acuerdo'] == 2].index
    #period_index['auxiliar'] = period_index['AC Principal Oustanding'].shift(1, axis=0)
    
    # CHECAR DEBERÏA ESTAR EL VALOR DE LOS PAGOS TAL Y COMO LA BASE DE DATOS
    #period_index.loc[idx_2, 'AC Principal'] = period_index.loc[idx_2, 'auxiliar'] - period_index.loc[idx_2, 'AC Principal Oustanding']
    #period_index.loc[idx_2, 'AC Other Fees'] = period_index.loc[idx_2, 'AC Payment Amount'] - period_index.loc[idx_2, 'AC Principal']
    
    #Vamos a unir la tabla en curso al valor del IVA REBU para sustituir el valor de 'AC Principal' por principal
    period_index = pd.merge(period_index, IVA_REBU, left_on = 'Loan ID', right_on = 'idcontratos', how = 'left')
    idx_3 = period_index[(period_index['Acuerdo'] == 2) & (period_index['Loan ID'].isin(ids_ivarebu_list))].index
    period_index.loc[idx_3, 'AC Principal'] = period_index.loc[idx_3, 'principal']
    
    #Dropemaos las columnas que unimos
    try:
        period_index.drop(['idcontratos', 'IVA_REBU'], axis = 1, inplace = True)
    except:
        pass
    
    #Dropeamos auxiliar y cambiamos nombre de columnas
    period_index.drop(['array'], axis = 1, inplace = True)
    period_index.rename(columns = {'principal_outstanding':'EOM Principal Outstanding', 'statusEN':'EOM Status', 'ym_status': 'EOM Status YM', 'Period':'EOM Period'}, inplace = True)
    
    #Movemos de lugar la columna con la cual se cuadra por ym la cartera y le cambiamos el nombre de Scheduled Payment YM a EOM Loan Book YM
    period_index['EOM Loan Book YM'] = period_index['Scheduled Payment YM']    
    period_index.drop(['Scheduled Payment YM', 'index'], axis = 1, inplace = True)
    
    #Nos quedamos sólo con las columnas únicas, dropeando las columnas con nombres repetidos
    period_index = period_index.loc[:,~period_index.columns.duplicated()].copy()    
    period_index = period_index[['Customer ID'] + [col for col in period_index.columns if col != 'Customer ID']]
    
    ###########################################################################
    #Vamos a hacer la regla de que si el pago es menor a la cuota designada, entonces vamos a afectar al buckets de servicios
    #y su respectivo IVA
    period_index['total_testeo'] = period_index[['AC Principal', 'AC Origination Fee', 'AC Origination Fee VAT', 'AC Interest',
                                                 'AC Service Fee', 'AC Service Fee VAT', 'AC Late Fees & Prepayment Fees', 'AC Late Fees & Prepayment Fees VAT']].sum(axis = 1)
    idx_pequenios = period_index[(period_index['total_testeo'] - period_index['AC Payment Amount'] > 0.02) & (period_index['AC Payment Amount'] != 0) & (~period_index['AC Effective Date'].isna()) & ((~period_index['AC Payment Amount'].isna()) | (~period_index['AC Principal'].isna()))].index
    period_index.loc[idx_pequenios, 'total_testeo'] = period_index.loc[idx_pequenios, 'total_testeo'] + period_index.loc[idx_pequenios, 'AC Other Fees']
    period_index['dif'] = 0
    period_index.loc[idx_pequenios, 'dif'] = (period_index.loc[idx_pequenios, 'AC Payment Amount'] - period_index.loc[idx_pequenios, 'total_testeo']).abs()
    

    period_index.loc[idx_pequenios, 'AC Service Fee'] = period_index.loc[idx_pequenios, 'AC Service Fee'] - period_index.loc[idx_pequenios, 'dif']/1.21
    period_index.loc[idx_pequenios, 'AC Service Fee VAT'] = period_index.loc[idx_pequenios, 'AC Service Fee VAT'] - iva_applicated*period_index.loc[idx_pequenios, 'dif']/1.21
    
    #Ahora vamoslimitar el AC Other Fees	 donde debe ser menor a 3 euros, caso sontrario se va a una cuenta maestra para 
    #Guardarlo en una cuenta maestra 
    period_index['AC Other Fees'] = period_index['AC Other Fees'].apply(lambda x: x if x <= 3 else 0)
    period_index['AC Other Fees'] = period_index['AC Other Fees'].apply(lambda x: x if 0 <= x else 0)
    
    try:
        period_index.drop(['total_testeo', 'dif'], axis = 1, inplace = True)
    except:
        period_index.drop('total_testeo', axis = 1, inplace = True)
    ###########################################################################
    print('Creando la relación de p2p')
    # En esta sección haremos un merge para sacar los valores en qué P2P se encontraba el contrato #P2P Date Out, AC Effective Date
    q0 = '''SELECT id_contrato as "Loan ID", nombrepq as P2P, DATE(fecha_entrada) as "P2P Date IN" 
    FROM usulatie_bbdd.logs_p2p as a
    LEFT JOIN usulatie_bbdd.def_p2p as b on b.id = p2p_nuevo   
    GROUP BY id_contrato, DATE(fecha_entrada), nombrepq
    ORDER BY id_contrato, fecha_entrada'''
    time_window_p2p = pd.read_sql(q0, db) 
    
    #Hacemos un shift bajando el renglón de fecha_entrada n+1 al n de fecha_salida y llenamos los valores nulos con la fecha actual
    time_window_p2p['P2P Date Out'] = time_window_p2p.groupby(['Loan ID'])[['P2P Date IN']].shift(-1)
    idx_f_salida = time_window_p2p[time_window_p2p['P2P Date Out'].isna()].index
    time_window_p2p.loc[idx_f_salida, 'P2P Date Out'] = datetime.date.today()
        
    #Hacemos un nuevo array donde tendremos un explode 
    time_window_p2p['AC Effective Date'] = time_window_p2p.apply(
    lambda x: pd.date_range(start=x['P2P Date IN'], end=x['P2P Date Out'], freq='D') if pd.notna(x['P2P Date IN']) and pd.notna(x['P2P Date Out']) else [],
    axis=1)
    rangos1 = time_window_p2p.explode('AC Effective Date').reset_index(drop=True) 
    rangos1['AC Effective Date'] = pd.to_datetime(rangos1['AC Effective Date'], format = '%Y-%m-%d').dt.date
    
    #Vamos a agrupar por Loan ID y AC Effective Date para quedarnos sólo con el primer registro
    rangos1 = rangos1.groupby(['Loan ID', 'AC Effective Date']).first().reset_index()
    
    
    period_index = pd.merge(period_index, rangos1, on = ['Loan ID', 'AC Effective Date'], how = 'left')
    
    time_window_p2p['Last Month Day'] = time_window_p2p.apply(lambda x: pd.date_range(start=x['P2P Date IN'], end=x['P2P Date Out'], freq='D') if pd.notna(x['P2P Date IN']) and pd.notna(x['P2P Date Out']) else [], axis=1)
    rangos2 = time_window_p2p.explode('Last Month Day').reset_index(drop=True) 
    idx_fechanula = rangos2[rangos2['Last Month Day'].isna()].index
    rangos2.loc[idx_fechanula, 'Last Month Day'] = rangos2.loc[idx_fechanula, 'P2P Date Out']
    
    rangos2['Last Month Day'] = pd.to_datetime(rangos2['Last Month Day'], format = '%Y-%m-%d').dt.date
    rangos2['Last Month Day'] = rangos2['Last Month Day'] - timedelta(days=1)    
    rangos2['EOM Loan Book YM'] = np.vectorize(mes_fecha)(rangos2['Last Month Day'])
    rangos2.rename(columns ={'P2P':'EOM P2P'}, inplace = True)
    rangos2['EOM Loan Book YM'] = rangos2['EOM Loan Book YM'].astype('float')
    
    rangos2 = rangos2.groupby(['Loan ID', 'EOM Loan Book YM']).last().reset_index()
    period_index = pd.merge(period_index, rangos2[['Loan ID', 'EOM P2P', 'EOM Loan Book YM']], on = ['Loan ID', 'EOM Loan Book YM'], how = 'left') 
    
    #Dropemaos las columnas que unimos
    try:
        period_index.drop(['idcontratos', 'principal'], axis = 1, inplace = True)
    except:
        pass
    
    ###########################################################################
    print('Arreglamos aquellos contratos que han tenido errores al momento de organizar lo pagos por fecha, debido a que hubo un reverso de pago')
    #Habrá casos en los que, al hacer el cambio de líneas en contratos para garantizar que el siguiente renglón sea menor o igual al actual 
    #modifique  el verdadero principal outstanding, modifiquemos el EOM Principal Outstanding. Para arreglar eso,  
    #Sacamos el valor mínimo del EOM Principal Outstanding con AC Effective Date no sea nula y comparamos con el último
    #renglón del contrato fijándonos en la columna EOM Principal Outstanding
    index_max_pago = period_index[~period_index['AC Effective Date YM'].isna()].groupby('Loan ID').agg(
                        {'EOM Principal Outstanding':'min', 'AC Effective Date YM':'max'}).reset_index().rename(
                        columns = {'EOM Principal Outstanding':'min_PO_pagado', 'AC Effective Date YM':'ym_PO_pagado'})
    
    index_max = period_index.groupby('Loan ID').last().reset_index().rename(columns = {'EOM Principal Outstanding':'min_PO'})
    
    #Hacemos el merge con la tabla principal
    period_index = pd.merge(period_index, index_max_pago, on = 'Loan ID', how = 'left')
    period_index = pd.merge(period_index, index_max[['Loan ID', 'min_PO']], on = 'Loan ID', how = 'left')
    
    #Traeremos todos los valores que diferan las columnas min_PO_pagado y min_PO. Sólo con este filtro estaremos trayendo
    # que incluso difieren debido a que pagaron cuotas adelantadas en un mes y eso hace que, el ym del último pago no
    #concuerde con el ym del cierre de mes
    filtro1 = period_index[period_index['min_PO_pagado'] != period_index['min_PO']]
    loans_filter1 = filtro1[(filtro1['ym_PO_pagado'] == filtro1['EOM Loan Book YM']) & (filtro1['EOM Principal Outstanding'] == filtro1['min_PO'])]['Loan ID'].to_list()
    filtro2 = filtro1[~filtro1['Loan ID'].isin(loans_filter1)].copy()
    
    #Finalmente quitaremos aquellos Loan ID cuyo min_PO != 0 y que EOM Principal Outstanding sea distinto de nulo cuando ym_PO_pagado < EOM Loan Book YM
    idx_error = filtro2[(filtro2['min_PO'] != 0) & (filtro2['ym_PO_pagado'] < filtro2['EOM Loan Book YM']) & (~filtro2['EOM Principal Outstanding'].isna())].index    
    
    #La siguiente línea se comento debido a que AC Principal Outstanding se está calculando por primera vez abajo y no en la línea 1679-1693
    #period_index.loc[idx_error, ['AC Principal Oustanding', 'EOM Principal Outstanding']] = period_index.loc[idx_error, 'min_PO_pagado']
    period_index.loc[idx_error, ['EOM Principal Outstanding']] = period_index.loc[idx_error, 'min_PO_pagado']
    
    #Dropeamos las columnas auxiliares que construimos
    period_index.drop(['min_PO', 'min_PO_pagado', 'ym_PO_pagado'], axis = 1, inplace = True)
    
    #Sacaremos la lógica de principal Recovered. Donde vamos a hacer dos lógicas diferentes: para los contratos menos o iguales al 3318 y después
    #para los demás usuarios. Primero haremos una suma acumalada de los valores de AC Principal y después limitaremos al capital prestado
    #Creamos una columna auxiliar para las cancelaciones
    period_index['Principal Recovered'] = (period_index[['Loan ID', 'AC Principal']].fillna(0)).groupby('Loan ID')['AC Principal'].cumsum()
    period_index['Principal Recovered'] = period_index.groupby('Loan ID')['Principal Recovered'].ffill()

    #Sacaremos los indices de los usuarios que son menores o iguales al contrato 3318    
    idx_less_3318 = period_index[period_index['Loan ID'] <= 3318].index
    period_index.loc[idx_less_3318, 'Principal Recovered'] = period_index.loc[idx_less_3318, ['Principal Recovered', 'Loan Amount']].apply(lambda row: row['Principal Recovered'] if row['Principal Recovered'] <= row['Loan Amount'] else (row['Loan Amount'] if pd.notna(row['Principal Recovered']) else np.nan), axis = 1)
       
    idx_great_3318 = period_index[period_index['Loan ID'] > 3318].index
    period_index.loc[idx_great_3318, 'Principal Recovered'] = period_index.loc[idx_great_3318, ['Principal Recovered', 'Loan Amount w/Origination Fee']].apply(lambda row: row['Principal Recovered'] if row['Principal Recovered'] <= row['Loan Amount w/Origination Fee'] else (row['Loan Amount w/Origination Fee'] if pd.notna(row['Principal Recovered']) else np.nan), axis = 1)
    
    #Finalmente llenamos los valores restantes con cero
    period_index.fillna({'Principal Recovered': 0}, inplace = True)
    
    #Ahora crearemos la columna AC Principal Oustanding restando Loan Amount o Loan Amount w/ Origination Fee, según sea el caso
    aux_period_index = period_index[period_index['Restructured Loan ID'] == -1].groupby('Loan ID', as_index = False).first()
    aux_period_index.rename(columns = {'Loan Amount': 'AUX Loan Amount','Origination Fee': 'AUX Origination Fee'}, inplace = True)
    period_index = pd.merge(period_index, aux_period_index[['Loan ID', 'AUX Loan Amount', 'AUX Origination Fee']], on ='Loan ID', how = 'left')
    
    period_index.loc[idx_less_3318, 'AC Principal Oustanding'] = period_index.loc[idx_less_3318, ['Principal Recovered', 'AUX Loan Amount']].apply(lambda row: row['AUX Loan Amount'] - row['Principal Recovered'], axis = 1)
    
    period_index.loc[idx_great_3318, 'AC Principal Oustanding'] = period_index.loc[idx_great_3318, ['Principal Recovered', 'AUX Loan Amount', 'AUX Origination Fee', 'Pending Origination Fee']].apply(lambda row: row['AUX Loan Amount'] - ( row['Principal Recovered'] + (row['AUX Origination Fee'] - row['Pending Origination Fee']) ), axis = 1 )
    period_index.drop(['AUX Loan Amount', 'AUX Origination Fee'], axis = 1, inplace = True)
    if geo == 'MX':
        pass
    else:
        #5. De acuerdo a los reportes de finanzas, los contratos que van del 9 al 700 son amortizados y un principal outstanding de cero, excepto los que
        #agreguemos en la siguiente lista
        excepciones_finanzas = [600, 277, 577, 108, 430, 546]
        amortizar_finanzas = list(range(9,701))
        for i in excepciones_finanzas:
            amortizar_finanzas.remove(i)
    
        idx_amortizar = period_index[(period_index['Loan ID'].isin(amortizar_finanzas)) & (period_index['EOM Principal Outstanding'] != 0) & (period_index['EOM Loan Book YM'] >= 202101)].index
        period_index.loc[idx_amortizar, ['AC Principal Oustanding', 'EOM Principal Outstanding']] = 0
        period_index.loc[idx_amortizar, 'EOM Status'] = 'Amortized'

    ########################################################################### 
    #Acomodamos las últimas columnas creadas
    col_period_index = list(period_index.columns)
    idx_feature = col_period_index.index('Pending Origination Fee')
    a = col_period_index[:idx_feature]
    b = col_period_index[-1:] + col_period_index[-2:-1]
    c = col_period_index[idx_feature:-2]
    col_period_index = a + b +c  
    period_index = period_index[col_period_index].copy()
    
    ######################### PRELACION CANCELACIONES #########################
    ########################### CASO 1 AMPLIACIONES ###########################
    #Vamos a sacar 4 casos para las cancelaciones. Aquellas cancelaciones que son por ampliación y las que son por pago
    #Además, cada una de ellas se dividirá en dos, aquellos contratos que son menores o iguales al 3318, que son los casos en
    #los que en la tabla de cuotas la Cuota de Originación estaba mezclada con el capital y por ende es nula esa columna
    
    if geo == 'ESP':
        idx_ampliaciones1 = period_index[(period_index['Extension'] == 'Extension') & (period_index['AC Early Prepayment'] > 0) & (period_index['Loan ID'] <= 3318)].index
        period_index.loc[idx_ampliaciones1, 'AC Principal'] = period_index.loc[idx_extension, 'AC Early Prepayment']
        
        idx_ampliaciones2 = period_index[(period_index['Extension'] == 'Extension') & (period_index['AC Early Prepayment'] > 0) & (period_index['Loan ID'] > 3318)].index
        period_index.loc[idx_ampliaciones2, 'AC Origination Fee'] = period_index.loc[idx_ampliaciones2, 'Pending Origination Fee']
        period_index.loc[idx_ampliaciones2, 'AC Principal'] = period_index.loc[idx_ampliaciones2, 'AC Early Prepayment'] - period_index.loc[idx_ampliaciones2, 'AC Origination Fee']
    else:
        idx_ampliaciones2 = period_index[(period_index['Extension'] == 'Extension') & (period_index['AC Early Prepayment'] > 0) ].index
        period_index.loc[idx_ampliaciones2, 'AC Origination Fee'] = period_index.loc[idx_ampliaciones2, 'Pending Origination Fee']
        period_index.loc[idx_ampliaciones2, 'AC Principal'] = period_index.loc[idx_ampliaciones2, 'AC Early Prepayment'] - period_index.loc[idx_ampliaciones2, 'AC Origination Fee']
    
    ############################### CASO 2 PAGOS ##############################
    
    idx_amtz= period_index[(period_index['Extension'] != 'Extension') & (period_index['AC Early Prepayment'] > 0)].index
    #Sacamos por un group la suma de todos los pagos que han hecho los usuarios
    sum_amortizaciones = period_index[period_index['Loan ID'].isin(period_index.loc[idx_amtz, 'Loan ID'].to_list())].groupby('Loan ID').agg( hold_principal=('AC Principal', 'sum'), hold_ga=('AC Origination Fee','sum'))
    sum_amortizaciones.set_index( idx_amtz, inplace = True)
    
    period_index = pd.merge(period_index, sum_amortizaciones, left_index=True, right_index=True, how = 'left')
    if geo == 'MX':
        for idx in idx_amtz:
            if period_index.loc[idx, 'Loan Amount'] - period_index.loc[idx, 'Origination Fee'] - period_index.loc[idx, 'hold_principal'] < period_index.loc[idx, 'AC Early Prepayment']:
                residuo1 = period_index.loc[idx, 'Loan Amount'] - period_index.loc[idx, 'Origination Fee'] - period_index.loc[idx, 'hold_principal']
                period_index.loc[idx, 'AC Principal'] = round(residuo1, 2)
                
                if period_index.loc[idx, 'Pending Origination Fee']*(1+iva_applicated) < period_index.loc[idx, 'AC Early Prepayment']-residuo1:
                    period_index.loc[idx, 'AC Origination Fee'] = round(period_index.loc[idx, 'Pending Origination Fee'], 2)
                    period_index.loc[idx, 'AC Origination Fee VAT']= round(period_index.loc[idx, 'Pending Origination Fee']*iva_applicated, 2)
                    residuo2 = period_index.loc[idx, 'AC Origination Fee'] + period_index.loc[idx, 'AC Origination Fee VAT']
                    
    ###############################################################################
                    #Para la siguiente parte necesitaremos tener un respaldo en días para saber a qué concepto se estaría yendo el dinero
                    #Es decir, si es que hay cuota en servicios o en intereses. Vamos a calcular la diferencia en días entre la cuota
                    #teórica actual y la siguiente y esta debe ser menor a la diferencia en días entre el pago y la cuota actual para usar
                    # el valor schedule, caso contratio; empezaremos con el valor proporcional en servicios (ya que siempre es una cuota
                    #constante) y con su iva, lo restante lo mandamos a IVA
                    
                    diff_dias_pago = (pd.to_datetime(period_index.loc[idx, 'AC Effective Date']) - pd.to_datetime(period_index.loc[idx, 'Scheduled Payment Date'])).days
                    diff_dias_prestamo = (pd.to_datetime(period_index.loc[idx+1, 'Scheduled Payment Date']) - pd.to_datetime(period_index.loc[idx, 'Scheduled Payment Date'])).days
                    rate = diff_dias_pago/diff_dias_prestamo
                    reserva= round((period_index.loc[idx, 'AC Early Prepayment']-residuo1-residuo2)/(1+iva_applicated), 2) 
                    
                    if (rate <= 1) & (0 < reserva):
                        if period_index.loc[idx,'Scheduled Service Fee'] == 0:
                            period_index.loc[idx, 'AC Service Fee'] = 0
                            period_index.loc[idx, 'AC Service Fee VAT'] = 0
                            period_index.loc[idx, 'AC Interest'] = reserva
                            period_index.loc[idx, 'AC Interest VAT'] = round(period_index.loc[idx, 'AC Interest']*iva_applicated, 2)
                            
                        else:
                            period_index.loc[idx, 'AC Service Fee'] = round((diff_dias_pago/diff_dias_prestamo)*(period_index.loc[idx, 'Scheduled Service Fee']-residuo1-residuo2)/(1+iva_applicated), 2) 
                            period_index.loc[idx, 'AC Service Fee VAT'] = period_index.loc[idx, 'AC Service Fee']*iva_applicated
                            residuo3 = period_index.loc[idx, 'AC Service Fee'] - period_index.loc[idx, 'AC Service Fee VAT']
                            
                            period_index.loc[idx, 'AC Interest'] = round((period_index.loc[idx, 'AC Early Prepayment']-residuo1-residuo2-residuo3)/(1+iva_applicated), 2) 
                            period_index.loc[idx, 'AC Interest VAT'] = round(period_index.loc[idx, 'AC Interest']*iva_applicated, 2)
                            
                    elif (1 < rate) & (0 < reserva):
                        j = int(rate)
                        servicios_pago = 0
                        for i in list(range(0,j+1)):
                            servicios_pago += period_index.loc[idx + j, 'Scheduled Service Fee']
                            if period_index.loc[idx + j, 'AC Service Fee'] != 0:
                                service_saved = period_index.loc[idx + j, 'Scheduled Service Fee']
                            else:
                                pass
                            
                        period_index.loc[idx, 'AC Service Fee'] = servicios_pago + round((rate - j)*(service_saved), 2)
                        period_index.loc[idx, 'AC Service Fee VAT'] = period_index.loc[idx, 'AC Service Fee']*iva_applicated
                        residuo4 = period_index.loc[idx, 'AC Service Fee'] + period_index.loc[idx, 'AC Service Fee VAT']
                        
                        period_index.loc[idx, 'AC Interest'] = round((period_index.loc[idx, 'AC Early Prepayment']-residuo1-residuo2-residuo4)/(1+iva_applicated), 2) 
                        period_index.loc[idx, 'AC Interest VAT'] = round(period_index.loc[idx, 'AC Interest']*iva_applicated, 2)
    ###############################################################################
                else:
                    
                    if period_index.loc[idx, 'AC Early Prepayment']-residuo1 <= period_index.loc[idx, 'Pending Origination Fee']:
                        period_index.loc[idx, 'AC Origination Fee'] = round((period_index.loc[idx, 'AC Early Prepayment']-residuo1), 2)
                        period_index.loc[idx, 'AC Origination Fee VAT'] = 0
                        period_index.loc[idx, 'Warning'] = '*'
                    
                    else:
                        period_index.loc[idx, 'AC Origination Fee'] = round((period_index.loc[idx, 'AC Early Prepayment']-residuo1), 2)
                        period_index.loc[idx, 'AC Origination Fee VAT'] = round(period_index.loc[idx, 'AC Early Prepayment']-residuo1-period_index.loc[idx, 'AC Origination Fee'], 2)
                        period_index.loc[idx, 'Warning'] = '*'
                        
                    period_index.loc[idx, 'AC Interest'] = 0
                    period_index.loc[idx, 'AC Interest VAT'] = 0
                    period_index.loc[idx, 'AC Service Fee'] = 0
                    period_index.loc[idx, 'AC Service Fee VAT'] = 0
                    
                            
            else:
                period_index.loc[idx, 'AC Principal'] = round(period_index.loc[idx, 'AC Early Prepayment'],2)
                period_index.loc[idx, 'AC Origination Fee'] = 0
                period_index.loc[idx, 'AC Origination Fee VAT'] = 0
                period_index.loc[idx, 'AC Interest'] = 0
                period_index.loc[idx, 'AC Interest VAT'] = 0
                period_index.loc[idx, 'AC Service Fee'] = 0
                period_index.loc[idx, 'AC Service Fee VAT'] = 0
                period_index.loc[idx, 'Warning'] = '*'
                
    else:
        
        #Corregir la parte en que sólo es distinto a Extension. Los índices que tienen Extension != Extension y AC Early Prepayment > 0, 
        #para estos valores haremos la división de los casos menores y mayores al contrato 3318
        idx_amtz0= period_index[(period_index['Extension'] != 'Extension') & (period_index['AC Early Prepayment'] > 0) & (period_index['Loan ID'] <= 3318)].index    
        for idx in list(idx_amtz0):
            if round(period_index.loc[idx, 'Loan Amount'] - period_index.loc[idx, 'hold_principal'], 2) <= period_index.loc[idx, 'AC Early Prepayment']:
                period_index.loc[idx, 'AC Principal'] = round(period_index.loc[idx, 'Loan Amount'] - period_index.loc[idx, 'hold_principal'], 2)
                period_index.loc[idx, 'AC Interest'] = period_index.loc[idx, 'AC Early Prepayment'] - period_index.loc[idx, 'AC Principal']
                
            else:
                period_index.loc[idx, 'AC Principal'] = period_index.loc[idx, 'AC Early Prepayment']
        
        idx_amtz = period_index[(period_index['Extension'] != 'Extension') & (period_index['AC Early Prepayment'] > 0) & (period_index['Loan ID'] > 3318)].index
        
        for idx in idx_amtz:
            if round(period_index.loc[idx, 'Loan Amount'] - period_index.loc[idx, 'Origination Fee'] - period_index.loc[idx, 'hold_principal'], 2) <= period_index.loc[idx, 'AC Early Prepayment']:
                period_index.loc[idx, 'AC Principal'] = round(period_index.loc[idx, 'Loan Amount'] - period_index.loc[idx, 'Origination Fee'] - period_index.loc[idx, 'hold_principal'], 2)
                
                if period_index.loc[idx, 'AC Early Prepayment'] - period_index.loc[idx, 'AC Principal'] >= period_index.loc[idx, 'Pending Origination Fee']:
                    period_index.loc[idx, 'AC Origination Fee'] = round(period_index.loc[idx, 'Pending Origination Fee'], 2)
                    hold = period_index.loc[idx, 'AC Early Prepayment'] - (period_index.loc[idx, 'AC Origination Fee'] + period_index.loc[idx, 'AC Principal'])
                    if hold >= iva_applicated*period_index.loc[idx, 'AC Origination Fee']:
                        period_index.loc[idx, 'AC Origination Fee VAT'] = round(iva_applicated*period_index.loc[idx, 'AC Origination Fee'], 2)
                        hold = hold -period_index.loc[idx, 'AC Origination Fee VAT']
                        if hold >= 0:
                            period_index.loc[idx, 'AC Interest'] = hold
                            period_index.loc[idx, 'AC Interest VAT'] = 0
                            period_index.loc[idx, 'AC Service Fee'] = 0
                            period_index.loc[idx, 'AC Service Fee VAT'] = 0
                        else:
                            period_index.loc[idx, 'AC Interest'] = 0
                            period_index.loc[idx, 'AC Interest VAT'] = 0
                            period_index.loc[idx, 'AC Service Fee'] = 0
                            period_index.loc[idx, 'AC Service Fee VAT'] = 0
                    else:
                        period_index.loc[idx, 'AC Origination Fee VAT'] = hold
                        period_index.loc[idx, 'AC Interest'] = 0
                        period_index.loc[idx, 'AC Interest VAT'] = 0
                        period_index.loc[idx, 'AC Service Fee'] = 0
                        period_index.loc[idx, 'AC Service Fee VAT'] = 0                       
                else: 
                    period_index.loc[idx, 'AC Origination Fee'] = round(period_index.loc[idx, 'AC Early Prepayment'] - period_index.loc[idx, 'AC Principal'], 2)
                    period_index.loc[idx, 'AC Origination Fee VAT']= 0
                    period_index.loc[idx, 'AC Interest'] = 0
                    period_index.loc[idx, 'AC Interest VAT'] = 0
                    period_index.loc[idx, 'AC Service Fee'] = 0
                    period_index.loc[idx, 'AC Service Fee VAT'] = 0           
            else:
                period_index.loc[idx, 'AC Principal'] = period_index.loc[idx, 'AC Early Prepayment']
                period_index.loc[idx, 'AC Origination Fee'] = 0
                period_index.loc[idx, 'AC Origination Fee VAT']= 0
                period_index.loc[idx, 'AC Interest'] = 0
                period_index.loc[idx, 'AC Interest VAT'] = 0
                period_index.loc[idx, 'AC Service Fee'] = 0
                period_index.loc[idx, 'AC Service Fee VAT'] = 0
            
############################## Esta sección es similar a la de arriba, sólo que como modificamos el AC Principal, tedremos que modificar todo de nuevo##############           
    
#Sacaremos la lógica de principal Recovered. Donde vamos a hacer dos lógicas diferentes: para los contratos menos o iguales al 3318 y después
    #para los demás usuarios. Primero haremos una suma acumalada de los valores de AC Principal y después limitaremos al capital prestado
    #Creamos una columna auxiliar para las cancelaciones
    period_index['Principal Recovered'] = (period_index[['Loan ID', 'AC Principal']].fillna(0)).groupby('Loan ID')['AC Principal'].cumsum()
    period_index['Principal Recovered'] = period_index.groupby('Loan ID')['Principal Recovered'].ffill()

    #Sacaremos los indices de los usuarios que son menores o iguales al contrato 3318    
    idx_less_3318 = period_index[period_index['Loan ID'] <= 3318].index
    period_index.loc[idx_less_3318, 'Principal Recovered'] = period_index.loc[idx_less_3318, ['Principal Recovered', 'Loan Amount']].apply(lambda row: row['Principal Recovered'] if row['Principal Recovered'] <= row['Loan Amount'] else (row['Loan Amount'] if pd.notna(row['Principal Recovered']) else np.nan), axis = 1)
       
    idx_great_3318 = period_index[period_index['Loan ID'] > 3318].index
    period_index.loc[idx_great_3318, 'Principal Recovered'] = period_index.loc[idx_great_3318, ['Principal Recovered', 'Loan Amount w/Origination Fee']].apply(lambda row: row['Principal Recovered'] if row['Principal Recovered'] <= row['Loan Amount w/Origination Fee'] else (row['Loan Amount w/Origination Fee'] if pd.notna(row['Principal Recovered']) else np.nan), axis = 1)
    
    #Finalmente llenamos los valores restantes con cero
    period_index.fillna({'Principal Recovered': 0}, inplace = True)
    
    
    #Ahora crearemos la columna AC Principal Oustanding restando Loan Amount o Loan Amount w/ Origination Fee, según sea el caso
    aux_period_index = period_index[period_index['Restructured Loan ID'] == -1].groupby('Loan ID', as_index = False).first()
    aux_period_index.rename(columns = {'Loan Amount': 'AUX Loan Amount','Origination Fee': 'AUX Origination Fee'}, inplace = True)
    period_index = pd.merge(period_index, aux_period_index[['Loan ID', 'AUX Loan Amount', 'AUX Origination Fee']], on ='Loan ID', how = 'left')
    period_index['Pending Origination Fee'] = (period_index.fillna({'AC Origination Fee': 0})).groupby('Loan ID')['AC Origination Fee'].cumsum()
    period_index['Pending Origination Fee'] = period_index['AUX Origination Fee'] - period_index['Pending Origination Fee']
    
    period_index.loc[idx_less_3318, 'AC Principal Oustanding'] = period_index.loc[idx_less_3318, ['Principal Recovered', 'AUX Loan Amount']].apply(lambda row: row['AUX Loan Amount'] - row['Principal Recovered'], axis = 1)
    
    period_index.loc[idx_great_3318, 'AC Principal Oustanding'] = period_index.loc[idx_great_3318, ['Principal Recovered', 'AUX Loan Amount', 'AUX Origination Fee', 'Pending Origination Fee']].apply(lambda row: row['AUX Loan Amount'] - ( row['Principal Recovered'] + (row['AUX Origination Fee'] - row['Pending Origination Fee']) ), axis = 1 )
    period_index.drop(['AUX Loan Amount', 'AUX Origination Fee'], axis = 1, inplace = True)
    if geo == 'MX':
        pass
    else:
        #5. De acuerdo a los reportes de finanzas, los contratos que van del 9 al 700 son amortizados y un principal outstanding de cero, excepto los que
        #agreguemos en la siguiente lista
        excepciones_finanzas = [600, 277, 577, 108, 430, 546]
        amortizar_finanzas = list(range(9,701))
        for i in excepciones_finanzas:
            amortizar_finanzas.remove(i)
    
        idx_amortizar = period_index[(period_index['Loan ID'].isin(amortizar_finanzas)) & (period_index['EOM Principal Outstanding'] != 0) & (period_index['EOM Loan Book YM'] >= 202101)].index
        period_index.loc[idx_amortizar, ['AC Principal Oustanding', 'EOM Principal Outstanding']] = 0
        period_index.loc[idx_amortizar, 'EOM Status'] = 'Amortized'
        
    period_index['Pending Origination Fee'] = period_index.groupby(['Loan ID'])['Pending Origination Fee'].ffill()
    period_index['Principal Recovered'] = period_index.groupby(['Loan ID'])['Principal Recovered'].ffill()
############################## Esta sección es similar a la de arriba, sólo que como modificamos el AC Principal, tedremos que modificar todo de nuevo##############
        
    #Vamos a cambiar los valores CPL que sean RPL, para eso traeremos los campos donde 
    q10 = '''SELECT id AS 'Loan ID', EXTRACT(YEAR_MONTH FROM coche_cambio_trafico) AS ym_cambio_trafico
    FROM usulatie_bbdd.contratos
    WHERE coche_cambio_trafico IS NOT NULL;'''
    rpl = pd.read_sql(q10, db)
    
    period_index = pd.merge(period_index, rpl, on = 'Loan ID', how = 'left')
    idx_rpl = period_index[(period_index['ym_cambio_trafico'].notna()) & (period_index['ym_cambio_trafico'] > period_index['EOM Loan Book YM']) & (period_index['EOM Status'] == 'CPL')].index
    period_index.loc[idx_rpl , 'EOM Status'] = 'RPL'
    
    idx_rpl1 = period_index[(period_index['ym_cambio_trafico'].isna()) & (period_index['EOM Status'] == 'CPL')].index
    period_index.loc[idx_rpl1, 'EOM Status'] = 'RPL'
    
    period_index.drop(['hold_principal', 'hold_ga', 'ym_cambio_trafico'], axis = 1, inplace = True)
    
    # Ya que en las amortizaciones no se paga IVA del gasto de apertura, sumaremos ese IVA hacia el concepto de intereses
    idx_amortiza1 = period_index[(period_index['AC Early Prepayment'] != 0) & (period_index['AC Origination Fee VAT'] > 0) & (period_index['Loan ID'] < 12801) & (pd.to_datetime('2024-10-01') <= pd.to_datetime(period_index['AC Effective Date'], errors = 'coerce'))].index
    idx_amortiza2 = period_index[(period_index['AC Early Prepayment'] != 0) & (period_index['AC Origination Fee VAT'] > 0) & ( pd.to_datetime(period_index['AC Effective Date'], errors = 'coerce') < pd.to_datetime('2024-10-01') )].index
    idx_amortiza = list(idx_amortiza1) + list(idx_amortiza2)
    period_index.loc[idx_amortiza, 'AC Interest'] = period_index.loc[idx_amortiza, 'AC Interest'] + period_index.loc[idx_amortiza, 'AC Origination Fee VAT']
    period_index.loc[idx_amortiza, 'AC Origination Fee VAT'] = 0
    

    # ============================================================================================
    if destiny == 'xlsx':
        print('Generando el excel...')
        period_index.to_excel(
            f'7. Loan Tape Stratification Collections {datetime.date.today()}_{geo}.xlsx',
            index=False
        )

    # VERSION .PARQUET AÑADIDA POR DAVID
    elif destiny == 'parquet':
        print('Generando el parquet...')
        period_index.to_parquet(
            f'7. Loan Tape Stratification Collections {datetime.date.today()}_{geo}.parquet',
            index=False,
            engine="pyarrow"
        )

    elif destiny == 'db':
        print('Cargando en la base de datos...')
        database_engine(geo, period_index, table_name, if_exists)

    else:
        print('El dataframe sólo está en la memoria, no se ha guardado en ningún archivo externo.')
        print('Revise el parámetro destiny de la función')

     # ============================================================================================
    
    #Agrupamos por year month la recuperación de cada uno de los buckets que tenemos
    cols = ['AC Payment Amount', 'AC Principal', 'AC Origination Fee', 'AC Origination Fee VAT', 'AC Interest', 'AC Interest VAT',
       'AC Service Fee', 'AC Service Fee VAT', 'AC Late Fees & Prepayment Fees', 'AC Late Fees & Prepayment Fees VAT', 'AC Other Fees']
    
    df = period_index.groupby(['AC Effective Date YM', 'AC Effective Date', 'Acuerdo'])[cols].sum().reset_index()
    comparacion = pd.DataFrame()
    comparacion['YM'] = df['AC Effective Date YM']
    comparacion['date'] = df['AC Effective Date']
    comparacion['Amortized'] = df[df['Acuerdo'] != 2]['AC Principal']
    comparacion['Car_sale_revenue'] = df[df['Acuerdo'] == 2]['AC Principal']
    comparacion['Origination'] = df['AC Origination Fee']
    comparacion['Services'] = df['AC Service Fee']
    comparacion['Interest'] = df['AC Interest'] + df['AC Late Fees & Prepayment Fees']
    comparacion['Full_Interest'] = df['AC Interest'] + df['AC Late Fees & Prepayment Fees'] + df['AC Other Fees']
    comparacion['VAT'] = df['AC Origination Fee VAT'] + df['AC Service Fee VAT'] + df['AC Interest VAT'] + df['AC Late Fees & Prepayment Fees VAT']
    
    
    
    cols1 = ['Amortized', 'Car_sale_revenue', 'Origination', 'Services', 'Interest', 'Full_Interest', 'VAT']
    comparacion_final = comparacion.groupby(['YM', 'date'])[cols1].sum().reset_index()
    comparacion_final.to_excel('comparacion_mes_mes_maestra.xlsx', sheet_name= table_name)
    
    return period_index
    
def spv_report(loan_tape_copy):
    def pivote_type(df, kind, cols):
            if kind == 'OSB':
                dt= pd.pivot_table(df, values='EOM Principal Outstanding', index= ['Loan ID', 'Loan Origination Date'], columns= 'Loan Book YM', aggfunc="sum", fill_value = 0)
                dt = dt[cols].reset_index()
            else:
                dt= pd.pivot_table(df, values='EOM Status', index= ['Loan ID', 'Loan Origination Date'], columns= 'Loan Book YM', aggfunc="sum", fill_value = 0)
                dt = dt[cols].reset_index()
            return dt
    
    print('Extrayendo los valores de los contratos que están en KNURU')
    contratos_knuru = pd.read_excel(r'G:\Unidades compartidas\Credit Risk Management\04 - Credit Risk Ad-hoc tasks\20. SPV Knuru\Historic_Ever SPV Loans.xlsx')
    
    id_knuru = contratos_knuru[contratos_knuru.columns[0]].to_list()
    print(id_knuru[-10:])
        
    #Creamos una tabla de sólo fechabanco y Loan ID con el primer renglón para después hacer un merge
    aux_contratos = loan_tape_copy[loan_tape_copy['Loan ID'].isin(id_knuru)].groupby(['Loan ID']).first().reset_index()
    aux_contratos = aux_contratos[['Loan ID', 'Loan Origination Date']]
        
    #Nos quedamos sólo con los registros que contienen valores en EOM Principal Outstanding y nos aseguramos que no haya
    #periodos repetidos. Sólo tenemo el contrato 663 con el periodo 44 repetido, pero no está en el SPV histórico
    #Este archivo contiene las amortizaciones por ampliación ya que así se ha venido manejando siempre
    loan_tape_copy = loan_tape_copy[(loan_tape_copy['EOM Principal Outstanding'].notna()) & (loan_tape_copy['Loan ID'].isin(id_knuru))][['Loan ID', 'EOM Principal Outstanding', 'EOM Status', 'EOM Loan Book YM']]
        
    #Convertimos la columna EOM Loan Book YM a un Mes-YY
    print('Convirtiendo las columna year month a formato MMM-YY y redondenado el EOM Principal Outstanding')
    loan_tape_copy['Loan Book YM'] = (loan_tape_copy['EOM Loan Book YM'].astype('int')).apply(lambda x: pd.to_datetime(str(x), format='%Y%m').strftime('%b-%y'))
    loan_tape_copy['EOM Principal Outstanding'] = loan_tape_copy['EOM Principal Outstanding'].round(0)
    
    #Sacamos una lista para ordenar las columnas cuando se hagan el pivot  table
    columnas = loan_tape_copy[['EOM Loan Book YM', 'Loan Book YM']].copy()
    columnas = columnas.groupby('EOM Loan Book YM').first().reset_index()
    columnas.sort_values(by = ['EOM Loan Book YM'], inplace = True)
    columnas = columnas['Loan Book YM'].to_list()
    
    loan_tape_copy = pd.merge(loan_tape_copy, aux_contratos, on = 'Loan ID', how = 'left')
    idx_acuerdos = loan_tape_copy[(loan_tape_copy['EOM Status'] == 'RPL') | (loan_tape_copy['EOM Status'] == 'CPL')].index
    loan_tape_copy.loc[idx_acuerdos, 'EOM Principal Outstanding'] = 0
    
    print('Creando las tablas pivotes finales')
    osb = pivote_type(loan_tape_copy, 'OSB', columnas)
    status = pivote_type(loan_tape_copy, 'STATUS', columnas)
        
    spv_report = pd.merge(osb, status, on = ['Loan ID', 'Loan Origination Date'], how = 'left', suffixes=('', ' '))
    print('Creando el SPV Report')
    spv_report.to_excel(f'9. {datetime.date.today()} - SPV Report BI_{geo}.xlsx')

def loan_tape_full_installment(loan_tape_copy):
    #Traemos la conexión de la base de datos
    db = database(geo)
    
    loan_tape_copy = loan_tape_copy[loan_tape_copy['EOM Period'].notna()][['Loan ID', 'Loan Amount', 'Origination Fee', 'Loan Amount w/Origination Fee', 'EOM Status', 'EOM Period', 'AC Effective Date YM',
                                                                      'EOM Loan Book YM', 'EOM Principal Outstanding', 'AC Payment Amount', 'AC Principal', 'AC Origination Fee', 'AC Origination Fee VAT',
                                                                       'AC Interest', 'AC Late Fees & Prepayment Fees', 'AC Service Fee', 'AC Service Fee VAT', 'Car Sale €', 'Extension', 'AC Early Prepayment']]
    #Rellenamos los valores de los pagos nulos con cero
    print('Sellecionando sólo las columnas necesarias para las cuotas')
    cols_money = ['EOM Principal Outstanding', 'AC Payment Amount', 'AC Principal', 'AC Origination Fee', 'AC Origination Fee VAT', 'AC Interest', 'AC Late Fees & Prepayment Fees', 'AC Service Fee', 'AC Service Fee VAT', 'Car Sale €']
    dic_fill = dict(zip(cols_money, [0]*len(cols_money)))
    loan_tape_copy.fillna(dic_fill, inplace = True)
    
    #Creamos el primer dataframe donde agruparemos por Loan ID y AC Effective Date YM y sumaremos las columnas
    #Para esto excluiremos los valores que tenga Extension
    print('Seleccionando los renglones dee pagos que sean distintos a una extension')
    installment1 = (loan_tape_copy[loan_tape_copy['Extension'] != 'Extension'].groupby(['Loan ID', 'AC Effective Date YM']).agg(Principal_Outstanding = ('EOM Principal Outstanding', 'sum'), Installment = ('AC Payment Amount', 'sum'), Principal = ('AC Principal', 'sum'), Origination_Fee = ('AC Origination Fee', 'sum'), Origination_Fee_VAT = ('AC Origination Fee VAT', 'sum'),
                                                                            Interest = ('AC Interest', 'sum'), Late_Prepayment_Fees = ('AC Late Fees & Prepayment Fees', 'sum'), Service_Fee = ('AC Service Fee', 'sum'), Service_Fee_VAT = ('AC Service Fee VAT', 'sum'), Car_Sale = ('Car Sale €', 'sum')).reset_index()).rename(columns = {'AC Effective Date YM': 'EOM Loan Book YM'})
    installment1['EOM Loan Book YM'] = installment1['EOM Loan Book YM'].astype('int')
    
    #Creamos el segundo dataframe que nos ayude Sólo con las extensiones
    print('Seleccionando los pagos que corresponden a extensiones')
    installment2 = loan_tape_copy[loan_tape_copy['Extension'] == 'Extension'][['Loan ID', 'AC Effective Date YM', 'Extension', 'AC Early Prepayment', 'AC Principal', 'AC Origination Fee']].rename(columns = {'AC Principal': 'LTU  Principal', 'AC Origination Fee': 'LTU Origination Fee', 'AC Effective Date YM': 'EOM Loan Book YM'})
    
    #Creamos una tabla donde guardemos por cada contrato los periodos, el Loan Book y otra tabla donde este el primer monto prestado
    ltp_1 = (loan_tape_copy[loan_tape_copy['EOM Loan Book YM'].notna()].groupby(['Loan ID', 'EOM Period', 'EOM Loan Book YM'])['EOM Status'].last()).reset_index()
    ltp_2 = loan_tape_copy.groupby(['Loan ID']).first().reset_index()
    ltp = pd.merge(ltp_1, ltp_2[['Loan ID', 'Loan Amount', 'Origination Fee'] ], on = 'Loan ID', how ='left')
    ltp.rename(columns = {'EOM Status': 'Status_Change'}, inplace = True)
    
    #Hacemos los merge
    print('Uniendo las tablas')
    installment = pd.merge(ltp, installment1, on = ['Loan ID', 'EOM Loan Book YM'], how = 'left')
    installment = pd.merge(installment, installment2, on = ['Loan ID', 'EOM Loan Book YM'], how = 'left')
    
    #Rellenamos los valores vacíos
    installment['Status_Change'] = installment.groupby('Loan ID')['Status_Change'].ffill()
    installment['Principal_Outstanding'] = installment.groupby('Loan ID')['Principal_Outstanding'].ffill()
    
    cols_money = ['Installment', 'Principal', 'Origination_Fee', 'Origination_Fee_VAT', 'Interest', 'Late_Prepayment_Fees', 'Service_Fee', 'Service_Fee_VAT', 'Car_Sale', 'AC Early Prepayment', 'LTU Principal', 'LTU Origination Fee']
    dic_fill = dict(zip(cols_money, [0]*len(cols_money)))
    installment.fillna(dic_fill, inplace = True)
    
    list_id = '('+str(list(installment['Loan ID'].unique()))[1:-1]+')'
    
    print('Importando información del cliente de la base de datos')
    q1 = f'''SELECT a.id AS 'Loan ID', a.coche_matricula AS 'License', def_nifcif AS DNI 
            FROM usulatie_bbdd.contratos AS a 
            LEFT JOIN usulatie_bbdd.clientes AS b ON b.id = a.idclientes
            WHERE a.id IN {list_id};'''
    info_client = pd.read_sql(q1, db)
    
    installment = pd.merge(installment, info_client, on = 'Loan ID', how = 'left')
    
    print('Creando el archivo excel...')
    installment.to_excel(f'2. Loan Tape Actuals Full Instalment {datetime.date.today()} BI_{geo}.xlsx')
    
# NEXT STEPS 
#Separar o agrerar una columna que divida la cartera pos subsidiarias (Ibancar World = OPCO & Ibancar DebtCO = KNURU)
#Revisar la prelación de los pagos de cancelación para ver que cuadren los intereses y servicios Ejemplo: contrato 2169
#Loand Id revisar 3905 este contrato porque tiene un yearmonth 2202


#Change production by develop if it's necessary
#MEXICO
#host: ibancar-mx-production-mx.cluster-ctzdfj4hyolu.eu-west-1.rds.amazonaws.com
#username: usuibn_mx
#password: F5us#5O0RBtj

#España
#host: ibancar-develop.cluster-c538k0e1eqzj.eu-west-1.rds.amazonaws.com
#username: usulatie_naunet
#password: @SKq&5@A96

