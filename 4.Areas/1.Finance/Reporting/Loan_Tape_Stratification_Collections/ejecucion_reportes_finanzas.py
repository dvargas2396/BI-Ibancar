# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 08:39:33 2024
@author: allan
"""
import sys
import os
from pathlib import Path
from time import time

# Configurar rutas dinámicamente
current_dir = Path(__file__).parent
print(f"📁 Directorio de trabajo: {current_dir}")

# Agregar el directorio actual al path para importar reporteria_finanzas
sys.path.append(str(current_dir))

# Crear carpetas si no existen
output_dir = current_dir / "output"
temp_dir = current_dir / "temp"
output_dir.mkdir(exist_ok=True)
temp_dir.mkdir(exist_ok=True)

tiempo_inicial = time() 

import reporteria_finanzas as rp

print("="*80)
print("    🏦 REPORTE LTSC - LOAN TAPE STRATIFICATION COLLECTIONS")
print("="*80)

#RECUERDA CAMBIAR EN EL MODULO reporteria_finanzas LA NOMENCLATURA MEX O ESP DEPENDE QUE GEOGRAFIA QUIERES CONCLUIR

# ========== CONFIGURACIÓN DEL REPORTE ==========
# Cambiar estos valores según necesidad:

GEOGRAFIA = 'ESP'  # 'ESP' o 'MX' 
FECHA_INICIO = '2017-01-01'
FECHA_CORTE = '2025-08-22'
FORMATO_SALIDA = 'parquet'  # 'xlsx', 'parquet' o 'db'

# ===============================================

print(f"🌍 Geografía: {GEOGRAFIA}")
print(f"📅 Fecha inicio: {FECHA_INICIO}")
print(f"📅 Fecha corte: {FECHA_CORTE}")
print(f"💾 Formato salida: {FORMATO_SALIDA}")
print(f"📂 Archivos se guardarán en: {output_dir}")

if FORMATO_SALIDA == 'parquet':
    print("🚀 Modo PARQUET activado - Ejecución más rápida para pruebas")
elif FORMATO_SALIDA == 'xlsx':
    print("📊 Modo EXCEL activado - Archivo completo para análisis")

print("="*80)

#Variables para modificar las fechas del reporte debe ser menor o igual. Es decir, se ingresa el último día del mes
starts = FECHA_INICIO
ym_s = int(starts[:4] + starts[5:7])
ys = int(starts[:4])
# ---------------------------------- #
cut_off = FECHA_CORTE
ym_co = int(cut_off[:4] + cut_off[5:7])
# ---------------------------------- #

#Variables para mandar a escribir
destiny = FORMATO_SALIDA
table_name = 'bi_loan_tape_collections'  # El nombre de la tabla o de la hoja
if_exists= 'replace' #Puede tomar los valores de fail', 'replace', 'append'

try:
    ############################################################################
    print('___________________________portfolio___________________________')
    #Creacion Asign_stats, escogiendo la fecha de hasta donde queremos recrear el reporte
    final, loan_tape_copy = rp.portfolio(ys, cut_off)

    print('___________________________payoff___________________________')
    # Creación de Payoff
    #rp.payoff(inicio= starts, corte=cut_off)

    print('___________________________vintage_pi___________________________')
    #Creacion de Vintage P+I (Principal + Interest)
    #rp.vintage_pi(final, cut_off, ym_co, ym_s)

    print('________________loan_tape_stratification_collection________________')
    #Creacion de loan_tape_stratification_collection
    period_index = rp.loan_tape_stratification_collection(loan_tape_copy, final, ym_s, destiny, table_name, if_exists)

    print('________________SPV_REPORT________________')
    #Creacion de SPV Report
    #spv_report = Bozhidara.spv_report(period_index)

    print('________________Loan_Tape_Full_Installment________________')
    #Creacion de Loan Tape Full Installment Report
    #loan_tape_full_installment = rp.loan_tape_full_installment(period_index)

    tiempo_final = round((time() - tiempo_inicial)/60, 1)
    
    print("="*80)
    print(f'✅ REPORTE COMPLETADO EXITOSAMENTE')
    print(f'⏱️  Tiempo de ejecución: {tiempo_final} minutos')
    
    # Mostrar información del archivo generado
    if destiny in ['xlsx', 'parquet']:
        print(f'📁 Archivo generado en: {output_dir}/')
        
        # Buscar el archivo generado
        import datetime
        expected_filename = f"7. Loan Tape Stratification Collections {datetime.date.today()}_{GEOGRAFIA}.{destiny}"
        expected_path = output_dir / expected_filename
        
        if expected_path.exists():
            file_size = expected_path.stat().st_size
            file_size_mb = file_size / (1024 * 1024)
            print(f'📊 Archivo: {expected_filename}')
            print(f'📏 Tamaño: {file_size_mb:.1f} MB')
            
            if destiny == 'parquet':
                print("💡 Para abrir el archivo parquet en Python:")
                print(f"   import pandas as pd")
                print(f"   df = pd.read_parquet('{expected_path}')")
        else:
            print(f"⚠️  No se encontró el archivo esperado: {expected_filename}")
    
    print("="*80)
    
except Exception as e:
    print("="*80)
    print(f"❌ ERROR DURANTE LA EJECUCIÓN:")
    print(f"   {str(e)}")
    print("="*80)
    import traceback
    traceback.print_exc()
    raise
finally:
    print(f"\n🕒 Tiempo total transcurrido: {round((time() - tiempo_inicial)/60, 1)} minutos")