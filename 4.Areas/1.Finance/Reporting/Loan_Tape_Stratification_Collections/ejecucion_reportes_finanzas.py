# -*- coding: utf-8 -*-
"""
Ejecutor de Reportería Financiera - Loan Tape Stratification Collections
Autor: allan
Fecha: 2024-02-23
Versión: 2.0
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from time import time

# Configuración de rutas relativas
current_dir = Path(__file__).parent
project_root = current_dir.parents[4]  # Llega hasta BI-Ibancar
output_dir = current_dir / "output"

# Agregar el directorio actual al path para importar el módulo
sys.path.append(str(current_dir))
import reporteria_finanzas as rp

# =============================================================================
# parámetros de configuración
# =============================================================================

# Configuración de fechas
fecha_inicio = '2017-01-01'
fecha_corte = '2025-02-01'
formato_salida = 'parquet'  # opciones: 'xlsx', 'parquet', 'db'
nombre_tabla = 'bi_loan_tape_collections'
modo_escritura = 'replace'  # opciones: 'fail', 'replace', 'append'

# Configuración de procesos (True para ejecutar, False para omitir)
ejecutar_procesos = {
    'portfolio': True,
    'payoff': False,
    'vintage_pi': False,
    'loan_tape_stratification': True,
    'spv_report': False,
    'loan_tape_full_installment': False
}

# =============================================================================
# ejecución de procesos
# =============================================================================

def ejecutar_reportes():
    """Función principal de ejecución de reportes"""
    tiempo_inicial = time()
    
    print(f"Iniciando procesamiento: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Rango de fechas: {fecha_inicio} a {fecha_corte}")
    print(f"Directorio de salida: {output_dir}")
    print("="*70)
    
    # Variables derivadas
    ym_s = int(fecha_inicio[:4] + fecha_inicio[5:7])
    ys = int(fecha_inicio[:4])
    ym_co = int(fecha_corte[:4] + fecha_corte[5:7])
    
    # Crear directorio de salida si no existe
    output_dir.mkdir(exist_ok=True)
    
    # Variables para almacenar resultados
    final = None
    loan_tape_copy = None
    period_index = None
    
    # Ejecutar procesos según configuración
    if ejecutar_procesos['portfolio']:
        print('___________________________portfolio___________________________')
        final, loan_tape_copy = rp.portfolio(ys, fecha_corte)
    
    if ejecutar_procesos['payoff']:
        print('___________________________payoff___________________________')
        rp.payoff(inicio=fecha_inicio, corte=fecha_corte)
    
    if ejecutar_procesos['vintage_pi']:
        print('___________________________vintage_pi___________________________')
        rp.vintage_pi(final, fecha_corte, ym_co, ym_s)
    
    if ejecutar_procesos['loan_tape_stratification']:
        print('________________loan_tape_stratification_collection________________')
        period_index = rp.loan_tape_stratification_collection(
            loan_tape_copy, final, ym_s, formato_salida, nombre_tabla, modo_escritura
        )
    
    if ejecutar_procesos['spv_report']:
        print('________________spv_report________________')
        # spv_report = rp.spv_report(period_index)
        pass
    
    if ejecutar_procesos['loan_tape_full_installment']:
        print('________________loan_tape_full_installment________________')
        # loan_tape_full_installment = rp.loan_tape_full_installment(period_index)
        pass
    
    tiempo_final = time()
    print("="*70)
    print(f'Tiempo total de ejecución: {round((tiempo_final - tiempo_inicial)/60, 2)} minutos')
    print(f"Procesamiento completado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Ejecutar el script
tiempo_inicial = time()
ejecutar_reportes()