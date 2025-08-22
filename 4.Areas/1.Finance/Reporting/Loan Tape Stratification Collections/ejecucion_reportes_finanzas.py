# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 08:39:33 2024
@author: allan
"""
import sys
from time import time
tiempo_inicial = time() 

sys.path.append(r'C:\Users\Bi_analyst\Desktop\Python')
import reporteria_finanzas as rp

#RECUERDA CAMBIAR EN EL MODULO reporteria_finanzas LA NOMENCLATURA MEX O ESP DEPENDE QUE GEOGRAFIA QUIERES CONCLUIR

#Variables para modificar las fechas del reporte debe ser menor o igual. Es decir, se ingresa el último día del mes
starts = '2017-01-01'
ym_s = int(starts[:4] + starts[5:7])
ys = int(starts[:4])
# ---------------------------------- #
cut_off = '2025-08-13'
ym_co = int(cut_off[:4] + cut_off[5:7])
# ---------------------------------- #

#Variables para mandar a escribir
destiny = 'xlsx'  #xlsx or db
table_name = 'bi_loan_tape_collections'  # El nombre de la tabla o de la hoja
if_exists= 'replace' #Puede tomar los valores de fail’, ‘replace’, ‘append’

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

print(f'El tiempo de ejecucion fue: {round((time() - tiempo_inicial)/60,1)} minutos' )
