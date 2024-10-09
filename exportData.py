import pandas as pd
from datetime import datetime
import datetime as dt
import os
import re
import openpyxl
import xlsxwriter
from glob import glob


def create_incidents_file(site_list, active_list, closed_list, df_info_sunlight, final_irradiance_data,
                          date, geo, tracker: bool = False):

    folder_path = glob(os.path.join(os.getcwd(), 'DMR files', geo))[0]
    print(folder_path)
    print(date)

    if tracker:
        flag = ' trk inc.'
        file_path = folder_path + '\\Tracker_Incidents' + str(date).replace('-','') + '.xlsx'
    else:
        flag = ' inc.'
        file_path = folder_path + '\\Incidents' + str(date).replace('-','') + '.xlsx'

    print(file_path)
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter', engine_kwargs={'options': {'nan_inf_to_errors': True}})

    df_info_sunlight.to_excel(writer, sheet_name='Info', index=False)
    final_irradiance_data.to_excel(writer, sheet_name='Irradiance', index=False)

    for site in site_list:
        only_site_name = site.replace('LSBP - ', '')
        active_sheet_name = only_site_name + ' active' + flag
        closed_sheet_name = only_site_name + ' closed' + flag

        df_active = active_list[site]
        df_closed = closed_list[site]

        df_closed['Status of incident'] = 'Closed'
        df_active['Status of incident'] = 'Active'
        df_active['Action required'] = ''

        df_closed.to_excel(writer, sheet_name=closed_sheet_name, index=False)
        for column in df_closed:
            column_length = max(df_closed[column].astype(str).map(len).max(), len(column))
            col_idx = df_closed.columns.get_loc(column)
            writer.sheets[closed_sheet_name].set_column(col_idx, col_idx, column_length)

        print('Closed events of ' + site + ' added')

        df_active.to_excel(writer, sheet_name=active_sheet_name, index=False)
        for column in df_active:
            column_length = max(df_active[column].astype(str).map(len).max(), len(column))
            col_idx = df_active.columns.get_loc(column)
            writer.sheets[active_sheet_name].set_column(col_idx, col_idx, column_length)

        print('Active events of ' + site + ' added')

    writer.close()


    return file_path