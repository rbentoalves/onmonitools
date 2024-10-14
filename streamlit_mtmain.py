import uuid as uuid
from datetime import datetime
import loadData
import treatData
import exportData
import os
from glob import glob
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSessionException



PROFILE = True #deactivate when deploying to SF

def generate_incidents_files(GEO, analysis_start_date, backlog_source, SITE_INFO, COMPONENT_DATA):
    status_window.info('Reading daily alarm report')
    alarms = loadData.get_daily_alarm_report(GEO, analysis_start_date)

    status_window.info('Reading backlog of data from:' + backlog_source)
    incidents_bl, tracker_incidents_bl = loadData.get_backlog_data(backlog_source, GEO, analysis_start_date)

    status_window.info('Reading irradiance data for the day')
    irradiance_data_day = loadData.get_irradiance_day(GEO, analysis_start_date)
    df_info_sunlight, irradiance_file_data_notcurated = (
        treatData.read_time_of_operation_new(irradiance_data_day, site_selection, SITE_INFO))

    status_window.info('Creating new incidents list')
    active_pu_list, active_tracker_list, closed_pu_list, closed_tracker_list = (
        treatData.create_incidents_list(alarms, df_info_sunlight, SITE_INFO, COMPONENT_DATA))

    status_window.info('Adding backlog')
    active_pu_list = treatData.complete_dataset_existing_incidents(active_pu_list, incidents_bl)
    active_tracker_list = treatData.complete_dataset_existing_incidents(active_tracker_list,
                                                                        tracker_incidents_bl)

    status_window.info('Creating PU incident files')
    incidents_path = exportData.create_incidents_file(site_selection, active_pu_list, closed_pu_list,
                                                      df_info_sunlight, irradiance_file_data_notcurated,
                                                      analysis_start_date, GEO)

    status_window.info('Creating tracker incident files')
    tracker_inc_path = exportData.create_incidents_file(site_selection, active_tracker_list, closed_tracker_list,
                                                        df_info_sunlight, irradiance_file_data_notcurated,
                                                        analysis_start_date, GEO, tracker=True)


    return incidents_path, tracker_inc_path

def generate_dmr(incidents_path, tracker_inc_path, GEO, analysis_start_date, SITE_INFO, COMPONENT_DATA):

    return



if __name__ == '__main__':
    st.set_page_config(page_title="Performance Analysis tool",
                       page_icon="☀️",
                       layout="wide")

    profile_mainkpis = ''
    GEO = 'USA'

    ALL_GENERAL_INFO, BUDGET_PROD, BUDGET_IRR, BUDGET_PR, COMPONENT_DATA, SITE_INFO, PRE_SELECT = (
        loadData.get_general_info(GEO))
    ALL_SITE_LIST = SITE_INFO.index

    print(ALL_SITE_LIST)
    print(PRE_SELECT)

    if not PRE_SELECT:
        PRE_SELECT = ALL_SITE_LIST

    st.title('☀️ Performance Analysis tool')
    status_window = st.empty()
    (dmr_tab, event_tracker_tab, curt_tab, clipping_tab, contractual_tab, debug_tab) = (
        st.tabs(['DMR', 'Event Tracker', 'Curtailment Calculation', 'Clipping Calculation',
                 'Clipping Calculation', 'Profiler']))

    status_window_run_all = st.empty()
    with (dmr_tab):
        with st.container():
            r1_1, r1_2 = st.columns(2)
            with r1_1:
                with st.expander('General Inputs', expanded=True):
                    g1_1, g1_2 = st.columns(2)
                    with g1_1:
                        analysis_start_date = st.date_input('Start date')
                        process_selection = st.selectbox('Process selection', ['Incidents List', 'Daily Report'])
                        backlog_source = st.selectbox('Source selection', ['Event Tracker', 'DMR'],
                                                        placeholder='Event Tracker')

                        if process_selection == 'Incidents List':
                            alarms = st.file_uploader('Choose daily alarm report')
                        else:
                            incidents_file = st.file_uploader('Choose incidents file')
                            tracker_incidents_file = st.file_uploader('Choose tracker incidents file')

                    with g1_2:
                        site_selection = st.multiselect('Site Selection', ALL_SITE_LIST, default=PRE_SELECT)
                        def disable(b):
                            st.session_state["analysis_run_disabled"] = b

                        run_analysis_btn = st.button('Run Analysis', use_container_width=True, on_click=disable,
                                                     args=(True,),
                                                     disabled=st.session_state.get("analysis_run_disabled", False))

                        if st.button("Reset All?", on_click=disable, args=(False,), use_container_width=True):
                            st.rerun()

            with r1_2:
                download_hub = st.empty()

        with st.container():
            results_chart = st.empty()
            # results_chart.scatter_chart(get_chart_results(), use_container_width=True)

        with st.container():
            power_chart = st.empty()

        with st.container():
            percentages_chart = st.empty()

    with (event_tracker_tab):
        inv_outages_table = st.empty()
        inv_outages_table2 = st.empty()

    with (curt_tab):
        curtailment_results = st.empty()

    with (clipping_tab):
        clipping_results = st.empty()

    with (debug_tab):
        debug_text = st.empty()
        debug_text.code(profile_mainkpis)


    if run_analysis_btn:
        analysis_start_ts = datetime.strptime((str(analysis_start_date) + " 00:00:00"), '%Y-%m-%d %H:%M:%S')
        analysis_end_ts = datetime.strptime((str(analysis_start_date) + " 23:45:00"), '%Y-%m-%d %H:%M:%S')

        from pyinstrument import Profiler

        profiler = Profiler()
        profiler.start()

        if process_selection == 'Incidents List':
            #alarm_report = st.file_uploader('Choose alarm report') #how slow is this?
            #alarms = pd.read_csv(alarm_report)

            #alarms = st.file_uploader()

            incidents_path, tracker_inc_path = generate_incidents_files(GEO, analysis_start_date, backlog_source,
                                                                        SITE_INFO, COMPONENT_DATA)

            status_window.info('Success!! Incidents files created!')

        if process_selection == 'Daily Report':
            dmr_path = generate_dmr((incidents_path, tracker_inc_path, GEO, analysis_start_date,
                                     SITE_INFO, COMPONENT_DATA))



            #results_table.dataframe(result, use_container_width=True)
            #results_chart.altair_chart(chart, use_container_width=True)
            #inv_outages_table.dataframe(df_incidents_period, use_container_width=True)

            '''# Add detailed results to tabs
            inv_outages_table2.dataframe(curtailment_df, use_container_width=True)
            detailed_results_table.dataframe(site_data, use_container_width=True)

            status_window_run_all.info("Plotting charts")
            # Plot charts
            results_chart.altair_chart(get_chart_results(site, site_data), use_container_width=True)
            power_chart.altair_chart(get_chart_power(site, site_data), use_container_width=True)
            percentages_chart.altair_chart(get_chart_percentages(site, site_data), use_container_width=True)'''

        profiler.stop()
        profile_mainkpis = profiler.output_text()
        debug_text.code(profile_mainkpis)

        st.snow()

