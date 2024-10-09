import pandas as pd
from datetime import datetime
import re

def remove_milliseconds_to_datetime(df, closed: bool = True):

    if closed:
        df['Event Start Time'] = [str(timestamp) for timestamp in df['Event Start Time']]
        df['Event End Time'] = [str(timestamp) for timestamp in df['Event End Time']]

        for index, row in df.iterrows():
            stime = df.loc[index, 'Event Start Time']
            etime = df.loc[index, 'Event End Time']
            if "." in stime:
                dot_position = stime.index('.')
                df.loc[index, 'Event Start Time'] = str(stime[:dot_position])
            if "." in etime:
                dot_position = etime.index('.')
                df.loc[index, 'Event End Time'] = str(etime[:dot_position])

        df['Event Start Time'] = pd.to_datetime(df['Event Start Time'], format='%Y-%m-%d %H:%M:%S')
        df['Event End Time'] = pd.to_datetime(df['Event End Time'], format='%Y-%m-%d %H:%M:%S')

    else:
        df['Event Start Time'] = [str(timestamp) for timestamp in df['Event Start Time']]

        for index, row in df.iterrows():
            stime = df.loc[index, 'Event Start Time']
            if "." in stime:
                dot_position = stime.index('.')
                df.loc[index, 'Event Start Time'] = str(stime[:dot_position])

        df['Event Start Time'] = pd.to_datetime(df['Event Start Time'], format='%Y-%m-%d %H:%M:%S')

    return df

def correct_site_name(site):
    while site[-1] == " ":
        site = site[:-1]

    while site[0] == " ":
        site = site[1:]

    return site


def read_time_of_operation_new(irradiance_df, site_list, site_info):
    irradiance_df = irradiance_df.loc[:, ~irradiance_df.columns.str.contains('^Unnamed')]
    irradiance_df = irradiance_df[:-1]    # removes last timestamp of dataframe, since it's always the next day
    irradiance_file_data_notcurated = irradiance_df.loc[:, ~irradiance_df.columns.str.contains('curated')]

    dict_timeofops = {}

    for site in site_list:
        site = correct_site_name(site)
        site_irr_poa = irradiance_file_data_notcurated.columns[
            irradiance_file_data_notcurated.columns.str.contains(site)].to_list()[0]
        #print(site_irr_poa)

        irradiance_site = irradiance_df[['Timestamp', site_irr_poa]]
        irradiance_site_filt = irradiance_site.loc[irradiance_site[site_irr_poa] > 20].reset_index(drop=True)
        #print(irradiance_site_filt)

        if not irradiance_site_filt.empty:
            stime = irradiance_site_filt['Timestamp'][0]
            etime = irradiance_site_filt['Timestamp'][len(irradiance_site_filt.index)-1]
        else:
            date = str(irradiance_df['Timestamp'][0].date())
            stime = date + ' 07:00:00'
            etime = date + ' 21:00:00'

        nom_power = float(site_info.loc[site, 'Nominal Power DC'])
        dict_timeofops[site] = {'Nominal Power DC': nom_power,
                                'Sunrise': stime,
                                'Sunset': etime}

    df_info_sunlight = pd.DataFrame.from_dict(dict_timeofops, orient='index')
    print(df_info_sunlight)

    return df_info_sunlight, irradiance_file_data_notcurated


def complete_dataset_capacity_data(df_list, all_component_data):
    for site in df_list.keys():
        incidents_site = df_list[site]
        # print(type(incidents_site))
        if not type(incidents_site) == str:
            for index, row in incidents_site.iterrows():
                site = row['Site Name']
                component = row['Related Component']

                try:
                    capacity = all_component_data.loc[(all_component_data['Site'] == site)
                                                      & (all_component_data['Component'] == component)][
                        "Nominal Power DC"].values[0]
                except IndexError:
                    capacity = "NA"

                # Add capacity
                incidents_site.loc[index, 'Capacity Related Component'] = capacity

            df_list[site] = incidents_site

    return df_list


def create_incidents_list(alarms, df_info_sunlight, site_info, component_data):
    # add extra columns

    active_pu_list = {}
    closed_pu_list = {}
    active_tracker_list = {}
    closed_tracker_list = {}

    for site in df_info_sunlight.index:
        site_alarms = alarms.loc[alarms['Site Name'] == site]

        sunrise = pd.to_datetime(df_info_sunlight.loc[site, 'Sunrise'])
        sunset = pd.to_datetime(df_info_sunlight.loc[site, 'Sunset'])

        # PU Outages
        site_pu_alarms = site_alarms[(site_alarms['Component Status'] == 'Not Producing') &
                                     ~(site_alarms["State"] == 'Tracker target availability')]

        # Tracker incidents
        site_tracker_alarms = site_alarms[site_alarms['Related Component'].str.contains('Tracker|TRACKER|M -') |
                                          (site_alarms["State"] == 'Tracker target availability')]

        #Active incidents
        active_pu_list[site] = remove_milliseconds_to_datetime(
            site_pu_alarms[(site_pu_alarms['Event End Time'].isna())], closed=False)

        active_tracker_list[site] = remove_milliseconds_to_datetime(
            site_tracker_alarms[(site_tracker_alarms['Event End Time'].isna())], closed=False)

        #Closed incidents
        closed_site_pu_alarms = remove_milliseconds_to_datetime(
            site_pu_alarms[~(site_pu_alarms['Event End Time'].isna())])
        closed_site_tracker_alarms = remove_milliseconds_to_datetime(
            site_tracker_alarms[~(site_tracker_alarms['Event End Time'].isna())])

        # Sunrise and sunset correction
        closed_pu_list[site] = closed_site_pu_alarms[~(closed_site_pu_alarms['Event End Time'] < sunrise)
                                                     & ~(closed_site_pu_alarms['Event Start Time'] > sunset)
                                                     & (closed_site_pu_alarms['Duration (h)'] > 0.016)]

        closed_tracker_list[site] = closed_site_tracker_alarms[
            ~(closed_site_tracker_alarms['Event End Time'] < sunrise)
            & ~(closed_site_tracker_alarms['Event Start Time'] > sunset)
            & (closed_site_tracker_alarms['Duration (h)'] > 0.016)]

    #Add capacities
    active_pu_list = complete_dataset_capacity_data(active_pu_list, component_data)
    closed_pu_list = complete_dataset_capacity_data(closed_pu_list, component_data)

    active_tracker_list = complete_dataset_capacity_data(active_tracker_list, component_data)
    closed_tracker_list = complete_dataset_capacity_data(closed_tracker_list, component_data)


    return active_pu_list, active_tracker_list, closed_pu_list, closed_tracker_list


def complete_dataset_existing_incidents(df_list, df_dmr):
    for site in df_list.keys():
        print("Completing dataset on " + site)
        incidents_site = df_list[site]
        df_raw_columns = incidents_site.columns.to_list()

        df_dmr_site = df_dmr.loc[df_dmr['Site Name'] == site]

        if type(df_dmr_site) == str:
            print("No previous active events")

        elif type(incidents_site) == str:
            print("No active events, adding previously active events")
            incidents_site = pd.concat([incidents_site, df_dmr_site])[df_raw_columns]

        else:
            incidents_site = pd.concat([incidents_site, df_dmr_site])[df_raw_columns]

        df_list[site] = incidents_site


    return df_list


