from .etl import clean_police_stats, clean_popluation_stats, clean_area_2_precint
from pathlib import Path
from pandas import merge
from geopandas import GeoDataFrame
from os import environ

pop_groups = dict(ages = ['a70_79', 'a40_49', 'a30_39', 'a10_19', 'a80_plus', 'a0_9','a60_69','a50_59','a20_29'],
races = ['asian','other_race','black_afri','white',  'coloured'],
languages = ['unspec_lan','sign_lang','tshivenda','isi_ndebel','setswana','isi_xhosa', 'na_lang','sepedi',
            'xitsonga','isi_zulu', 'english', 'other_lang','si_swati', 'sesotho', 'afrikaans'],
educations = ['cert_dip','other_edu','secondary','primary', 'tertiary', 'no_schooli','ntc'],
area = ['urban_area','farm_area','tribal_are'])


def load_crime_stats(population_group=None, crime_list=None, provence=None):
    # lower provers
    if provence is not None:
        provence = provence.lower()
    # get data set dir
    data_path = get_work_path()
    # load an clean police
    police_stats = clean_police_stats(data_path.joinpath('Police_Statistics___2005_-_2017.csv'))
    if crime_list is not None:
        police_stats = police_stats[police_stats['Crime'].isin(crime_list)]
    if provence is not None:
        police_stats = police_stats.query(f"Province == '{provence}'")
    # population shape file
    pop_stats = clean_popluation_stats(data_path.joinpath('population/geo_export_3ec3ac74-ddff-4220-8007-b9b5643f79af.shp'))
    base_group = ['sal_code_i','pr_name', 'sp_name', 'geometry']
    if population_group is not None:
        # filter out columns
        pop_stats = pop_stats[pop_groups[population_group]+base_group]
    if provence is not None:
        pop_stats = pop_stats.query(f"pr_name == '{provence}'")
    # shape id to weights
    precinct = clean_area_2_precint(data_path.joinpath('Precinct_to_small_area_weights.csv'))
    # munge data
    df = merge(precinct, pop_stats, left_on='small_area',right_on='sal_code_i')
    df = merge(df, police_stats,left_on='precinct', right_on='Police Station')
    # calclate crime per shape file as proportion of precint weight
    df['total_crime'] = df.weight * df.Incidents
    # keep as geo-dataframe
    df = GeoDataFrame(df, crs=pop_stats.crs)
    # clean data frame
    df = df.drop(['sal_code_i','pr_name','sp_name', 'Police Station' , 'Incidents','weight'], axis=1)
    # agg precinct back into shapes
    temp_df = df.groupby(['small_area', 'Year', 'Crime'])[['total_crime']].sum().round()
    df = df.drop_duplicates(subset=['small_area', 'Year', 'Crime']).drop(['total_crime'], axis=1)
    df = merge(df, temp_df, on=['small_area', 'Year', 'Crime'])
    return df



def get_work_path(datapath='build/job/eighty_20/datasets'):
    home_path = Path(environ.get('HOME'))
    data_path = Path(datapath)
    if not home_path.joinpath(data_path).exists():
        raise(OSError(f'{datapath} does not exists.'))
    return home_path.joinpath(data_path)
