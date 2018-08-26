import pandas as pd
import geopandas as gpd
from pathlib import Path

def clean_police_stats(path):
    path = Path(path)
    if not path.exists():
        raise(OSError('path does not exsist'))
    police_stats = pd.read_csv(path)
    # make stations lower case
    police_stats['Police Station'] = police_stats['Police Station'].str.lower()
    police_stats['Province'] = police_stats['Province'].str.lower()
    # clean column names
    police_stats.Crime = police_stats.Crime.str.replace('17  ', '')
    # make year into int
    police_stats.Year = police_stats.Year.apply(lambda s: int(s.split('-')[0]))
    return police_stats

def clean_popluation_stats(path):
    path = Path(path)
    if not path.exists():
        raise(OSError('path does not exsist'))
    population_gdf = gpd.read_file(path)
    # make index an int
    population_gdf.sal_code_i = population_gdf.sal_code_i.astype(int)
    # make provence lower case
    population_gdf['pr_name'] = population_gdf['pr_name'].str.lower()
    return population_gdf

def clean_area_2_precint(path):
    path = Path(path)
    if not path.exists():
        raise(OSError('path does not exsist'))
    precint2_sa = pd.read_csv(path)
    # lower join fileds
    precint2_sa['precinct'] = precint2_sa['precinct'].str.lower()
    return precint2_sa

