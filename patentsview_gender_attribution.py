import os
from gender_it.gender_it_functions import reading_wgnd, get_gender
from gender_it.utilities import get_adhoc_config
from sqlalchemy import text
import pandas as pd
import datetime
import pinyin
import numpy as np

def get_thresholds():
    gender_thresholds = {
    .8: ['KR'],
    .9: ['IN'],
    .6: ['CN','SG', 'TW', 'MO', 'HK'],
    .97: ['CN','SG', 'TW', 'MO', 'HK', 'IN', 'KR']
    }
    return gender_thresholds

def run_gender_attr(df, threshold):
    result = get_gender(df[['name_first', 'country']], "name_first", country_column = "country", threshold=threshold)
    df2 = result[ (result['gender']=="not found") | (result['gender']=="?")]
    result2 = get_gender(df2[['name_first', 'country']], "name_first", country_column = "country", split_list = [" ", "-"], threshold=threshold)
    df3 = result2[ (result2['gender']=="not found") | (result2['gender']=="?")]
    result3 = get_gender(df3[['name_first']], "name_first", threshold=threshold)

    gender_attr = result[(result['gender']=="F") | (result['gender']=="M")]
    gender_attr2 = result2[(result2['gender']=="F") | (result2['gender']=="M")]
    results_final = pd.concat([gender_attr,gender_attr2, result3])
    results_final = results_final.drop_duplicates()
    final = pd.merge(df, results_final, how="left", on=["name_first", 'country'])
    return final

def run_test_against_ERNEST(engine):
    with engine.connect() as conn:
        # granted_records = conn.execute(text(f"""select uuid, name_first, country, male from rawgender_ernest_attr_granted_20210930 as a inner join patent.rawlocation b on a.rawlocation_id=b.id;"""))
        granted_records = conn.execute(text(f"""select uuid, name_first, country, male from rawgender_ernest_attr_granted_20210930 as a inner join patent.rawlocation b on a.rawlocation_id=b.id;"""))
        dfg = pd.DataFrame(granted_records.fetchall())
        dfg.columns = granted_records.keys()
        print(dfg.shape)
    with engine.connect() as conn:
        # pre_records = conn.execute(text(f"""select id as uuid, name_first, country, male from rawgender_ernest_attr_pregrant_20210930;"""))
        pre_records = conn.execute(text(f"""select id as uuid, name_first, country, male from rawgender_ernest_attr_pregrant_20210930;"""))
        dfp = pd.DataFrame(pre_records.fetchall())
        print(dfp.shape)
        dfp.columns = pre_records.keys()
    df = pd.concat([dfg, dfp])
    return df

def get_disambiguated_inventor_batch(engine, start_date, end_date, type):
    with engine.connect() as conn:
        # granted_records = conn.execute(text(f"""select uuid, name_first, country, male from rawgender_ernest_attr_granted_20210930 as a inner join patent.rawlocation b on a.rawlocation_id=b.id;"""))
        if type == 'granted_patent':
            q = f"""
    select uuid, name_first, country, a.version_indicator
    from rawinventor a 
        inner join rawlocation b on a.rawlocation_id=b.id
    where a.version_indicator >= '{start_date}' and  a.version_indicator <= '{end_date}' 
            """
        else:
            q = f"""
            select id, name_first, country, a.version_indicator
            from rawinventor a 
             where a.version_indicator >= '{start_date}' and  a.version_indicator <= '{end_date}' 
--     where a.version_indicator >= '2023-06-29' and  a.version_indicator <= '2023-06-29'                     
                    """
        print(q)
        granted_records = conn.execute(text(q))
        dfg = pd.DataFrame(granted_records.fetchall())
        dfg.columns = granted_records.keys()
        print(dfg.shape)
        print(" ")
        # print(dfg.head())
    return dfg

def update_unique_firstname_country_lookup():
    print("HI ")

def run_AIR_genderit(df):
    gender_thresholds = get_thresholds()
    final = pd.DataFrame()
    for key in gender_thresholds.keys():
        if key != .97:
            print(" ")
            print(f" Processing {gender_thresholds[key]} ...")
            print(" ")
            temp_df = df[df['country'].isin(gender_thresholds[key])]
        else:
            print(" ")
            print(f" Processing EVERYTHING BUT {gender_thresholds[key]} ...")
            print(" ")
            temp_df = df[~df.country.isin(gender_thresholds[key])]
        if temp_df.empty:
            print(f"\t\t No records for {gender_thresholds[key]} for this batch ...")
            continue
        else:
            temp_df.head()
            threshold = key
            final_temp = run_gender_attr(temp_df, threshold)
            final = pd.concat([final, final_temp])
    return final
    # final['ernest_gender'] = np.where(final['male'] == 0, "F", np.where(final['male'] == 1, "M", ""))

if __name__ == "__main__":
    # data1 = reading_wgnd(1, os.getcwd())
    # data2 = reading_wgnd(2, os.getcwd())
    # data3 = reading_wgnd(3, os.getcwd())
    # print(pinyin.get('你好', format="strip", delimiter=" "))
    # engine = get_adhoc_config(database="patent")
    engine = get_adhoc_config(database="pregrant_publications")
    gen_att_engine = get_adhoc_config(database="gender_attribution")
    d = datetime.date(2019, 7, 4)
    while d < datetime.date(2023, 4, 10):
        start_date = d
        end_date = d + datetime.timedelta(days=175)
        df = get_disambiguated_inventor_batch(engine, start_date, end_date, type)
        breakpoint()
        final = run_AIR_genderit(df)
        try:
            final.to_sql('pgpubs_inventor_genderit_attribution', con=gen_att_engine, if_exists='append', chunksize=1000)
        except:
            breakpoint()
        print(f"FINISHED PROCESSING PATENT.RAWINVENTORS {start_date}: {end_date}")
        # final.to_sql('patent_inventor_genderit_attribution', con=gen_att_engine, if_exists='append', chunksize=1000)
        d =  end_date

# FINISHED PROCESSING PATENT.RAWINVENTORS 1985-08-01:1986-12-14



