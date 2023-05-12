import os
from gender_it_functions import reading_wgnd, get_gender
from utilities import get_adhoc_config
from sqlalchemy import text
import pandas as pd
import pinyin

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

if __name__ == "__main__":
    # read_wgnd()
    # data1 = reading_wgnd(1, os.getcwd())
    # data2 = reading_wgnd(2, os.getcwd())
    # data3 = reading_wgnd(3, os.getcwd())
    # print(pinyin.get('你好', format="strip", delimiter=" "))
    engine = get_adhoc_config(database="gender_attribution")
    with engine.connect() as conn:
        granted_records = conn.execute(text(f"""select uuid, name_first, country, male from rawgender_ernest_attr_granted_20210930 as a inner join patent.rawlocation b on a.rawlocation_id=b.id;"""))
        dfg = pd.DataFrame(granted_records.fetchall())
        dfg.columns = granted_records.keys()
    with engine.connect() as conn:
        pre_records = conn.execute(text(f"""select id as uuid, name_first, country, male from rawgender_ernest_attr_pregrant_20210930;"""))
        dfp = pd.DataFrame(pre_records.fetchall())
        dfp.columns = pre_records.keys()
    df = pd.concat([dfg, dfp])
    gender_thresholds = get_thresholds()
    final = pd.DataFrame()
    for key in gender_thresholds.keys():
        print("                ")
        print(key)
        print("                ")
        if key != .97:
            temp_df = df[df['country'].isin(gender_thresholds[key])]
        else:
            temp_df = df[~df.country.isin(gender_thresholds[key])]
        threshold = key
        final_temp = run_gender_attr(temp_df, threshold)
        final = pd.concat([final,final_temp])
    print(final.shape)
    print(df.shape)
    final.to_csv("20210930_results_varying_thresholds.csv")
    breakpoint()

