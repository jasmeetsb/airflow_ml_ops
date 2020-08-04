import pandas as pd
import numpy as np

from google.cloud import bigquery

#Authentication
#client = bigquery.Client()
client = bigquery.Client.from_service_account_json('<PATH to Service account key *.json file>')


#Retrieve data from BQ
sql = """
SELECT * FROM `jsb-demos.yellow_taxi.yellow_taxi_fare_2m` 
#WHERE pickup_datetime < '2016-01-03'
ORDER BY pickup_datetime 
LIMIT 10000
"""
#Convert to DataFrame
train_data = client.query(sql).to_dataframe()

#Random Split data into two sets. 
#Dummy step. In actual deployment assign two data sets to be validated to df1 and df2
msk = np.random.rand(len(train_data)) < 0.5
df1 = train_data[msk]
df2 = train_data[~msk]


#Define function to compare two datasets and calculate deltas in key stats

def evaluate_drift(old_data, new_data):
    df1 = old_data
    df2 = new_data

    #Calculate key stats
    df1_sts = df1.describe()
    df2_sts = df2.describe()

    rows = df1_sts.index.values
    columns = df1_sts.columns.values
    
    #Append blank fields
    df1_sts = df1_sts.append(pd.Series(name='skew',dtype="float64"))
    df1_sts = df1_sts.append(pd.Series(name='kurt',dtype="float64"))
    df1_sts = df1_sts.append(pd.Series(name='percent_missing',dtype="float64"))
    
    df2_sts = df2_sts.append(pd.Series(name='skew',dtype="float64"))
    df2_sts = df2_sts.append(pd.Series(name='kurt',dtype="float64"))
    df2_sts = df2_sts.append(pd.Series(name='percent_missing',dtype="float64"))

    #Calculate skew, kurt, missing% for each field
    for col in columns:
        df1_sts[col]['skew'] = df1[col].skew()
        df1_sts[col]['kurt'] = df1[col].kurt()
        df1_sts[col]['percent_missing'] = df1[col].isnull().sum()/len(df1)*100
        
        df2_sts[col]['skew'] = df2[col].skew()
        df2_sts[col]['kurt'] = df2[col].kurt()
        df2_sts[col]['percent_missing'] = df2[col].isnull().sum()/len(df2)*100
        


    rows = df1_sts.index.values
    columns = df1_sts.columns.values
    drift_summary = pd.DataFrame(columns=columns, index=rows)

    #Calculate the delta(%) for each field between the two datasets
    for col in columns:
        for row in rows:
            if (df1_sts[col][row] != 0):
                drift_value = (df1_sts[col][row] - df2_sts[col][row])/df1_sts[col][row]
            elif (df2_sts[col][row] != 0):
                drift_value= (df1_sts[col][row] - df2_sts[col][row])/df2_sts[col][row]
            else:
                drift_value = 0


            drift_summary[col][row] = abs(round(drift_value, 2))

    return(df1_sts,df2_sts, drift_summary)




df1_sts, df2_sts, drift_summary = evaluate_drift(df1,df2)

print(drift_summary)