
with open("/home/davidwalczyk19/getCity/counties", "r") as file:
    counties = []
    for line in file:
        line = line.strip()
        counties.append(line)


from google.cloud import bigquery
import pandas as pd
client = bigquery.Client()

#had to create separate tables for New_York & St_Lawrence (fields 31 & 51)

counties_df = pd.DataFrame(columns = ["avg_age", "f", "m", "u", "dem", "rep", "blk", "green", "ind", "active", "am", "inactive", "purged", "history_length", "num_dp"])
def age(results, counties_df, i): 
    for items in results: 
        data = float(str(items["f0_"]).replace('(','').replace(')','') )
        counties_df.loc[i, "avg_age"] = data
    return counties_df

def gender(results, counties_df, i):
    for items in results:
        data = items["gender"]
        enter = items["gen"]
        if data == "F":
            counties_df.loc[i,"f"] = enter
        elif data == "M":
            counties_df.loc[i,"m"] = enter
        elif data == "U":
            counties_df.loc[i,"u"] = enter

    return counties_df
    

def enrollment(results, counties_df, i):
    for items in results: 
        data = items["enrollment"]
        enter = items["enr"]
        if data == "DEM":
            counties_df.loc[i,"dem"] = enter
        elif data == "REP":
            counties_df.loc[i,"rep"] = enter
        elif data == "BLK":
            counties_df.loc[i,"blk"] = enter

    return counties_df


def otherparty(results, counties_df, i):
    for items in results: 
        data = items["otherparty"]
        enter = items["enr"]
        if data == "IND":
            counties_df.loc[i,"ind"] = enter
        elif data == "GRE":
            counties_df.loc[i,"green"] = enter

    return counties_df

def status(results, counties_df, i):
    for items in results: 
        data = items["status"]
        enter = items["enr"]
        if data == "A":
            counties_df.loc[i,"active"] = enter
        elif data == "P":
            counties_df.loc[i,"purged"] = enter
        elif data == "I":
            counties_df.loc[i,"inactive"] = enter
        elif data == "AM":
            counties_df.loc[i,"am"] = enter

    return counties_df

def historyLength(results, counties_df, i):
    for items in results: 
        data = float(str(items["f0_"]).replace('(','').replace(')','') )
        counties_df.loc[i, "history_length"] = data

    return counties_df

def length(results, counties_df, i):
    for items in results: 
        data = int(str(items["f0_"]).replace('(','').replace(')','') )
        counties_df.loc[i, "num_dp"] = data

    return counties_df

for i in range(len(counties)):
    if counties[i] == "New York" or counties[i] == "St. Lawrence":

        if counties[i] == "New York":
            name = "New_York"
        else:
            name = "St_Lawrence"
        query = """
        select abs(avg(cast(substring(dob,1,4) as integer)-2023)) from `cannabisanalysis-dw.NYS_db.{}`
        """.format(name)

        query2 = """
        select gender, count(gender) as gen from `cannabisanalysis-dw.NYS_db.{}` group by gender order by gen desc
        """.format(name)

        query3 = """
        select enrollment, count(enrollment) as enr from `cannabisanalysis-dw.NYS_db.{}` group by enrollment order by enr desc
        """.format(name)

        query4 = """
        select otherparty, count(otherparty) as enr from `cannabisanalysis-dw.NYS_db.{}` group by otherparty order by enr desc
        """.format(name)

        query5 = """
        select status, count(status) as enr from `cannabisanalysis-dw.NYS_db.{}` group by status order by enr desc
        """.format(name)

        query6 = """
        select avg(length(voterhistory)) from `cannabisanalysis-dw.NYS_db.{}`
        """.format(name)

        query7 = """
        select count(sboeid) from `cannabisanalysis-dw.NYS_db.{}`
        """.format(name)

    else: 
        query = """
        select abs(avg(cast(substring(dob,1,4) as integer)-2023)) from `cannabisanalysis-dw.NYS_db.{}`
        """.format(counties[i])

        query2 = """
        select gender, count(gender) as gen from `cannabisanalysis-dw.NYS_db.{}` group by gender order by gen desc
        """.format(counties[i])

        query3 = """
        select enrollment, count(enrollment) as enr from `cannabisanalysis-dw.NYS_db.{}` group by enrollment order by enr desc
        """.format(counties[i])

        query4 = """
        select otherparty, count(otherparty) as enr from `cannabisanalysis-dw.NYS_db.{}` group by otherparty order by enr desc
        """.format(counties[i])
        
        query5 = """
        select status, count(status) as enr from `cannabisanalysis-dw.NYS_db.{}` group by status order by enr desc
        """.format(counties[i])

        query6 = """
        select avg(length(voterhistory)) from `cannabisanalysis-dw.NYS_db.{}`
        """.format(counties[i])

        query7 = """
        select count(sboeid) from `cannabisanalysis-dw.NYS_db.{}`
        """.format(counties[i])

    counties_df = age(client.query(query), counties_df, i)
    counties_df = gender(client.query(query2), counties_df, i)
    counties_df = enrollment(client.query(query3), counties_df, i)
    counties_df = otherparty(client.query(query4), counties_df, i)
    counties_df = status(client.query(query5), counties_df, i)
    counties_df = historyLength(client.query(query6), counties_df, i)
    counties_df = length(client.query(query7), counties_df, i)
   #counties_df.loc[i, "weight"] = counties_df.loc[i,""] # of data points / population

counties_df.index = counties
counties_df.to_csv("countiesInfo.csv")
