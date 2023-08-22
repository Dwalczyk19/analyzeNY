'''want to finish this code with a dataset that encompasses every unique zip code identifer for town, city and village 
e.g 
if plattsburgh differs by city and town, set zip-codes for each and specifiy their labels & municipality affilation
'''

'''
to mix all dfs we plan to have 3 different functions

1. to separate the municipalities that have only one instance (769): create column with list of zip codes, type of mun, and label
2. separate dual instances (220). sicne, we know that towns can encompass multiple villages maybe find analagous zips 
based on either lat and long or just compare to every single value that they encompass. Keep same zip codes for now for
all of them 
3. tri-instances (very few, 5?) if they have city label them as city otherwise apply same function as dual instances
'''


import pandas as pd
pd.set_option('display.max_columns', None)
df = pd.read_csv("/home/dwalz/cannabis-analysis/zip_code_database.csv") #zip codes, primary city; this an external folder
NY = df[df["state"] == "NY"]
zip_file = pd.read_csv("/home/dwalz/cannabis-analysis/Zip-CodesNY.csv")

rural_coding = pd.read_csv("/home/dwalz/cannabis-analysis/NY opt out_rural coding - Sheet 1.csv")
unique_rc = rural_coding[["municipality", "county", "population", "type", "labels"]].drop_duplicates()
unique_rc = unique_rc.iloc[:-2]
unique_rc["type"] = unique_rc["type"].apply(lambda x:x.rstrip("/xa0").strip())
#print(NY[["zip", "primary_city"]])

'''find all find all frequencies of counties'''
counties57 = {items for items in rural_coding["county"]}

frequency = dict(sorted(unique_rc["county"].value_counts().to_dict().items())) #frequency of counties 
type_freq = dict(sorted(unique_rc["type"].value_counts().to_dict().items()))
print(type_freq)

city = {}
town = {}
village = {}




mDict = {}
for a,b,c,d,e in unique_rc[["municipality", "population", "type", "labels", "county"]].itertuples(index = False):
    a = a.strip()
    if "#" in b: 
        b = '0'
    
    if a in mDict: 
        mDict[a].append((c,d,int(b.replace(",", ""))))
    else: 
        mDict[a] = [(c,d,int(b.replace(",","")))]

singular_municipality = [x for x,y in mDict.items() if len(y) == 1] #rural_coding: w/ only one instance


shared = {}
for items,values in mDict.items():
    if items.strip() in NY["primary_city"].tolist() and items.strip() not in shared: #if zip codes are avaliable in NY ds & key not already in shared
        shared[items] = list(NY.loc[df["primary_city"] == items, "zip"])
        
print(len(shared))
#all singular values and their respective zip codes in pandas dataframe 

#next you need to get all other values and determine their optimal zip codes based off some different methodology 
#find difference between town city village, get optimal population values 
double_municipality = [x for x,y in mDict.items() if len(y) == 2]




'''a village is part of a town 
cities are not part of towns (but have the power of towns)
a village can be a part of more than one town
'''



