
import requests
import json
from subprocess import call
from pyproj import Proj, transform
import pandas as pd
import io
from joblib import Parallel, delayed
import multiprocessing

# what are your inputs, and what operation do you want to
# perform on each input. For example...
num_cores = 3
#Auth Token - should have a long lifetime
token = "bea27618b9039f18ce5ea472944ca72"
#set static json body values and permsissions
body={}
pem1={}
pem1['username']= 'seanbc'
pem1['permission']='ALL'
pem2={}
pem2['username']= 'jgeis'
pem2['permission']='ALL'
pem3={}
pem3['username']= 'omeier'
pem3['permission']='ALL'
pem4={}
pem4['username']= 'ikewai-admin'
pem4['permission']='ALL'
pem5={}
pem1['username']= 'public'
pem1['permission']='READ'

body['name'] = "Water_Quality_Site"
body['schemaId'] = "576483085810658840-242ac1110-0001-013" #on ikewai-dev

body['permissions']=[pem1,pem2,pem3,pem4,pem5]
#should loop through each dataframe row convert to json and modify to fit well schema



#r = requests.get('https://www.waterqualitydata.us/Station/search?bBox=-158.63159179687497,20.951179993976236,-157.43408203125,22.04491330024569&mimeType=geojson')

#fetch all the stations in the state of HI
r = requests.get('https://www.waterqualitydata.us/Station/search?bBox=-161.44485622644424,17.567520365737877,-153.49075466394424,23.8928136159648&mimeType=geojson')

r.json()["features"]
r.json()["features"][0]
r.json()["features"][0]["geometry"]
site_count = len(r.json()["features"])

i=0

print(site_count)
def catalogSite(i):
    print("I: ",i)
    print(r.json()["features"][i])
    js = {}
    js['name']=r.json()["features"][i]['properties']['MonitoringLocationName']
    js['description']=r.json()["features"][i]['properties']['ProviderName'] + ' water quality site.'
    js['longitude'] = r.json()["features"][i]["geometry"]["coordinates"][0]
    js['latitude'] = r.json()["features"][i]["geometry"]["coordinates"][1]
    js['loc'] = r.json()["features"][i]["geometry"]
    js['MonitoringLocationName'] = r.json()["features"][i]['properties']['MonitoringLocationName']
    js['siteUrl']=r.json()["features"][i]['properties']['siteUrl']
    js['MonitoringLocationTypeName']=r.json()["features"][i]['properties']['MonitoringLocationTypeName']
    js['ProviderName']=r.json()["features"][i]['properties']['ProviderName']
    js['activityCount']=r.json()["features"][i]['properties']['activityCount']
    js['HUCEightDigitCode']=r.json()["features"][i]['properties']['HUCEightDigitCode']
    js['ResolvedMonitoringLocationTypeName']=r.json()["features"][i]['properties']['ResolvedMonitoringLocationTypeName']
    js['OrganizationFormalName']=r.json()["features"][i]['properties']['OrganizationFormalName']
    js['OrganizationIdentifier']=r.json()["features"][i]['properties']['OrganizationIdentifier']
    js['resultCount']=r.json()["features"][i]['properties']['resultCount']
    js['MonitoringLocationIdentifier']=r.json()["features"][i]['properties']['MonitoringLocationIdentifier']

    download=requests.get('https://www.waterqualitydata.us/Result/search?siteid='+r.json()["features"][i]['properties']['MonitoringLocationIdentifier']+'&mimeType=csv&zip=no&sorted=no')
    mydf = pd.read_csv(io.StringIO(download.content.decode('utf-8')))
    mydf['var']= mydf["CharacteristicName"] + ' - ' +mydf["ResultMeasure/MeasureUnitCode"]
    variables = mydf['var'].value_counts()
    if mydf['ActivityStartDate'].min() == float('nan'):
      js['start_date'] = mydf['ActivityStartDate'].min()
    else:
      js['start_date'] = None;
    if mydf['ActivityStartDate'].max() == float('nan'):
      js['end_date'] = mydf['ActivityStartDate'].max()
    else:
      js['end_date'] = None;

    if mydf['ActivityStartDate'].max() < mydf['ActivityEndDate'].max():
      js['end_date'] = mydf['ActivityEndDate'].max()
    js['keywords'] =",".join(variables.keys())
    js['variables'] = variables.to_dict()
    body['published'] = 'True'
    body['value'] = js
    body['geospatial']= True;
    with open('sites/'+r.json()["features"][i]['properties']['MonitoringLocationIdentifier']+'.json', 'w') as outfile:
        json.dump(body, outfile)
    call("metadata-addupdate -z "+token+" -V -F sites/"+r.json()["features"][i]['properties']['MonitoringLocationIdentifier']+".json", shell=True)
input = range(site_count)
results = Parallel(n_jobs=num_cores)(delayed(catalogSite)(i) for i in input)
