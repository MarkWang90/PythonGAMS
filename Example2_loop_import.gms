*** this example imports the county-level USDA crop mix data from USDA NASS using loop
*** loop is used since single querying exceeds the 50,000 limit.

* On the major platforms, GMSPYTHONHOME gets set automatically, otherwise the user has to set it
* This condition can also be removed, if one has set up its Python environment appropriately
$if not setenv GMSPYTHONHOME $abort.noError Embedded code Python not ready to be used

set countyfips    /system.empty/
    cropall /system.empty/
    irrigstatus       /system.empty/
    mixesa /system.empty/
   ;
parameter cropmixdata(countyfips,cropall,irrigstatus,mixesa) original crop mix data from usda nass;


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~
* start of Python code
$onEmbeddedCode Python:
pd.options.mode.chained_assignment = None
import nass
import pandas as pd
import numpy as np
import os
os.chdir("D:\\GDrive\\water\\RioGrande_Data_Mark\\CropBudgetPDF\\WriteToGAMS")


api = nass.NassApi('4A3B90E6-199F-344A-B466-3236FE813C2B')

#see source information at: view-source:https://quickstats.nass.usda.gov/

columns_to_keep = ['year','freq_desc','state_fips_code','county_code','asd_code','group_desc',
'commodity_desc','prodn_practice_desc','short_desc','statisticcat_desc','unit_desc','Value']

cropmix=pd.DataFrame()
year=range(1975,2018)
source=['CENSUS', 'SURVEY']

## get the nass data
for thisyear in year:
    q = api.query()
#    q.filter('source_desc','SURVEY')
    q.filter('sector_desc','CROPS')
    q.filter('agg_level_desc','COUNTY')
    q.filter('state_name','TEXAS')
    q.filter('year',thisyear)
    q.filter('unit_desc','ACRES')
    if q.count()>0:
        thisyeardf=pd.DataFrame(q.execute())[columns_to_keep]
        cropmix=cropmix.append(thisyeardf)
        print('working on year '+str(thisyear)+ ', importing total of '+ str(q.count())+ ' observations')
    else:
        print('no data in year '+str(thisyear))


## some clean ups
print('raw nass data dimension is '+str(cropmix.shape))
cropmix=cropmix.loc[cropmix['group_desc'] != 'HORTICULTURE']  #remove horticulture
print(str(cropmix.shape)+" after remove horticulture")

cropmix=cropmix.loc[cropmix['statisticcat_desc'] == 'AREA HARVESTED']  #only use area harvested
print(str(cropmix.shape)+" only use area harvested")

conditions = [
 cropmix['prodn_practice_desc']=='IRRIGATED',
 cropmix['prodn_practice_desc']=='NON-IRRIGATED',
 cropmix['prodn_practice_desc']=='NON-IRRIGATED, CONTINUOUS CROP',
 cropmix['prodn_practice_desc']=='NON-IRRIGATED, FOLLOWING SUMMER FALLOW'  ]

choices=['irrigated','dryland','dryland','dryland']
cropmix['irrigstatus']=np.select(conditions,choices,default="other")

cropmix=cropmix.loc[cropmix['irrigstatus'] != 'other']  #only keep dry/irrigated land
print(str(cropmix.shape)+" only keep dry/irrigated land")

cropmix=cropmix.loc[cropmix['county_code'] != '998']  #remove other county combined
print(str(cropmix.shape)+" after remove other county combined")

cropmix=cropmix.loc[cropmix['Value'] != '                 (D)'] #remove incomplete data
print(str(cropmix.shape)+" after remove incomplete data")


cropmix['fips']=cropmix['state_fips_code']+cropmix['county_code']
cropmix_to_export=cropmix[['fips','commodity_desc','irrigstatus','year','Value']]

cropmix_list=cropmix_to_export.values.tolist()
for i in range(len(cropmix_list)):
    try:
        temp=cropmix_list[i][4].replace(',','')
        int(temp)
    except:
        print(cropmix_list[i])

cropmix_to_export['value_float']=[int(x.replace(',','')) for x in cropmix_to_export['Value']]
cropmix_to_export['sum_value']=cropmix_to_export.groupby(['fips','commodity_desc','irrigstatus','year'])['value_float'].transform('sum')
cropmix_to_export=cropmix_to_export[['fips','commodity_desc','irrigstatus','year','sum_value']].drop_duplicates()
cropmix_list=cropmix_to_export.values.tolist()

countyfips=list(set(cropmix_to_export['fips']))
cropall=list(set(cropmix_to_export['commodity_desc']))
irrigstatus=list(set(cropmix_to_export['irrigstatus']))
mixesa=list(set(cropmix_to_export['year']))
cropmixdata=[( x[0],x[1],x[2],x[3],x[4] ) for x in cropmix_list]

### part 4 GAMS get data


gams.set("countyfips",countyfips)
gams.set("cropall",cropall)
gams.set("irrigstatus",irrigstatus)
gams.set("mixesa",mixesa)
gams.set("cropmixdata",cropmixdata)

$offEmbeddedCode countyfips  cropall  irrigstatus  mixesa   cropmixdata
*end of python code
* ~~~~~~~~~~~~

display countyfips,  cropall,  irrigstatus,  mixesa,   cropmixdata;


