*** this example imports the county-level USDA livestock heads data from USDA NASS


* On the major platforms, GMSPYTHONHOME gets set automatically, otherwise the user has to set it
* This condition can also be removed, if one has set up its Python environment appropriately
$if not setenv GMSPYTHONHOME $abort.noError Embedded code Python not ready to be used

set countyfips   /system.empty/
    usda_dist    /system.empty/
    livestockall /system.empty/
    mixesa       /system.empty/
   ;
parameter livestockmixnew(*,usda_dist,livestockall,mixesa) original livestock mix data from usda nass;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~
* start of Python code
$onEmbeddedCode Python:

import nass
import pandas as pd
import numpy as np
import os
os.chdir("D:\\GDrive\\water\\RioGrande_Data_Mark\\CropBudgetPDF\\WriteToGAMS")


#############################################################
### part 3 import livestock mix

api = nass.NassApi('4A3B90E6-199F-344A-B466-3236FE813C2B')

columns_to_keep = ['year','freq_desc','state_fips_code','county_code','asd_code',
'commodity_desc','prodn_practice_desc','short_desc','unit_desc','Value']

api.param_values('statisticcat_desc')
q = api.query()
q.filter('source_desc','SURVEY');q.count()
q.filter('sector_desc','ANIMALS & PRODUCTS');q.count()
q.filter('group_desc','LIVESTOCK');q.count()
q.filter('agg_level_desc','COUNTY');q.count()
q.filter('state_name','TEXAS');q.count()
#see source information at: view-source:https://quickstats.nass.usda.gov/

livestock_db2=pd.DataFrame(q.execute())
livestock_db = livestock_db2[columns_to_keep]

set(livestock_db['unit_desc'])
livestock_heads = livestock_db.loc[livestock_db['unit_desc']=='HEAD',]
set(livestock_heads['commodity_desc'])

short_desc=list(set(livestock_heads['short_desc']))

conditions = [
 livestock_heads['short_desc']=='CATTLE, COWS, MILK - INVENTORY',
 livestock_heads['short_desc']=='WOOL - SHORN, MEASURED IN HEAD',
 livestock_heads['short_desc']=='CATTLE, ON FEED - PLACEMENTS, MEASURED IN HEAD',

 livestock_heads['short_desc']=='SHEEP, INCL LAMBS - INVENTORY',
 livestock_heads['short_desc']=='GOATS, ANGORA - INVENTORY',
 livestock_heads['short_desc']=='CATTLE, INCL CALVES - INVENTORY',

 livestock_heads['short_desc']=='GOATS, MILK - INVENTORY',
 livestock_heads['short_desc']=='GOATS, MEAT & OTHER - INVENTORY',
 livestock_heads['short_desc']=='SHEEP, EWES, BREEDING, GE 1 YEAR - INVENTORY',

 livestock_heads['short_desc']=='GOATS - INVENTORY',
 livestock_heads['short_desc']=='CATTLE, ON FEED - SALES FOR SLAUGHTER, MEASURED IN HEAD',
 livestock_heads['short_desc']=='HOGS - INVENTORY',

 livestock_heads['short_desc']=='MOHAIR, ANGORA - CLIPPED, MEASURED IN HEAD',
 livestock_heads['short_desc']=='CATTLE, ON FEED - INVENTORY',
 livestock_heads['short_desc']=='CATTLE, COWS, BEEF - INVENTORY'
    ]

choices=['cattlecowmilk','sheepwool','cattleonfeed_placement',
        'sheeplambs','goatsangora','cattlecalves',
        'milkgoats','meatgoats','sheepewes',
        'goats','cattleonfeed4slaughter','hogs',
        'angora_mohair','cattleonfeed','beefcow']

livestock_heads['livestock1']=np.select(conditions,choices,default="other");print(livestock_heads.shape)
## the above chunk of code just rename to shorter names for livestocks, if you don't want any to be included, just change the corresponding
## choices to "other" and it would be dropped by the next line of code

livestock_heads = livestock_heads.loc[livestock_heads['livestock1'] != 'other'];print(livestock_heads.shape)
livestock_heads = livestock_heads.loc[livestock_heads['Value'] != '                 (D)',];print(livestock_heads.shape)
livestock_heads = livestock_heads.loc[livestock_heads['asd_code'] != '99',];print(livestock_heads.shape)
livestock_heads = livestock_heads.loc[livestock_heads['county_code'] != '998',];print(livestock_heads.shape)

livestock_heads['fips']=livestock_heads['state_fips_code']+livestock_heads['county_code']
livestock_export=livestock_heads[['fips','asd_code','livestock1','year','Value']]

livestockmixnew=livestock_export.values.tolist()

for i in range(len(livestockmixnew)):
    try:
        temp=livestockmixnew[i][4].replace(',','')
        int(temp)
    except :
        print(livestockmixnew[i])

countyfips=list(set(livestock_export['fips']))
usda_dist=list(set(livestock_export['asd_code']))
livestockall=list(set(livestock_export['livestock1']))
mixesa=list(set(livestock_export['year']))
livestockmixnew=[( x[0],x[1],x[2],x[3],int(x[4].replace(',','')) ) for x in livestockmixnew]


#############################################################
### part 4 GAMS get data

gams.set("countyfips",countyfips)
gams.set("usda_dist",usda_dist)
gams.set("livestockall",livestockall)
gams.set("mixesa",mixesa)
gams.set("livestockmixnew",livestockmixnew)


$offEmbeddedCode  countyfips usda_dist  livestockall mixesa livestockmixnew
display countyfips, usda_dist, livestockall, mixesa, livestockmixnew;
