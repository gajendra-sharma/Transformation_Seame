# -- coding: utf-8 --
"""
Created on Tue Aug  17 21:25:33 2021
@author: Hemant Kumar
"""

import pandas as pd
import pygsheets as pg
from pyhive import presto
from datetime import date, timedelta
import boto3
from google.oauth2 import service_account
import google.auth
from tempfile import NamedTemporaryFile
import numpy as np

 

try:

    conn_presto = presto.Connection(host="presto.oyorooms.io", port = 8889, username='gajendra.sharma@oyorooms.com' )

except:

    conn_presto= None



def downloadFileFromS3(bucket, path):
  s3 = boto3.resource('s3').Bucket(bucket)
  tmp = NamedTemporaryFile(delete=False)
  s3.download_fileobj(path, tmp)
  return tmp.name
   
# Downloading credentials from S3
cred_file = downloadFileFromS3('prod-datapl-r-scheduler','team/central_supply/credentials/hemantkumar-313310-5da6889f7de7.json')
gc = pg.authorize(service_account_file=cred_file)



Live_all = '''
select  case 
                       when hs.display_category like '%Palette%' then 'Palette'
                       when hs.display_category like '%OYO X' then 'CapitalO'
                       else hs.oyo_product end as oyo_product,
                       ls.oyo_id, ls.contracted_rooms,ls.name,ls.city_name as city,
                       case 
                       when ls.cluster_name = 'Airport Delhi' then 'Delhi'
                       when ls.cluster_name = 'Connaught Place Delhi' then 'Delhi'
                       when ls.cluster_name = 'Karol Bagh Delhi' then 'Delhi'
                       when ls.cluster_name = 'Paharganj Delhi' then 'Delhi'
                       when ls.cluster_name = 'Kashmere Gate Delhi' then 'Delhi'
                       when ls.cluster_name = 'Pragati Maidan Delhi' then 'Delhi'
                       else ls.hub_name end as hub, ls.cluster_name as cluster, ls.d,
                       case 
                       when restriction = 'Y' then used_rooms
                       else sellable_rooms end as sellable_rooms, date_format(psc.time,'%b''%y') as month_year,
                       psc.time as live_date
                       from aggregatedb.live_srn ls inner join aggregatedb.hotels_summary hs on hs.oyo_id=ls.oyo_id
                       left join 
                       (select * from (select *,row_number() over (partition by oyo_id order by time) row
                       from aggregatedb.property_status_changes
                       where new_status='Live')
                       where row=1 )
                       psc on psc.oyo_id=ls.oyo_id
                       where d= current_date and ls.country_id in (1)  and hs.agreement_type IN (5,6,7) and ls.status = 2
'''

Live_all = pd.read_sql(Live_all, conn_presto)

sh2 = gc.open_by_key('1IeqdjCOoa8Zs6h9igFOb5QxC_cVwGG2RFKFkb9Vdufs')

wks2 = sh2.worksheet('title','Live_all')

wks2.clear(start='A1', end='K24000')

wks2.set_dataframe(Live_all, (1,1), fit = False, copy_head = True)

print("Live_all")
