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
SELECT abc.date,abc.city,abc.hub,abc.oyo_id,abc.cluster,abc.booking_source,abc.booking_status
,abc.prepaid_flag,SUM(abc.BRN)as brn,abc.direct_pah_flag
from
( SELECT cast(A.date1 as varchar) as date,hs.oyo_id,coalesce(C.prepaid_flag,0) as prepaid,
hs.cluster,hs.city,hs.hub,hs.oyo_product as Product,

case 
        when b.source in (22,23,38) and b.micro_market_id is null then 'App'
        when b.source in (17,21) and b.micro_market_id is null then 'Web'
        when b.source in (0,7)  and (b.micro_market_id is null OR b.micro_market_id = 99) then 'Direct'
        when b.source = 4  and b.micro_market_id is null then 'Walkin'
        when b.source in (1,10,64)  and b.micro_market_id is null then 'OTA'
        when b.micro_market_id is not null and b.micro_market_id > 0 and b.micro_market_id not in (99) then 'MM'
        else 'Rest' end as booking_source,
    
case 
        when (bb.fci_flag is null or bb.fci_flag = 0) and b.status in (values 1,2) then 'organic urn'
        when b.status in (3) then 'canc'
        when b.status in (4) then 'no show'
        when b.status in (13) then 'void'
        when b.status in (0) then 'confirm' 
        else 'other' end as booking_status, 
            
coalesce(c.prepaid_flag,0) as prepaid_flag 
--,b.DA_Flag as DA_flag 
,sum(b.oyo_rooms) as BRN,b.id as id,
b.direct_pah_flag

from
(SELECT checkin,id,hotel_id,oyo_rooms,created_at,status,
case when date(checkin) = date(checkout) then date(checkout) + interval '1' day else date(checkout) end as checkout1,
--Case when lower(notes) like '%digital audit%' then 1 else 0 end as DA_Flag,
force_prepaid,
source,micro_market_id,
(case when cancellation_reason = 13 and status in (3,13) then 1 else 0 end) as direct_pah_flag
FROM ordering_realtime.bookings
WHERE status in (0,1,2,3,4,13) and inserted_at >= '202102'
and source not in (values 13,14)
) b
INNER JOIN
(SELECT DISTINCT date(cal.d) as date1 FROM default.calendar as cal
WHERE date(cal.d) BETWEEN date(current_date - interval '2' day) and (Current_date)
)A
ON A.date1 >= date(b.checkin) and A.date1 < date(b.checkout1)
left join
(select distinct(p.booking_id) as booking_id, 1 as prepaid_flag
from (select id,merchant,status,is_visible,booking_id,payment_type,amount from ordering_realtime.payment_transacs p where inserted_at >= '202102') p
where p.merchant not in (values 0,1,2,17,35,34,37,45,47,48,49,50,67,73,44,6,46,56,42,16,65,31,57) and p.status= 1 and p.is_visible = TRUE)
C ON b.id=C.booking_id
inner join (select hotel_id,oyo_id,cluster_name as cluster,city_name as city, hub_name as hub, oyo_product
from aggregatedb.hotels_summary
where country_id = 1 and oyo_product not in ('OYO_Living','OYO X','RS-MG-Amount','Marketplace')
and strpos(oyo_id,'OYO')=0 
and oyo_id not in ('DEL814','GRG1992')
and hub_name in ('Jaipur')
)hs
on hs.hotel_id = b.hotel_id

left join (select id,
(case when lower(notes) not like ('%force checked in%') then 0 else 1 end) as fci_flag
from ingestiondb.bookings_base
where inserted_at >= '202102' and status in (0,1,2,3,4,13)
and source not in (4)) as bb on bb.id = b.id

group by 1,2,3,4,5,6,7,8,9,10,12,13) as abc
group by 1,2,3,4,5,6,7,8,10
order by 1
'''

Live_all = pd.read_sql(Live_all, conn_presto)

sh2 = gc.open_by_key('1buvzRXT2NbBqU3fEDgreACuykNSjYbeACbBuh5wX1xs')

wks2 = sh2.worksheet('title','FTD')

wks2.clear(start='A1', end='J100000')

wks2.set_dataframe(Live_all, (1,1), fit = False, copy_head = True)

print("Live_all")
