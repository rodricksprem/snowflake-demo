use database practice_db;

use schema raw_layer;


show tables;

select * from DIMCUSTOMER limit 10;


alter table DIMCUSTOMER 
set data_retention_time_in_days=10;

alter schema raw_layer set data_retention_time_in_days=15;

show tables;

drop schema raw_layer;

undrop schema raw_layer; -- can be undrop before data_retention_time expires


select * from DIMCUSTOMER limit 100; 

update DIMCUSTOMER set customerkey=100; -- wrong update


select * from DIMCUSTOMER limit 100; 

--need to retrive old data 

select * from dimcustomer at (offset=> -60*5) limit 100;

select * from dimcustomer at (timestamp=> '2025-12-25T12:45:00+05:30') limit 100;--Future data is not yet available for table DIMCUSTOMER.

--TIMESTAMP_TZ: TIMESTAMP_TZ internally stores UTC values together with an associated time zone offset. When a time zone isn't provided, the session time zone offset is used. All operations are performed with the time zone offset specific to each record.
select * from dimcustomer at (timestamp=> '2025-12-25T12:30:00+05:30'::TIMESTAMP_TZ) limit 100;

select * from dimcustomer before (statement=> '01c1480e-0004-2689-000a-9f0e000472ea') limit 100;



