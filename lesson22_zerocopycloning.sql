use database practice_db;


use schema raw_layer;

create or replace database practice_db_clone
clone practice_db ; -- internal stages or external tables wont cloned

desc database practice_db;


create or replace table DIMCUSTOMER_clone
clone DIMCUSTOMER;


-- if ID and clone_group_id are same , means  that tables are not cloned
-- For cloned tables , active_bytes=0 . ex:DIMCUSTOMER_CLONE
select * from information_schema.table_storage_metrics where table_catalog='PRACTICE_DB'; 



show tables;

select * from dimcustomer_clone where geographykey = 37 and customeralternatekey='AW00011001';

--update clone table
update dimcustomer_clone set customerkey=101, customeralternatekey='AWS0011002' where geographykey = 37 and customeralternatekey='AW00011001';

insert into dimcustomer_clone (customerkey,geographykey,customeralternatekey,firstname,lastname) values (1001,37,'AW00011001','rodricks','premkumar');

--above update affect only clone table
select * from dimcustomer_clone where geographykey = 37 and customeralternatekey='AWS0011002';

truncate table dimcustomer;

select * from dimcustomer_clone limit by 10;
select * from dimcustomer where geographykey = 37 and customeralternatekey='AW00011001';

--update original table
update dimcustomer set customerkey=101, customeralternatekey='AWS0011001_origin' where geographykey = 37 and customeralternatekey='AW00011001';

--above update affect only origin table
select * from dimcustomer where geographykey = 37 and customeralternatekey='AWS0011001_origin';

select * from dimcustomer_clone where geographykey = 37 and customeralternatekey='AWS0011001_origin';

