use database practice_db;

use schema raw_layer;

show tables;

insert into employees (eid,ename) values (1001,'rodricks');
select * from employees limit 10;

create or replace stream employees_stream on table employees ;


select * from employees_stream;

insert into employees (eid,ename) values (1002,'prem');


select * from employees_stream;

insert into employees (eid,ename) values (1003,'flavio');

select * from employees_stream;
delete from employees where eid = 1001;

select * from employees_stream;

update employees set ename='RODRICKS Prem' where eid=1002;

select * from employees_stream;

update employees set eid=1004 where eid=1003;

SELECT SYSTEM$STREAM_HAS_DATA('dimcustomer_stream');

show streams;

select * from employees;

show tables;

alter table dimcustomer set max_data_extension_time_in_days=20; --default 14 days, after that data in stream become stale.


