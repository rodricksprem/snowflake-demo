use database practice_db;

use schema raw_layer;

show tables;

create table prodemployees like employees;
truncate table employees;
create or replace stream employees_stream on table employees ;

insert into employees (eid,ename) values (1001,'rodricks');
select * from employees limit 10;



select * from employees_stream;

insert into employees (eid,ename) values (1002,'prem');


select * from employees_stream;

insert into employees (eid,ename) values (1003,'flavio');

select * from employees_stream;
insert into prodemployees 
  select eid,ename from employees_stream;
 
delete from employees where eid = 1001;

select * from employees_stream;

update employees set ename='RODRICKS Prem' where eid=1002;



update employees set eid=1004 where eid=1003;
select * from employees_stream;

SELECT SYSTEM$STREAM_HAS_DATA('employees_stream'); -- False means no data in streams

show streams;

select * from employees;

show tables;

alter table dimcustomer set max_data_extension_time_in_days=20; --default 14 days, after that data in stream become stale.


-- Merge the changes from the stream. The graphic below this SQL explains 
-- how this processes all changes in one DML transaction.
merge into PRODEMPLOYEES P using
  (select * from EMPLOYEES_STREAM where METADATA$ACTION <> 'DELETE' or METADATA$ISUPDATE = false) S on P.EID = S.EID -- retrive all the records from stream whose action is 'INSERT' and isUpdate is True/False or action is 'Delete and isUpdate is True'
    when matched AND S.METADATA$ISUPDATE = false and S.METADATA$ACTION = 'DELETE' then -- delete from production table
      delete
    when matched AND S.METADATA$ISUPDATE = true then --insert into production table
      update set P.EID = S.EID, 
                 P.ENAME = S.ENAME
    when not matched then -- S.ID not present in Production Table
      insert (EID, ENAME) 
      values (S.EID, S.ENAME);

select * from prodemployees;

--Append only Stream
truncate table employees;
create or replace stream employeesappendonly_stream on table employees append_only=True; -- track only insert operations

insert into employees (eid,ename) values (1001,'rodricks');
select * from employees limit 10;



select * from employeesappendonly_stream;

insert into employees (eid,ename) values (1002,'prem');


select * from employeesappendonly_stream;

insert into employees (eid,ename) values (1003,'flavio');

select * from employeesappendonly_stream;
insert into prodemployees 
  select eid,ename from employeesappendonly_stream;
 
delete from employees where eid = 1001;

select * from employeesappendonly_stream; -- no effect as the stream is appendonly

update employees set ename='RODRICKS Prem' where eid=1002;



update employees set eid=1004 where eid=1003;
select * from employeesappendonly_stream; -- no effect of update 

insert into employees (eid,ename) values (1005,'benito');

select * from employeesappendonly_stream;

create or replace stream employeesappendonly_stream on table employees append_only=True; -- track only insert operations

create or replace stream employeesinsertonly_stream on table employees insert_only=True; -- Error:Streams of type INSERT_ONLY can only be created on external tables or Iceberg tables with an external catalog integration.

create or replace stream employeesinsertonly_stream on external table DEMO_EXT_TABLE insert_only=True;
show streams;

