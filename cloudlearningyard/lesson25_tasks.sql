use database practice_db;
use schema raw_layer;
show sequences;


create or replace task employeeinsert_task
warehouse = compute_wh
schedule  = '5 MINUTE'
as 
insert into employees values (my_db.my_schema_08.seq_01.nextval,'F_NAME')
;

show tasks;
-- execute task manally
execute task  employeeinsert_task;
--validating the tarkget table to verify does the task executed and inserted the record
select * from employees;
-- by default task are in suspend state, we ned to resume it
alter task employeeinsert_task resume ; 

--CHECK TASK HISTORY
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'EMPLOYEEINSERT_TASK';



-- using sererless compute and schedule using cron 
create or replace task customer_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' 
schedule  = 'USING CRON 25 * * * * UTC'
as 
insert into customer values (my_db.my_schema_08.seq_02.nextval,'C_NAME')

;

Alter task customer_task resume;

SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'CUSTOMER_TASK';

create or replace table logtable
(logid int, logtimestamp datetime);

-- while I try to exeucute below log task as child task , I got the below error
-- Error:Unable to update graph with root task PRACTICE_DB.RAW_LAYER.EMPLOYEEINSERT_TASK since that root task is not suspended.
-- Fix: I need to suspand the parent task

alter task employeeinsert_task suspend;

alter task customer_task suspend;


--Task Graph 

CREATE TASK task_root
  SCHEDULE = '1 MINUTE'
  AS SELECT 1;

create or replace task employee_task
warehouse = compute_wh
after task_root
as 
insert into employees values (my_db.my_schema_08.seq_01.nextval,'F_NAME')
;  

create or replace task customer_task
warehouse = compute_wh
after task_root
as 
insert into customer values (my_db.my_schema_08.seq_02.nextval,'C_NAME')
;  


create or replace task log_task
warehouse = compute_wh
after employee_task,customer_task
as
insert into logtable values (my_db.my_schema_08.seq_03.nextval, current_timestamp);


--CHECK DEPENTANT TASKS
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(TASK_NAME => 'LOG_TASK', RECURSIVE => TRUE));


SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) where NAME = 'LOG_TASK';

ALTER TASK log_task RESUME;

ALTER TASK employee_task RESUME;

ALTER TASK CUSTOMER_task RESUME;

ALTER TASK TASK_ROOT RESUME;


show tasks;

select * from logtable;



ALTER TASK log_task SUSPEND; --Unable to update graph with root task PRACTICE_DB.RAW_LAYER.TASK_ROOT since that root task is not suspended., so you need to suspend the parents first



-- correct order of suspend the task graph
ALTER TASK TASK_ROOT SUSPEND;

ALTER TASK employee_task SUSPEND;

ALTER TASK CUSTOMER_task SUSPEND;

ALTER TASK log_task SUSPEND; 

--correct order of resuming the task graph

ALTER TASK log_task RESUME;

ALTER TASK employee_task RESUME;

ALTER TASK CUSTOMER_task RESUME;

ALTER TASK TASK_ROOT RESUME;