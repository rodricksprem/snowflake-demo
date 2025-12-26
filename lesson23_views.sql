use database practice_db;
use schema raw_layer;

-- non-materialized view
select * from snowflake_sample_data.tpch_sf1.customer;

select * from snowflake_sample_data.tpch_sf1.customer where c_nationkey in (13,15);
-- rows abstraction
create or replace view vw_customer_us as select * from snowflake_sample_data.tpch_sf1.customer where c_nationkey in (13,15);

select * from vw_customer_us;

--column abstraction

create or replace view vw_customer_colabstract_us as select * exclude(c_acctbal) from snowflake_sample_data.tpch_sf1.customer where c_nationkey in (13,15);

select * from vw_customer_colabstract_us;

--materialized view

create or replace materialized view mvw_customer_us as select * from snowflake_sample_data.tpch_sf1.customer where c_nationkey in (13,15);


select * from mvw_customer_us;

-- join table view vs materialized view 

create or replace view vw_customer_colabstract_us as select * exclude (c_acctbal) from snowflake_sample_data.tpch_sf1.customer c inner join snowflake_sample_data.tpch_sf1.nation n 
on c.c_nationkey=n.n_nationkey;

select * from vw_customer_colabstract_us;

-- when we try to join more one table in materialized view we got the below error:
--SQL compilation error: error line 0 at position -1 Invalid materialized view definition. More than one table referenced in the view definition
create or replace materialized view mvw_customer_colabstract_us as select * exclude (c_acctbal) from snowflake_sample_data.tpch_sf1.customer c inner join snowflake_sample_data.tpch_sf1.nation n 
on c.c_nationkey=n.n_nationkey;


--secure view
--it is slower in performance so use it when really need to secure the base table and its columns


create or replace secure view secure_vw_customer_colabstract_us as select * exclude (c_acctbal) from snowflake_sample_data.tpch_sf1.customer c inner join snowflake_sample_data.tpch_sf1.nation n 
on c.c_nationkey=n.n_nationkey;

show views;

select * from vw_customer_colabstract_us;


select * from secure_vw_customer_colabstract_us;

