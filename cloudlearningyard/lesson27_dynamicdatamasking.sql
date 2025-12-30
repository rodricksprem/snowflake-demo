use database practice_db;
use schema raw_layer;
-- masking policy to mask first nam
create or replace masking policy mask_ssn_no as (ssnno string) returns string -> 
    case
        when current_role() in ('SYSADMIN') then ssnno
        when current_role() in ('CALL_CNETER_AGENT') then regexp_replace(ssnno, substring(ssnno, 1, 7), 'xxx-xx-')
        when current_role() in ('PROD_SUPP_MEMBER') then 'xxx-xx-xxxx'
        else 'masked'
    end;
create or replace table customer(
    id number,
    first_name string,
    last_name string,
    DoB string,
    ssn string,
    country string,
    city string,
    zipcode string); 

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (1, 'John', 'Miller', '1988-03-14', '123-45-4321', 'USA', 'New York', '10001');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (2, 'Priya', 'Sharma', '1992-07-22', '234-56-9876', 'India', 'Bengaluru', '560001');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (3, 'Carlos', 'Gomez', '1985-11-05', '345-67-2468', 'Mexico', 'Guadalajara', '44100');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (4, 'Emma', 'Wilson', '1990-01-18', '456-78-1357', 'UK', 'London', 'SW1A1AA');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (5, 'Daniel', 'Brown', '1979-09-30', '567-89-8642', 'USA', 'Chicago', '60601');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (6, 'Aisha', 'Khan', '1995-06-12', '678-90-5791', 'UAE', 'Dubai', '00000');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (7, 'Lucas', 'Martin', '1987-12-03', '789-01-9087', 'France', 'Paris', '75001');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (8, 'Mei', 'Chen', '1993-04-27', '890-12-3146', 'China', 'Shanghai', '200000');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (9, 'Robert', 'Taylor', '1981-08-09', '901-23-7539', 'Canada', 'Toronto', 'M5H2N2');

INSERT INTO customer (id, first_name, last_name, DoB, ssn, country, city, zipcode)
VALUES (10, 'Sofia', 'Rossi', '1996-02-21', '012-34-6624', 'Italy', 'Milan', '20100');

select * from customer;

-- apply mask_fname masking policy to customer.ssn column
alter table customer modify column ssn set masking policy mask_ssn_no;

select * from customer;

-- masking policy to mask first name
create or replace masking policy mask_fname as (first_name string) returns string ->
case
    when current_role() in ('CALL_CNETER_AGENT') then 'xxx-xxx-xxx'
    when current_role() in ('PROD_SUPP_MEMBER') then 'xxx-xxx-xxx'
    when current_role() in ('SYSADMIN') then first_name
    else null
    
end;

-- apply mask_fname masking policy to customer.first_name column
alter table if exists customer modify column first_name set masking policy mask_fname;
-- to unset masking policy
--alter table if exists customer modify column first_name unset masking policy;
  -- masking policy to mask last name
create or replace masking policy mask_lname as (lname_txt string) returns string ->
  case
    when current_role() in ('CALL_CNETER_AGENT') then lname_txt
    when current_role() in ('PROD_SUPP_MEMBER') then 'xxxxxx'
    when current_role() in ('SYSADMIN') then lname_txt
    else NULL
  end;
-- apply mask_lname masking policy to customer.last_name column
alter table if exists customer modify column last_name set masking policy mask_lname;

-- masking policy to mask date of birth name
create or replace masking policy mask_dob as (dob_txt string) returns string ->
  case
    when current_role() in ('CALL_CNETER_AGENT') then regexp_replace(dob_txt,substring(dob_txt,1,8),'xxxx-xx-')
    when current_role() in ('PROD_SUPP_MEMBER') then 'xxxx-xx-xx'
    when current_role() in ('SYSADMIN') then dob_txt
    
    else NULL
  end;

-- apply mask_dob masking policy to customer.dob column
alter table if exists customer modify column dob set masking policy mask_dob;

select * from customer;

select * from customer where ssn='678-90-5791';

--Conditional Data Masking Policies

-- DDL for user table
create or replace table user
(
    id number,
    first_name string,
    last_name string,
    DoB string,
    highest_degree string,
    visibility boolean,
    city string,
    zipcode string
);

-- User table sample dataset
insert into user values
(100,'Francis','Rodriquez','1988-01-27','Graduation',true,'Atlanta',30301),
( 101,'Abigail','Nash','1978-09-18',   'Post Graduation',false,'Denver',80201),
( 102,'Kasper','Short','1996-07-29', 'None',false,'Phoenix',85001);

-- create conditional masking policy using visibility field
create or replace masking policy mask_degree as (degree_txt string,visibility boolean) returns string ->
  case
    when visibility = true then degree_txt
    else '***Masked***'
end;
-- apply masking policy
alter table if exists user modify column highest_degree set masking policy mask_degree using (highest_degree,visibility);

select * from user;


