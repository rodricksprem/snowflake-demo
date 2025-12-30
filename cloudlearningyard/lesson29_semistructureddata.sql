use database practice_db;

use schema raw_layer;

select array_construct('java certified','EJB certified','AWS certified');

select * from table( flatten(input=>array_construct('java certified','EJB certified','AWS certified'),mode=>'Array'));


-- object construct

create or replace table employee_details(
id number,
name string,
designation string,
certification object

);

insert into employee_details select 1,'rodricks','Data Analyst', object_construct('snowflake core','790');

insert into employee_details select 2,'prem kumar','cloud architect', object_construct('aws solution architect','830','aws developer associate','900');

select * from employee_details;

select * from table (flatten (object_construct('aws solution architect','830','aws developer associate','900')));


-- recursive mode won't have any impact in this example
select id, name, designation , certification.key::string as certification_name ,certification.value::string as certification_score 
from employee_details, lateral flatten (input=>employee_details.certification,mode=>'Object',recursive=>True) as certification;


--both object and array types

create or replace transient table mobile (
 name varchar,
 brand varchar,
 front_camera varchar,
 rear_camera object,
 dim_lwh array 
);

insert into mobile select 'iphone12','apple', '12Mp',object_construct('Primary/Std','14Mp','Wide Angle','16Mp'), 
array_construct('16.49','8.96','2.82');

select name,brand,front_camera,rear_camera.key::string as camera_position, rear_camera.value::string as camera_resolution , dim_lwh.value::string as dimension from mobile, lateral flatten(input=> mobile.rear_camera,mode=>'object') rear_camera,lateral flatten(input=> mobile.dim_lwh,mode=>'array') dim_lwh;


-- json data 

create or replace table emp04
(
jsondata variant
);

insert into emp04 select parse_json('
{
    "id":1,
    "name":"Rodricks",
    "Certifications": [
    {
    "name":"Snowpro Core",
    "score": "900"
    },
    {
    "name":"Snowpro Core v2",
    "score": "920"
    },
    ],
    "Designation": "Data Analyst"
}
');

insert into emp04 select parse_json('
{
    "id":2,
    "name":"PremKumar",
    "Certifications": [
    {
    "name":"Snowpro Core",
    "score": "890"
    },
    {
    "name":"Snowpro Core v2",
    "score": "900"
    },
    ],
    "Designation": "Data Analyst"
}
');

select emp04.jsondata:id::string id , emp04.jsondata:name::string, 
emp04.jsondata:Certifications ,cert.*
from emp04,lateral flatten (input=>emp04.jsondata:Certifications)  cert;

select emp04.jsondata:id::string id , emp04.jsondata:name::string, 
emp04.jsondata:Certifications , cert.value:name::string as cert_name,cert.value:score::string as cert_score
 from emp04,lateral flatten (input=>emp04.jsondata:Certifications)  cert;



create or replace table emp05
(
jsondata variant
);
insert into emp05 
select parse_json('
{
"name": "rodricks",
"salary": 1400000,
"joining_date":"07-09-2018",
"programming_skills":[

{
"language":"Java",
"proficiency": "Beginner",
"experience":{
"version":"11",
"years":"10"
}
},
{
"language":"Python",
"proficiency": "Beginner",
"experience":{
"version":"13",
"years":"4"
}
}

],
"database_proficiency":[
{
"technology": "oracle",
"proficiency":"Beginner"

},
{
"technology": "postgress",
"proficiency":"Beginner"

}
],
"cloud_proficiency":[
    "AWS",
    "Oracle"
],
"Certifications":{
"snowflake":[
"snowflakepro "

]
,
"cloud":[
"AWS",
"OIC"
],
"java":{
"version":8,
"pass":"yes",
"score":"98%"
}

}
}');

select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    programming_skills.value:experience.version::string as version,
    programming_skills.value:experience.years::string as years,
    
    programming_skills.value:language::string as language,
    programming_skills.value:proficiency::string as proficiency,
    database_proficiency.VALUE:proficiency::string as proficiency,
    database_proficiency.VALUE:technology::string as technology,
    cloud_proficiency.*,
    Certifications.*,
    
    
from emp05,
lateral flatten(input=>jsondata:programming_skills, mode => 'array') programming_skills,
lateral flatten(input=>jsondata:database_proficiency) database_proficiency,
lateral flatten(input=>jsondata:cloud_proficiency) cloud_proficiency,
lateral flatten(input=>jsondata:Certifications) Certifications;



select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    
    Certifications.*,
    
    
from emp05,
lateral flatten(input=>jsondata:Certifications) Certifications;

select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    
    Certifications.*,
    
    
from emp05,
lateral flatten(input=>jsondata:Certifications,path=>'cloud') Certifications ;

select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    
    Certifications.*,
    
    
from emp05,
lateral flatten(input=>jsondata:Certifications,path=>'java') Certifications where key='pass' and value='yes';


select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    
    Certifications.*,
    
    
from emp05,
lateral flatten(input=>jsondata:Certifications,path=>'java') Certifications where path='java.pass' and value='yes';


--recursive usage , by default recursive is False, if it is True then it works like explode function
select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    
    Certifications.*,
    
    
from emp05,
lateral flatten(input=>jsondata:Certifications, recursive=>True) Certifications;


--outer usage. it is used to handle if inner objects or arrays are null



select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    
    Certifications.*,
    
    
from emp05,
lateral flatten(input=>jsondata:Certifications,path=>'java') Certifications ;



insert into emp05 
select parse_json('
{
"name": "premkumar",
"salary": 1400000,
"joining_date":"07-09-2018",
"programming_skills":[],
"database_proficiency":[
{
"technology": "oracle",
"proficiency":"Beginner"

},
{
"technology": "postgress",
"proficiency":"Beginner"

}
],
"cloud_proficiency":[
    "AWS",
    "Oracle"
],
"Certifications":{
"snowflake":[
"snowflakepro "

]
,
"cloud":[
"AWS",
"OIC"
],
"java":{
"version":8,
"pass":"yes",
"score":"98%"
}

}
}');
--as programming_skills is null for premkumar record, below query wont have premkumar record in the query result. to fix this we need to pass outer=True.
select 
    jsondata:name::string as name ,
    jsondata:salary::number as salary ,
    jsondata:joining_date::varchar as joining_date,
    
    programming_skills.*,
    
    
from emp05,
lateral flatten(input=>jsondata:programming_skills,outer=>True) programming_skills ;





-- retirve the rows
select * from jsondata
, lateral flatten(input=>datacolumn);

-- get the columns from each rows
select singleitem.value:Address,singleitem.value:CompanyName,singleitem.value:Email,singleitem.value:Empid,singleitem.value:Experience,singleitem.value:JoiningDate,singleitem.value:Name,singleitem.value:Phone from jsondata
, lateral flatten(input=>datacolumn) singleitem;

--cast columns to datatype
select singleitem.VALUE:Address::string as address,singleitem.value:CompanyName::string as companyname,singleitem.value:Email::string as email, singleitem.value:Empid::string as empid ,singleitem.value:Experience::string as experience ,singleitem.value:JoiningDate::string as joiningdate,singleitem.value:Name::string as name ,singleitem.value:Phone::string as phone from jsondata
, lateral flatten(input=>datacolumn)singleitem;


select * from xmldata;


select DATACOLUMN:"$" from xmldata; -- $ returns element's value
select DATACOLUMN:"@" from xmldata; -- @ returns element's name 

select * from xmldata, lateral flatten (datacolumn:"$");





select xmlget(datacolumn,'dept_id'):"$"::string as dept_id,
xmlget(datacolumn,'dept_name'):"$"::string as dept_name,
--xmlget(datacolumn,'employee'):"$" as employee,


xmlget(employee.value,'emp_id'):"$"::string as emp_id,
xmlget(employee.value,'emp_fname'):"$"::string as emp_fname,
xmlget(employee.value,'emp_lname'):"$"::string as emp_lname
from xmldata,lateral flatten (input=>xmldata.datacolumn:"$") employee; -- this will return unnecessary rows wherer employee details are null, so to over come that I have added below where condition


select xmlget(datacolumn,'dept_id'):"$"::string as dept_id,
xmlget(datacolumn,'dept_name'):"$"::string as dept_name,
--xmlget(datacolumn,'employee'):"$" as employee,

--employee.*,
xmlget(employee.value,'emp_id'):"$"::string as emp_id,
xmlget(employee.value,'emp_fname'):"$"::string as emp_fname,
xmlget(employee.value,'emp_lname'):"$"::string as emp_lname,

xmlget(employee.value,'emp_ssn'):"$"::string as emp_ssn,
--address.*,
xmlget(address.value,'street_1'):"$"::string as street_1,

xmlget(address.value,'street_2'):"$"::string as street_2,

xmlget(address.value,'city'):"$"::string as city,

xmlget(address.value,'state'):"$"::string as state,

xmlget(address.value,'zipcode'):"$"::string as zipcode,
from xmldata,lateral flatten (input=>xmldata.datacolumn:"$")  employee,
lateral flatten (input=>employee.value:"$")  address 
where --get(employee.value,'@')='employee';
--and 
get(address.value,'@')='address';


select * from xmldata, lateral flatten(xmldata.datacolumn:"$");