create table car_type(
car_type_id number,
car_color varchar(50)
);

select * from car_type ;

truncate table car_type;

drop table car_type;

create table location(
location_id number,
location varchar(100),
BOROUGH varchar(50),
ZONE varchar(50),
SERVICE_ZONE varchar(50)
);

select * from location;

drop table location;

create table rate_code(
rate_code_id number,
rate_code varchar(20)
);

select * from rate_code;

create table vendor(
vendor_id number,
vendor_name varchar(30)
);

select * from vendor;

create table payment_type(
payment_type_id number,
payment_type varchar(20)
);

select * from payment_type;

create table store_and_flag(
stf_id number,
stf char(2)
);

drop table store_and_flag;

select * from store_and_flag;

create table TSPA_CORE(
trip_id NUMBER,
vendor_id NUMBER,
trip_distance FLOAT,
pickup_datetime DATE,
dropoff_datetime DATE,
pickup_location_id NUMBER,
dropoff_location_id NUMBER,
passenger_count FLOAT,
rate_code_id NUMBER,
store_and_fwd_flag varchar(2),
payment_type_id NUMBER,
fare_amount FLOAT,
mta_tax FLOAT,
improvement_surcharge FLOAT,
tip_amount FLOAT,
tolls_amount FLOAT,
total_amount FLOAT,
congestion_surcharge FLOAT,
airport_fee FLOAT,
car_type NUMBER
);

drop table tspa_core;

select * from tspa_core
where extract(month from pickup_datetime) = 2;