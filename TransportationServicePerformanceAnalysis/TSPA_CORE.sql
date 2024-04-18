create table car_type(
car_type_id number,
car_color varchar(50)
);

select * from car_type ;

truncate table car_type;

drop table car_type;

create table location(
location_id number,
BOROUGH varchar(50),
ZONE varchar(50),
SERVICE_ZONE varchar(50))

select * from location;

create table rate_code(
rate_code_id number,
rate_code varchar(20)
);

create table vendor(
vendor_id number,
vendor_name varchar(30)
);

create table payment_type(
payment_type_id number,
payment_type varchar(20)
);

select * from payment_type