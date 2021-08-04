--landing business data

DROP TABLE IF EXISTS stg_yelp_business;
CREATE TABLE IF NOT EXISTS stg_yelp_business
(
address varchar(max),
AcceptsInsurance varchar,
AgesAllowed varchar,
Alcohol varchar,
Ambience varchar(max),
BYOB varchar,
BYOBCorkage varchar,
BestNights varchar,
BikeParking varchar,
BusinessAcceptsBitcoin varchar,
BusinessAcceptsCreditCards varchar,
BusinessParking varchar(max),
ByAppointmentOnly varchar,
Caters varchar,
CoatCheck varchar,
Corkage varchar,
DietaryRestrictions varchar,
DogsAllowed varchar,
DriveThru varchar,
GoodForDancing varchar(max),
GoodForKids varchar(max),
GoodForMeal varchar(max),
HairSpecializesIn varchar,
HappyHour varchar,
HasTV varchar,
Music varchar,
NoiseLevel varchar,
Open24Hours varchar,
OutdoorSeating varchar,
RestaurantsAttire varchar,
RestaurantsCounterService varchar,
RestaurantsDelivery varchar,
RestaurantsGoodForGroups varchar,
RestaurantsPriceRange2 varchar,
RestaurantsReservations varchar,
RestaurantsTableService varchar,
RestaurantsTakeOut varchar,
Smoking varchar,
WheelchairAccessible varchar,
WiFi varchar,
business_id varchar ,
categories varchar(max),
city varchar,
hours varchar(max),
is_open integer,
latitude Decimal(8,6),
longitude Decimal(9,6),
name varchar,
postal_code varchar distkey,
review_count bigint,
stars double precision,
state varchar
) SORTKEY AUTO;
  
  

DROP TABLE IF EXISTS stg_yelp_checkin;
CREATE TABLE IF NOT EXISTS stg_yelp_checkin
(business_id varchar,
 date TIMESTAMP);
 

 
 --remove friends
DROP TABLE IF EXISTS stg_yelp_users;
CREATE TABLE IF NOT EXISTS stg_yelp_users
(average_stars double precision distkey,
 compliment_cool bigint,
 compliment_cute bigint,
 compliment_funny bigint,
 compliment_hot bigint,
 compliment_list bigint,
 compliment_more bigint,
 compliment_note bigint,
 compliment_photos bigint,
 compliment_plain bigint,
 compliment_profile bigint,
 compliment_writer bigint,
 cool bigint,
 elite varchar(max),
 fans bigint,
 funny bigint,
 name varchar,
 review_count bigint,
 user_id varchar,
 yelping_since timestamp ,
 primary key(user_id)
);




DROP TABLE IF EXISTS stg_yelp_users_friends;
CREATE TABLE IF NOT EXISTS stg_yelp_users_friends
(user_id varchar,
 friend_id varchar ,
 primary key(user_id)
);

 

  
DROP TABLE IF EXISTS stg_yelp_reviews;
CREATE TABLE IF NOT EXISTS stg_yelp_reviews
(_corrupt_record varchar(max) ,
business_id varchar,
 cool bigint,
 date varchar,
 funny bigint,
 review_id varchar,
 stars double precision,
 text varchar(max),
 useful bigint,
 user_id varchar(max),
 primary key(review_id)
)


/*
Table for the weather data

*/
DROP TABLE IF EXISTS stg_noaa_weathers;
CREATE TABLE IF NOT EXISTS stg_noaa_weathers
( date varchar,
  location_id varchar,
 city_name varchar,
 average_temp FLOAT4,
 station varchar,
 long Decimal(9,6),
 lat Decimal(8,6),
 prcp FLOAT4,
  primary key(date,location_id,station)
);

DROP TABLE IF EXISTS ldg_noaa_weathers;
CREATE TABLE IF NOT EXISTS ldg_noaa_weathers
( date varchar,
  location_id varchar,
 city_name varchar,
 average_temp FLOAT4,
 station varchar,
 long Decimal(9,6),
 lat Decimal(8,6),
 prcp FLOAT4
)


/*
dimensional model for analytics
*/


DROP TABLE IF EXISTS fdn_dim_business;
CREATE TABLE IF NOT EXISTS fdn_dim_business
(
business_id varchar ,
name varchar,
address varchar(max),
 latitude Decimal(8,6),
longitude Decimal(9,6),
  categories varchar(max),
RestaurantsPriceRange2 varchar,
review_count bigint,
stars double precision,
primary key(business_id)
) SORTKEY AUTO;

DROP TABLE IF EXISTS fdn_dim_users;
CREATE TABLE IF NOT EXISTS fdn_dim_users
( user_id varchar,
  average_stars double precision distkey,
 is_influencer int,
 friends_count bigint,
 review_count bigint,
 useful bigint,
 yelping_since timestamp ,
 primary key(user_id)
);

DROP TABLE IF EXISTS fdn_dim_weather;
CREATE TABLE IF NOT EXISTS fdn_dim_weather
( date timestamp,
 business_id varchar,
 average_temp FLOAT4,
 prcp FLOAT4,
 avglong Decimal(9,6),
 avglat Decimal(8,6),
 avgdist FLOAT4,
 primary key(date,business_id)
);


DROP TABLE IF EXISTS fdn_fact_reviews;
CREATE TABLE IF NOT EXISTS fdn_fact_reviews
(date timestamp,
 business_id varchar,
 user_id varchar(max),
 review_id varchar,
 stars double precision,
 useful bigint,
 cool bigint,
 funny bigint,
 text varchar(max),
 checkin_count float,
 primary key(date,business_id,user_id,review_id)
);
