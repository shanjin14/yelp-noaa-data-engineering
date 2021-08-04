class SqlQueries:
    stg_noaa_upsert =("""
    update stg_noaa_weathers t
    set city_name=s.city_name, average_temp=s.average_temp, long=s.long,lat=s.lat,prcp=s.prcp
    from ldg_noaa_weathers s
    where t.date=s.date and t.location_id=s.location_id and t.station=s.station;
    
    
    INSERT INTO stg_noaa_weathers
    select s. * from ldg_noaa_weathers s 
    left join stg_noaa_weathers t on t.date=s.date and t.location_id=s.location_id and t.station=s.station
    where t.date is null and t.location_id is null and  t.station is null;
    """)
    
    
    dim_business_insert = ("""
        insert into fdn_dim_business
        select 
        business_id,
        name,
        address,
        latitude,
        longitude,
        categories,
        RestaurantsPriceRange2,
        review_count,
        stars
        from stg_yelp_business ;
    """)

    dim_users_insert = ("""
        insert into fdn_dim_users
        select 
        A.user_id,
        average_stars,
        coalesce(case when friend_cnt>50 then 1 else 0 end,0),
        coalesce(friend_cnt,0),
        review_count,
        useful,
        yelping_since
        from stg_yelp_users A
        left join (
        select user_id,count(distinct friend_id) friend_cnt from stg_yelp_users_friends
        group by user_id) B on A.user_id=B.user_id;
    """)

    dim_weather_insert = ("""
        insert into fdn_dim_weather
        WITH matched_business_id_station AS (
        Select row_number() over (partition by business_id order by dist) RID,*
        From (
        select distinct A.business_id,longitude,latitude,station,round(ST_DistanceSphere(ST_Point(A.longitude, A.latitude), ST_Point(B.long, B.lat)) / 1000, 0) dist
        from stg_yelp_business A cross join
        (select distinct station,long,lat from stg_noaa_weathers) B
        where round(ST_DistanceSphere(ST_Point(A.longitude, A.latitude), ST_Point(B.long, B.lat)) / 1000, 0) < 50
        ))
        Select to_timestamp(A.date, 'YYYY-MM-DDTHH:MI:SS')::date,C.business_id,avg(average_temp),avg(prcp),avg(long),avg(lat),avg(dist)
        from stg_noaa_weathers A
        join matched_business_id_station B on A.station=B.station
        join stg_yelp_business C on B.business_id=C.business_id
        where RID <=3
        group by to_timestamp(date, 'YYYY-MM-DDTHH:MI:SS')::date,C.business_id;
    """)
    
    fact_insert = ("""
    insert into fdn_fact_reviews
    select 
    to_timestamp(A.date, 'YYYY-MM-DD HH:MI:SS')::date,
    A.business_id,
    user_id,
    review_id,
    stars,
    useful,
    cool,
    funny,
    text,
    coalesce(checkin_count,0)*1.0/count(*) over (partition by to_timestamp(A.date, 'YYYY-MM-DD HH:MI:SS')::date,A.business_id) 
    from stg_yelp_reviews A 
    left join (
    select date::date as cidate,business_id,count(*) checkin_count from stg_yelp_checkin
    group by date::date,business_id) B on to_timestamp(A.date, 'YYYY-MM-DD HH:MI:SS')::date=cidate and A.business_id=B.business_id
    """)