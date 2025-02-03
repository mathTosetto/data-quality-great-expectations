CREATE SCHEMA production;

CREATE TABLE production.taxi_data (
    vendor_id INT NOT NULL,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INT NOT NULL,
    trip_distance REAL NOT NULL,
    rate_code_id INT NOT NULL,
    store_and_fwd_flag VARCHAR(10) NOT NULL,
    pickup_location_id INT NOT NULL,
    dropoff_location_id INT NOT NULL,
    payment_type INT NOT NULL,
    fare_amount REAL NOT NULL,	
    extra REAL NOT NULL,
    mta_tax REAL NOT NULL,
    tip_amount REAL NOT NULL,
    tolls_amount REAL NOT NULL,
    improvement_surcharge REAL NOT NULL,
    total_amount REAL NOT NULL,
    congestion_surcharge REAL
);
