from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table

def stage_bookings():
    df = spark.readStream.table('flights.bronze.source_bookings')
    return df

@dp.view
def trans_bookings():
    df = spark.readStream.table('stage_bookings')
    df = df.withColumn('amount',col('amount').cast(DoubleType())).withColumn('modifiedDate',current_timestamp()).withColumn('booking_date',to_date(col('booking_date'))).drop('_rescued_data')
    return df


rules = {
    "rule_one":"booking_id IS NOT NULL",
    "rule_two":"passenger_id IS NOT NULL"
}

@dp.table
@dp.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table('trans_bookings')
    return df


## Flights Data

@dp.view
def trans_flight():
    df = spark.readStream.table('flights.bronze.source_flights')
    df = df.withColumn('modifiedDate',current_timestamp()).drop('_rescued_data')
    return df

dp.create_streaming_table('silver_flights')

dp.create_auto_cdc_flow(
    target = 'silver_flights',
    source = 'trans_flight',
    keys = ['flight_id'],
    sequence_by = col('modifiedDate'),
    stored_as_scd_type = 1
    )

## Passengers Data

@dp.view
def trans_passengers():
    df = spark.readStream.table('flights.bronze.source_passengers')
    df = df.withColumn('modifiedDate',current_timestamp()).drop('_rescued_data')
    return df

dp.create_streaming_table('silver_passengers')

dp.create_auto_cdc_flow(
    target = 'silver_passengers',
    source = 'trans_passengers',
    keys = ['passenger_id'],
    sequence_by = col('modifiedDate'),
    stored_as_scd_type = 1
    )

## Airports

@dp.view
def trans_airports():
    df = spark.readStream.table('flights.bronze.source_airports')
    df = df.withColumn('modifiedDate',current_timestamp()).drop('_rescued_data')
    return df

dp.create_streaming_table('silver_airports')

dp.create_auto_cdc_flow(
    target = 'silver_airports',
    source = 'trans_airports',
    keys = ['airport_id'],
    sequence_by = col('modifiedDate'),
    stored_as_scd_type = 1
    )

## Silver Business View

@dp.table

def silver_business():
   df = dp.readStream('silver_bookings').join(dp.readStream('silver_flights'),['flight_id']).join(dp.readStream('silver_passengers'),['passenger_id']).join(dp.readStream('silver_airports'),['airport_id']).drop('modifiedDate')
   return df










