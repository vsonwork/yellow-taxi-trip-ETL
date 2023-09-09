select 	ts."VendorID", p.latitude as dropoff_latitude, p.longitude as dropoff_longitude,
		d.longitude as pickup_longitude, d.latitude as pickup_latitude,
        ts.pickup_weekday, ts.pickup_hour,
        ts.dropoff_weekday, ts.dropoff_hour,
        ts.passenger_count,
        ts.trip_distance,
        ts.rate_code_name, ts.payment_type_name,
        ts.fare_amount,
        ts.extra,
        ts.mta_tax,
        ts.tip_amount,
        ts.tolls_amount,
        ts.improvement_surcharge,
        ts.total_amount
from {{source("public", "trip_schedule")}} ts
join {{source("public", "pickup_location_lat_lon")}} p on ts."pickup_location_ID"= p."pickup_location_ID"
join {{source("public", "dropoff_location_lat_lon")}} d on ts."dropoff_location_ID" = d."dropoff_location_ID"
