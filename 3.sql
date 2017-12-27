CREATE OR REPLACE FUNCTION airlines.bookings.airport_placard(
  intrvl   INTERVAL,
  air_code CHAR(3)
)
  RETURNS TABLE(
    flight_id      INTEGER,
    flight_no      CHAR(6),
    airport_code   CHAR(6),
    airport_name   TEXT,
    airport_city   TEXT,
    departure_time TIMESTAMP WITH TIME ZONE,
    arrival_time   TIMESTAMP WITH TIME ZONE,
    status         VARCHAR(20),
    is_departure   BOOLEAN
  ) AS $$
BEGIN
  RETURN QUERY
  SELECT
    f.flight_id                                         AS flight_id,
    f.flight_no                                         AS flight_no,
    CASE WHEN (f.departure_airport = air_code)
      THEN f.arrival_airport
    ELSE f.departure_airport END                        AS airport_code,
    CASE WHEN (f.departure_airport = air_code)
      THEN arr_airport.airport_name
    ELSE dep_airport.airport_name END                   AS airport_name,
    CASE WHEN (f.departure_airport = air_code)
      THEN arr_airport.city
    ELSE dep_airport.city END                           AS airport_city,
    coalesce(f.actual_departure, f.scheduled_departure) AS departure_time,
    coalesce(f.actual_arrival, f.scheduled_arrival)     AS arrival_time,
    f.status                                            AS status,
    CASE WHEN (f.departure_airport = air_code)
      THEN FALSE
    ELSE TRUE END                                       AS is_departure
  FROM airlines.bookings.flights AS f
    JOIN airlines.bookings.airports AS dep_airport ON f.departure_airport = dep_airport.airport_code
    JOIN airlines.bookings.airports AS arr_airport ON f.arrival_airport = arr_airport.airport_code
  WHERE (f.departure_airport = air_code AND
         age(coalesce(f.actual_departure, f.scheduled_departure), airlines.bookings.now()) <= intrvl AND
         age(coalesce(f.actual_departure, f.scheduled_departure), airlines.bookings.now()) >= INTERVAL '0 hours') OR
        (f.arrival_airport = air_code AND
         age(coalesce(f.actual_arrival, f.scheduled_arrival), airlines.bookings.now()) <= intrvl AND
         age(coalesce(f.actual_arrival, f.scheduled_arrival), airlines.bookings.now()) >= INTERVAL '0 hours');
END;
$$ LANGUAGE plpgsql;