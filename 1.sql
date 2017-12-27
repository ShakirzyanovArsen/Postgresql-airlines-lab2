CREATE INDEX ON airlines.bookings.flights (arrival_airport);
CREATE INDEX ON airlines.bookings.flights (arrival_airport);
CREATE INDEX ON airlines.bookings.flights (departure_airport);
CREATE INDEX ON airlines.bookings.airports (airport_code);
CREATE INDEX ON airlines.bookings.seats (aircraft_code, fare_conditions);

CREATE MATERIALIZED VIEW airlines.bookings.available_seats AS
  SELECT
    flights.flight_id,
    seats_tmp.fare_conditions,
    seats_tmp.count - ticket_flights_tmp.count as count
  FROM airlines.bookings.flights AS flights
    JOIN LATERAL (SELECT
                    seats.fare_conditions,
                    count(seats.seat_no) AS "count",
                    seats.aircraft_code
                  FROM airlines.bookings.seats AS seats
                  WHERE seats.aircraft_code = flights.aircraft_code
                  GROUP BY seats.aircraft_code, seats.fare_conditions) AS seats_tmp ON TRUE
    JOIN (SELECT
            ticket_flights.fare_conditions,
            count(ticket_flights.ticket_no) AS "count",
            ticket_flights.flight_id
          FROM airlines.bookings.ticket_flights AS ticket_flights
          GROUP BY ticket_flights.fare_conditions, ticket_flights.flight_id) AS ticket_flights_tmp
      ON seats_tmp.fare_conditions = ticket_flights_tmp.fare_conditions AND
         ticket_flights_tmp.flight_id = flights.flight_id;
CREATE INDEX ON airlines.bookings.available_seats (flight_id);

CREATE INDEX ON airlines.bookings.ticket_flights (flight_id, fare_conditions);

CREATE OR REPLACE FUNCTION airlines.bookings.flights_available_seats
  (
    flight_id INTEGER
  )
  RETURNS TABLE(
    fc  VARCHAR(10),
    cnt INTEGER
  ) AS
$$
DECLARE temp_cond  VARCHAR(10);
        temp_count INTEGER;
BEGIN
  DROP TABLE IF EXISTS tmp_av_seats;
  CREATE TEMPORARY TABLE IF NOT EXISTS tmp_av_seats (
    fair_condition VARCHAR(10) PRIMARY KEY,
    count          INTEGER
  ) ON COMMIT DROP;
  INSERT INTO tmp_av_seats (fair_condition, count)
    SELECT
      seats.fare_conditions,
      count(seats.seat_no) AS "count"
    FROM airlines.bookings.seats AS seats
    WHERE seats.aircraft_code = (
      SELECT fl.aircraft_code
      FROM airlines.bookings.flights AS fl
      WHERE fl.flight_id = $1
    )
    GROUP BY seats.fare_conditions;

  FOR temp_cond, temp_count IN
  SELECT
    ticket_flights.fare_conditions,
    count(ticket_flights.ticket_no) AS "count"
  FROM airlines.bookings.ticket_flights AS ticket_flights
  WHERE ticket_flights.flight_id = $1
  GROUP BY ticket_flights.fare_conditions
  LOOP
    UPDATE tmp_av_seats
    SET count = count - temp_count
    WHERE fair_condition = temp_cond;
  END LOOP;

  RETURN QUERY SELECT
                 fair_condition,
                 count
               FROM tmp_av_seats
               WHERE count > 0;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION bookings.get_available_flights(
  "from" CHAR(3),
  "to"   CHAR(3),
  sort1  VARCHAR(255) DEFAULT NULL,
  sort2  VARCHAR(255) DEFAULT NULL
)
  RETURNS TABLE(
    flight_ids     INTEGER [],
    flight_nums    CHAR(6) [],
    time_in_flight INTERVAL,
    departure      TIMESTAMP,
    arrival        TIMESTAMP
  ) AS $$
DECLARE query TEXT;
BEGIN
  CREATE TEMPORARY SEQUENCE IF NOT EXISTS temp_flights_seq;
  CREATE TEMPORARY TABLE temp_flights (
    id             INT DEFAULT nextval('temp_flights_seq'),
    flight_ids     INTEGER [],
    flight_nums    CHAR(6) [],
    time_in_flight INTERVAL,
    departure      TIMESTAMP,
    arrival        TIMESTAMP,
    docking_count  INTEGER
  ) ON COMMIT DROP;
  CREATE INDEX ON temp_flights (time_in_flight);

  INSERT INTO temp_flights (flight_ids, flight_nums, time_in_flight, departure, arrival, docking_count)
    SELECT
      ARRAY [flights.flight_id],
      ARRAY [flights.flight_no],
      age(flights.scheduled_arrival, flights.scheduled_departure),
      flights.scheduled_departure,
      flights.scheduled_arrival,
      1
    FROM airlines.bookings.airports AS air_from
      JOIN airlines.bookings.flights AS flights ON air_from.airport_code = flights.departure_airport
      JOIN airlines.bookings.airports AS air_to ON flights.arrival_airport = air_to.airport_code
    WHERE air_from.airport_code = $1 AND air_to.airport_code = $2
          AND flights.scheduled_departure > airlines.bookings.now()
          AND (SELECT COUNT(av_seats2.flight_id)
               FROM airlines.bookings.available_seats AS av_seats2
               WHERE flights.flight_id = av_seats2.flight_id) > 0;

  INSERT INTO temp_flights (flight_ids, flight_nums, time_in_flight, departure, arrival, docking_count)
    SELECT
      ARRAY [flight1.flight_id, flight2.flight_id],
      ARRAY [flight1.flight_no, flight2.flight_no],
      age(flight2.scheduled_arrival, flight1.scheduled_departure) AS time_in_flight,
      flight1.scheduled_departure,
      flight2.scheduled_arrival,
      2
    FROM airlines.bookings.airports AS air_from
      JOIN airlines.bookings.flights AS flight1
        ON air_from.airport_code = flight1.departure_airport AND flight1.status != 'Canceled'
      JOIN airlines.bookings.flights AS flight2
        ON flight1.arrival_airport = flight2.departure_airport AND flight2.status != 'Canceled'
      JOIN airlines.bookings.airports AS air_to ON flight2.arrival_airport = air_to.airport_code
    WHERE air_from.airport_code = $1 AND air_to.airport_code = $2
          AND flight1.scheduled_departure > airlines.bookings.now()
          AND flight2.scheduled_departure > flight1.scheduled_arrival + '40 minutes'
          AND flight2.scheduled_arrival - flight1.scheduled_departure < '2 days' :: INTERVAL
          AND (SELECT COUNT(av_seats1.flight_id)
               FROM airlines.bookings.available_seats AS av_seats1
               WHERE flight1.flight_id = av_seats1.flight_id) > 0
          AND (SELECT COUNT(av_seats2.flight_id)
               FROM airlines.bookings.available_seats AS av_seats2
               WHERE flight2.flight_id = av_seats2.flight_id) > 0;

  INSERT INTO temp_flights (flight_ids, flight_nums, time_in_flight, departure, arrival, docking_count)
    SELECT
      ARRAY [flight1.flight_id, flight2.flight_id, flight3.flight_id],
      ARRAY [flight1.flight_no, flight2.flight_no, flight3.flight_no],
      age(flight3.scheduled_arrival, flight1.scheduled_departure) AS time_in_flight,
      flight1.scheduled_departure,
      flight3.scheduled_arrival,
      3
    FROM airlines.bookings.airports AS air_from
      JOIN airlines.bookings.flights AS flight1
        ON air_from.airport_code = flight1.departure_airport AND flight1.status != 'Canceled'
      JOIN airlines.bookings.flights AS flight2
        ON flight1.arrival_airport = flight2.departure_airport AND flight2.status != 'Canceled'
      JOIN airlines.bookings.flights AS flight3
        ON flight2.arrival_airport = flight3.departure_airport AND flight3.status != 'Canceled'
      JOIN airlines.bookings.airports AS air_to ON flight3.arrival_airport = air_to.airport_code
    WHERE air_from.airport_code = $1 AND air_to.airport_code = $2
          AND flight1.scheduled_departure > airlines.bookings.now()
          AND flight2.scheduled_departure > flight1.scheduled_arrival + '40 minutes'
          AND flight3.scheduled_departure > flight2.scheduled_arrival + '40 minutes'
          AND flight3.scheduled_arrival - flight1.scheduled_departure < '2 days' :: INTERVAL
          AND (SELECT COUNT(av_seats1.flight_id)
               FROM airlines.bookings.available_seats AS av_seats1
               WHERE flight1.flight_id = av_seats1.flight_id) > 0
          AND (SELECT COUNT(av_seats2.flight_id)
               FROM airlines.bookings.available_seats AS av_seats2
               WHERE flight2.flight_id = av_seats2.flight_id) > 0
          AND (SELECT COUNT(av_seats3.flight_id)
               FROM airlines.bookings.available_seats AS av_seats3
               WHERE flight3.flight_id = av_seats3.flight_id) > 0;
  "query" = 'SELECT
                 temp_flights.flight_ids,
                 temp_flights.flight_nums,
                 temp_flights.time_in_flight,
                 temp_flights.departure,
                 temp_flights.arrival
               FROM temp_flights ';
  IF NOT sort1 ISNULL THEN
    "query" = "query" || 'ORDER BY temp_flights.' || sort1;
  END IF;
  IF NOT sort2 ISNULL AND sort1 ISNULL THEN
    "query" = "query" || 'ORDER BY temp_flights.' || sort2;
  END IF;
  IF NOT sort2 ISNULL AND NOT sort1 ISNULL THEN
    "query" = "query" || ' ,temp_flights.' || sort2;
  END IF;
  RETURN QUERY EXECUTE "query";
END;
$$ LANGUAGE plpgsql