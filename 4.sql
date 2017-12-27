CREATE OR REPLACE FUNCTION airlines.bookings.booking_info(
  book_ref CHAR(6)
)
  RETURNS TABLE(
    id                  INTEGER,
    ticket_no           CHAR(13),
    flight_id           INTEGER,
    flight_no           CHAR(6),
    scheduled_departure TIMESTAMP WITH TIME ZONE,
    scheduled_arrival   TIMESTAMP WITH TIME ZONE,
    departure_airport   CHAR(3),
    arrival_airport     CHAR(3),
    status              VARCHAR(20),
    aircraft_code       CHAR(3),
    actual_departure    TIMESTAMP WITH TIME ZONE,
    actual_arrival      TIMESTAMP WITH TIME ZONE,
    fare_conditions     VARCHAR(10),
    amount              NUMERIC(10, 2),
    replace_for         INTEGER
  ) AS $$
DECLARE
  rec                        RECORD;
  left_timestamp_constraint  TIMESTAMP WITH TIME ZONE;
  right_timestamp_constraint TIMESTAMP WITH TIME ZONE;
BEGIN
  CREATE TEMPORARY SEQUENCE IF NOT EXISTS temp_booking_info_seq;
  CREATE TEMPORARY TABLE IF NOT EXISTS temp_booking_info (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('temp_booking_info_seq'),
    ticket_no           CHAR(13),
    flight_id           INTEGER,
    flight_no           CHAR(6),
    scheduled_departure TIMESTAMP WITH TIME ZONE,
    scheduled_arrival   TIMESTAMP WITH TIME ZONE,
    departure_airport   CHAR(3),
    arrival_airport     CHAR(3),
    status              VARCHAR(20),
    aircraft_code       CHAR(3),
    actual_departure    TIMESTAMP WITH TIME ZONE,
    actual_arrival      TIMESTAMP WITH TIME ZONE,
    fare_conditions     VARCHAR(10),
    amount              NUMERIC(10, 2),
    replace_for         INTEGER
  ) ON COMMIT DROP;
  INSERT INTO temp_booking_info (ticket_no, flight_id, flight_no, scheduled_departure, scheduled_arrival,
                                 departure_airport, arrival_airport, status, aircraft_code, actual_departure,
                                 actual_arrival, fare_conditions, amount, replace_for)
    SELECT
      DISTINCT
      t_f.ticket_no,
      f.*,
      t_f.fare_conditions,
      t_f.amount,
      NULL::INTEGER AS replace_for
    FROM airlines.bookings.bookings AS b
      JOIN airlines.bookings.tickets AS t ON b.book_ref = t.book_ref
      JOIN airlines.bookings.ticket_flights AS t_f ON t.ticket_no = t_f.ticket_no
      JOIN airlines.bookings.flights AS f ON t_f.flight_id = f.flight_id
    WHERE b.book_ref = $1
    ORDER BY
      f.scheduled_departure,
      f.actual_departure NULLS LAST,
      f.scheduled_arrival,
      f.actual_arrival NULLS LAST;

  FOR rec IN SELECT *
             FROM temp_booking_info AS t_b_i
             WHERE t_b_i.status = 'Cancelled'
             ORDER BY t_b_i.id LOOP

    SELECT coalesce(t_b_i.actual_arrival, t_b_i.scheduled_arrival)
    FROM temp_booking_info AS t_b_i
    WHERE
      coalesce(t_b_i.actual_departure, t_b_i.scheduled_arrival) <
      coalesce(rec.actual_departure, rec.scheduled_departure)
      AND NOT t_b_i.id = rec.id AND NOT t_b_i.status = 'Cancelled'
    ORDER BY coalesce(t_b_i.actual_arrival, t_b_i.scheduled_arrival) DESC
    LIMIT 1
    INTO left_timestamp_constraint;

    SELECT coalesce(t_b_i.actual_departure, t_b_i.scheduled_departure)
    FROM temp_booking_info AS t_b_i
    WHERE
      (t_b_i.actual_departure, t_b_i.scheduled_departure) > coalesce(rec.actual_arrival, rec.scheduled_arrival)
      AND NOT t_b_i.id = rec.id AND NOT t_b_i.status = 'Cancelled'
    ORDER BY (t_b_i.actual_departure, t_b_i.scheduled_departure)
    LIMIT 1
    INTO right_timestamp_constraint;

    INSERT INTO temp_booking_info (flight_id, flight_no, scheduled_departure, scheduled_arrival,
                                 departure_airport, arrival_airport, status, aircraft_code, actual_departure,
                                 actual_arrival, fare_conditions, amount, replace_for)
    SELECT f.*, a_s.fare_conditions,rec.amount,rec.id FROM airlines.bookings.flights AS f
      JOIN airlines.bookings.available_seats AS a_s ON f.flight_id = a_s.fare_conditions
    WHERE
      ((right_timestamp_constraint ISNULL AND coalesce(f.actual_arrival, f.scheduled_arrival)  > airlines.bookings.now())
       OR coalesce(f.actual_arrival, f.scheduled_arrival) < right_timestamp_constraint)
      AND (left_timestamp_constraint ISNULL AND coalesce(f.actual_departure, f.scheduled_departure)  > airlines.bookings.now()
           OR coalesce(f.actual_departure, f.scheduled_departure) > left_timestamp_constraint)
      AND f.arrival_airport = rec.arrival_airport AND f.departure_airport = rec.departure_airport
      AND NOT f.status = 'Cancelled';
    right_timestamp_constraint = NULL;
    left_timestamp_constraint = NULL;
  END LOOP;
  RETURN QUERY SELECT *
               FROM temp_booking_info
               ORDER BY
                 temp_booking_info.scheduled_departure,
                 temp_booking_info.actual_departure NULLS LAST,
                 temp_booking_info.scheduled_arrival,
                 temp_booking_info.actual_arrival NULLS LAST;

END;
$$ LANGUAGE plpgsql;