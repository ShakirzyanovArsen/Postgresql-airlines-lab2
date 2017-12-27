CREATE OR REPLACE FUNCTION airlines.bookings.hex_to_int(hexval VARCHAR)
  RETURNS BIGINT AS $$
DECLARE
  result BIGINT;
BEGIN
  EXECUTE 'SELECT x''' || hexval || '''::bigint'
  INTO result;
  RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION airlines.bookings.sell_tickets(
  flights            INT [],
  fare_conditions    VARCHAR(10) [],
  passenger_ids      VARCHAR [],
  passenger_names    TEXT [],
  contact_data_array JSONB [],
  ticket_costs       NUMERIC(10, 2) []
)
  RETURNS CHAR(6) AS $$
DECLARE
  result        CHAR(6);
  new_ticket_no CHAR(13);
  total_cost    NUMERIC(10, 2);
  val           NUMERIC(10, 2);
BEGIN
  BEGIN
    SELECT to_hex(airlines.bookings.hex_to_int(MIN(b.book_ref)) - 1)
    FROM airlines.bookings.bookings AS b
    INTO result;
    total_cost = 0;
    FOREACH val IN ARRAY ticket_costs LOOP
      total_cost := total_cost + val;
    END LOOP;

    result = lpad(result, 6, '0');
        INSERT INTO airlines.bookings.bookings (book_ref, book_date, total_amount) VALUES (result, now(), total_cost);
    FOR i IN 1..array_length(flights, 1) LOOP
      IF NOT exists(SELECT 1
                    FROM airlines.bookings.available_seats AS av_seats
                    WHERE av_seats.flight_id = flights [i] AND av_seats.fare_conditions = $2[i]
                          AND av_seats.count >= array_length(passenger_ids, 1))
      THEN
        RAISE EXCEPTION 'Нет мест';
      END IF;
      FOR j IN 1..array_length(passenger_ids, 1) LOOP
        SELECT to_hex(airlines.bookings.hex_to_int(MIN(t.ticket_no)) - 1)
        FROM airlines.bookings.tickets AS t
        INTO new_ticket_no;
        INSERT INTO airlines.bookings.tickets (ticket_no, book_ref, passenger_id, passenger_name, contact_data)
        VALUES (new_ticket_no, result, passenger_ids [j], passenger_names [j], contact_data_array [j]);
        INSERT INTO airlines.bookings.ticket_flights (ticket_no, flight_id, fare_conditions, amount)
        VALUES (new_ticket_no, $1 [i], $2 [i], $6 [i]);
      END LOOP;
    END LOOP;
  END;
  REFRESH MATERIALIZED VIEW airlines.bookings.available_seats;
  RETURN result;
END;
$$ LANGUAGE plpgsql