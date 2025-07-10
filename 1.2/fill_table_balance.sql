DO $$
DECLARE
    calc_date DATE;
BEGIN
    FOR calc_date IN SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, '1 day'::INTERVAL)::DATE
    LOOP
        CALL ds.fill_account_balance_f(calc_date);
    END LOOP;
END;
$$;