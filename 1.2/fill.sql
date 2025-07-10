INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
    on_date,
    account_rk,
    balance_out,
    balance_out_rub
)
SELECT 
    '2017-12-31'::DATE,
    b.account_rk,
    b.balance_out,
    b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
FROM DS.FT_BALANCE_F b
LEFT JOIN DS.MD_ACCOUNT_D a ON 
    a.account_rk = b.account_rk
    AND a.data_actual_date <= '2017-12-31' 
  LEFT JOIN DS.MD_EXCHANGE_RATE_D er ON 
    er.currency_rk = a.currency_rk
    AND er.data_actual_date <= '2017-12-31'
   WHERE b.on_date = '2017-12-31';

DO $$
DECLARE
    calc_date DATE;
BEGIN
    FOR calc_date IN SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, '1 day'::INTERVAL)::DATE
    LOOP
        CALL ds.fill_account_turnover_f(calc_date);
    END LOOP;
END;
$$;   