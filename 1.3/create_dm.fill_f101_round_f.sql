CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$

DECLARE
    v_FromDate DATE;
    v_ToDate DATE;
BEGIN
    -- Определение отчетного периода
    --Не уверен что правильно понял то нам нужен предыдущий месяц
    v_FromDate := DATE_TRUNC('month', i_OnDate - INTERVAL '1 month')::DATE;
    v_ToDate := (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')::DATE;
    
    INSERT INTO LOGS.ETL_LOGS (event_timestamp, event_type, table_name)
    VALUES (CURRENT_TIMESTAMP, 'START', 'DM.DM_F101_ROUND_F');
    
    DELETE FROM DM.DM_F101_ROUND_F 
    WHERE from_date = v_FromDate AND to_date = v_ToDate;
    

    INSERT INTO DM.DM_F101_ROUND_F (
        from_date,
        to_date,
        chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    )
    WITH 
    -- Актуальные счета за отчетный период
    actual_accounts AS (
        SELECT DISTINCT
            a.account_rk,
            a.char_type,
            a.currency_code,
            SUBSTRING(a.account_number, 1, 5) AS ledger_account_short,
            la.chapter
        FROM DS.MD_ACCOUNT_D a
        JOIN DS.MD_LEDGER_ACCOUNT_S la ON 
            la.ledger_account = SUBSTRING(a.account_number, 1, 5)::INTEGER
            AND la.start_date <= v_ToDate
            AND (la.end_date >= v_FromDate OR la.end_date IS NULL)
        WHERE a.data_actual_date <= v_ToDate
          AND (a.data_actual_end_date >= v_FromDate OR a.data_actual_end_date IS NULL)
    ),
    
    -- Остатки на начало периода (последний день предыдущего месяца)
    start_balances AS (
        SELECT 
            a.ledger_account_short,
            a.chapter,
            a.char_type,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_val,
            SUM(b.balance_out_rub) AS balance_in_total
        FROM actual_accounts a
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b ON 
            b.account_rk = a.account_rk
            AND b.on_date = v_FromDate - INTERVAL '1 day'
        GROUP BY a.ledger_account_short, a.chapter, a.char_type
    ),
    
    -- Остатки на конец периода (последний день отчетного месяца)
    end_balances AS (
        SELECT 
            a.ledger_account_short,
            a.chapter,
            a.char_type,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_out_val,
            SUM(b.balance_out_rub) AS balance_out_total
        FROM actual_accounts a
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b ON 
            b.account_rk = a.account_rk
            AND b.on_date = v_ToDate
        GROUP BY a.ledger_account_short, a.chapter, a.char_type
    ),
    
    -- Обороты за отчетный период
    turnovers AS (
        SELECT 
            a.ledger_account_short,
            a.chapter,
            a.char_type,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
            SUM(t.debet_amount_rub) AS turn_deb_total,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
            SUM(t.credit_amount_rub) AS turn_cre_total
        FROM actual_accounts a
        LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t ON 
            t.account_rk = a.account_rk
            AND t.on_date BETWEEN v_FromDate AND v_ToDate
        GROUP BY a.ledger_account_short, a.chapter, a.char_type
    )
    
    SELECT 
        v_FromDate,
        v_ToDate,
        s.chapter,
        s.ledger_account_short,
        s.char_type,
        COALESCE(s.balance_in_rub, 0),
        COALESCE(s.balance_in_val, 0),
        COALESCE(s.balance_in_total, 0),
        COALESCE(t.turn_deb_rub, 0),
        COALESCE(t.turn_deb_val, 0),
        COALESCE(t.turn_deb_total, 0),
        COALESCE(t.turn_cre_rub, 0),
        COALESCE(t.turn_cre_val, 0),
        COALESCE(t.turn_cre_total, 0),
        COALESCE(e.balance_out_rub, 0),
        COALESCE(e.balance_out_val, 0),
        COALESCE(e.balance_out_total, 0)
    FROM start_balances s
    JOIN turnovers t ON 
        s.ledger_account_short = t.ledger_account_short 
        AND s.chapter = t.chapter 
        AND s.char_type = t.char_type
    JOIN end_balances e ON 
        s.ledger_account_short = e.ledger_account_short 
        AND s.chapter = e.chapter 
        AND s.char_type = e.char_type;
    
    -- Логирование завершения
    INSERT INTO LOGS.ETL_LOGS (event_timestamp, event_type, table_name, rows_processed)
    VALUES (CURRENT_TIMESTAMP, 'FINISH', 'DM.DM_F101_ROUND_F', 
           (SELECT COUNT(*) FROM DM.DM_F101_ROUND_F WHERE from_date = v_FromDate AND to_date = v_ToDate));

END;
$$;