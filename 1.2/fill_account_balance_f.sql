CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Логирование начала операции
    INSERT INTO LOGS.ETL_LOGS (event_timestamp, event_type, table_name)
    VALUES (CURRENT_TIMESTAMP, 'START', 'DM.DM_ACCOUNT_BALANCE_F');
    
    -- Удаление данных за указанную дату (идемпотентность)
    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;
    
    -- Вставка рассчитанных остатков
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    WITH 
    -- Актуальные счета на дату расчета
    actual_accounts AS (
        SELECT 
            account_rk,
            char_type,
            currency_rk
        FROM DS.MD_ACCOUNT_D
        WHERE data_actual_date <= i_OnDate 
          AND (data_actual_end_date >= i_OnDate OR data_actual_end_date IS NULL)
    ),
    
    -- Остатки за предыдущий день
    prev_day_balances AS (
        SELECT 
            account_rk,
            balance_out,
            balance_out_rub
        FROM DM.DM_ACCOUNT_BALANCE_F
        WHERE on_date = i_OnDate - INTERVAL '1 day'
    ),
    
    -- Обороты за текущий день
    current_day_turnovers AS (
        SELECT 
            account_rk,
            credit_amount,
            credit_amount_rub,
            debet_amount,
            debet_amount_rub
        FROM DM.DM_ACCOUNT_TURNOVER_F
        WHERE on_date = i_OnDate
    ),
    
    -- Актуальные курсы валют
    currency_rates AS (
        SELECT 
            currency_rk,
            reduced_cource
        FROM DS.MD_EXCHANGE_RATE_D
        WHERE data_actual_date <= i_OnDate 
          AND (data_actual_end_date >= i_OnDate OR data_actual_end_date IS NULL)
    ),
    
    -- Расчет новых остатков
    calculated_balances AS (
        SELECT 
            a.account_rk,
            CASE 
                WHEN a.char_type = 'А' THEN 
                    COALESCE(p.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
                WHEN a.char_type = 'П' THEN 
                    COALESCE(p.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
                ELSE 0
            END AS balance_out,
            CASE 
                WHEN a.char_type = 'А' THEN 
                    COALESCE(p.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
                WHEN a.char_type = 'П' THEN 
                    COALESCE(p.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
                ELSE 0
            END AS balance_out_rub
        FROM actual_accounts a
        LEFT JOIN prev_day_balances p ON a.account_rk = p.account_rk
        LEFT JOIN current_day_turnovers t ON a.account_rk = t.account_rk
    )
    
    SELECT 
        i_OnDate,
        account_rk,
        balance_out,
        balance_out_rub
    FROM calculated_balances;
    
    -- Логирование завершения
    INSERT INTO LOGS.ETL_LOGS (event_timestamp, event_type, table_name, rows_processed)
    VALUES (CURRENT_TIMESTAMP, 'FINISH', 'DM.DM_ACCOUNT_BALANCE_F', 
           (SELECT COUNT(*) FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate));
END;
$$;