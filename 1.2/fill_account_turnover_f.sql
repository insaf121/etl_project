CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Логирование начала операции
    INSERT INTO LOGS.ETL_LOGS (event_timestamp, event_type, table_name)
    VALUES (CURRENT_TIMESTAMP, 'START', 'DM.DM_ACCOUNT_TURNOVER_F');
    
    -- Удаление данных за указанную дату (идемпотентность)
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;
    
    -- Вставка оборотов по кредиту и дебету с конвертацией в рубли
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    WITH 
    -- Получаем актуальные курсы валют на дату расчета
    currency_rates AS (
        SELECT 
            currency_rk,
            reduced_cource
        FROM DS.MD_EXCHANGE_RATE_D
        WHERE data_actual_date <= i_OnDate 
          AND (data_actual_end_date >= i_OnDate OR data_actual_end_date IS NULL)
    ),
    
    -- Обороты по кредиту (где счет был кредитовым)
    credit_turnovers AS (
        SELECT 
            p.credit_account_rk AS account_rk,
            SUM(p.credit_amount) AS credit_amount,
            SUM(p.credit_amount * COALESCE(cr.reduced_cource, 1)) AS credit_amount_rub
        FROM DS.FT_POSTING_F p
        LEFT JOIN DS.MD_ACCOUNT_D a ON 
            a.account_rk = p.credit_account_rk
            AND a.data_actual_date = i_OnDate 
        LEFT JOIN currency_rates cr ON 
            cr.currency_rk = a.currency_rk
        WHERE p.oper_date = i_OnDate
        GROUP BY p.credit_account_rk
    ),
    
    -- Обороты по дебету (где счет был дебетовым)
    debet_turnovers AS (
        SELECT 
            p.debet_account_rk AS account_rk,
            SUM(p.debet_amount) AS debet_amount,
            SUM(p.debet_amount * COALESCE(cr.reduced_cource, 1)) AS debet_amount_rub
        FROM DS.FT_POSTING_F p
        LEFT JOIN DS.MD_ACCOUNT_D a ON 
            a.account_rk = p.debet_account_rk
            AND a.data_actual_date = i_OnDate
        LEFT JOIN currency_rates cr ON 
            cr.currency_rk = a.currency_rk
        WHERE p.oper_date = i_OnDate
        GROUP BY p.debet_account_rk
    )
    
    SELECT 
        i_OnDate,
        COALESCE(c.account_rk, d.account_rk) AS account_rk,
        COALESCE(c.credit_amount, 0) AS credit_amount,
        COALESCE(c.credit_amount_rub, 0) AS credit_amount_rub,
        COALESCE(d.debet_amount, 0) AS debet_amount,
        COALESCE(d.debet_amount_rub, 0) AS debet_amount_rub
    FROM credit_turnovers c
    FULL OUTER JOIN debet_turnovers d ON c.account_rk = d.account_rk;
    
    -- Логирование завершения
    INSERT INTO LOGS.ETL_LOGS (event_timestamp, event_type, table_name, rows_processed)
    VALUES (CURRENT_TIMESTAMP, 'FINISH', 'DM.DM_ACCOUNT_TURNOVER_F', (SELECT COUNT(*) FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate));
END;
$$;