-- 1. Создание схем DS и LOGS
CREATE SCHEMA IF NOT EXISTS DS;
CREATE SCHEMA IF NOT EXISTS LOGS;
CREATE SCHEMA IF NOT EXISTS DM;

-- 2. Логовая таблица для ETL-процессов
CREATE TABLE IF NOT EXISTS LOGS.ETL_LOGS (
    log_id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50) NOT NULL,  -- 'START', 'FINISH', 'ERROR'
    table_name VARCHAR(50),           -- Имя целевой таблицы
    rows_processed INTEGER,           -- Количество обработанных строк
    details TEXT                      -- Дополнительная информация (ошибки, предупреждения)
);

-- 3. Таблицы детального слоя DS

-- 3.1. Таблица балансов (FT_BALANCE_F)
CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    currency_rk NUMERIC,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk)
);

-- 3.2. Таблица проводок (FT_POSTING_F)
CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
    oper_date DATE NOT NULL,
    credit_account_rk NUMERIC NOT NULL,
    debet_account_rk NUMERIC NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);
-- Примечание: Первичный ключ отсутствует (по ТЗ таблица полностью перезаписывается)

-- 3.3. Таблица счетов (MD_ACCOUNT_D)
CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk NUMERIC NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk)
);

-- 3.4. Таблица валют (MD_CURRENCY_D)
CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
    currency_rk NUMERIC NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);

-- 3.5. Таблица курсов валют (MD_EXCHANGE_RATE_D)
CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk NUMERIC NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);

-- 3.6. Таблица балансовых счетов (MD_LEDGER_ACCOUNT_S)
CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1),
    PRIMARY KEY (ledger_account, start_date)
);


CREATE INDEX IF NOT EXISTS idx_ft_balance_f_account_rk ON DS.FT_BALANCE_F (account_rk);
CREATE INDEX IF NOT EXISTS idx_md_account_d_currency_rk ON DS.MD_ACCOUNT_D (currency_rk);



CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_TURNOVER_F (
    on_date DATE,
    account_rk NUMERIC,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);


CREATE TABLE IF NOT EXISTS DM.DM_F101_ROUND_F (
    from_date DATE,
    to_date DATE,
    chapter CHAR(1),
    ledger_account CHAR(5),
    characteristic CHAR(1),
    balance_in_rub NUMERIC(23,8),
    r_balance_in_rub NUMERIC(23,8),
    balance_in_val NUMERIC(23,8),
    r_balance_in_val NUMERIC(23,8),
    balance_in_total NUMERIC(23,8),
    r_balance_in_total NUMERIC(23,8),
    turn_deb_rub NUMERIC(23,8),
    r_turn_deb_rub NUMERIC(23,8),
    turn_deb_val NUMERIC(23,8),
    r_turn_deb_val NUMERIC(23,8),
    turn_deb_total NUMERIC(23,8),
    r_turn_deb_total NUMERIC(23,8),
    turn_cre_rub NUMERIC(23,8),
    r_turn_cre_rub NUMERIC(23,8),
    turn_cre_val NUMERIC(23,8),
    r_turn_cre_val NUMERIC(23,8),
    turn_cre_total NUMERIC(23,8),
    r_turn_cre_total NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    r_balance_out_rub NUMERIC(23,8),
    balance_out_val NUMERIC(23,8),
    r_balance_out_val NUMERIC(23,8),
    balance_out_total NUMERIC(23,8),
    r_balance_out_total NUMERIC(23,8),
    PRIMARY KEY (from_date, to_date, chapter, ledger_account, characteristic)
);


CREATE INDEX IF NOT EXISTS idx_dm_account_turnover_f_date ON DM.DM_ACCOUNT_TURNOVER_F (on_date);
CREATE INDEX IF NOT EXISTS idx_dm_f101_round_f_dates ON DM.DM_F101_ROUND_F (from_date, to_date);