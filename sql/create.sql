CREATE TABLE IF NOT EXISTS income_statement (
    ticker VARCHAR(20),
    date DATE,
    revenue DECIMAL(20 , 2 ),
    cost_of_revenue DECIMAL(20 , 2 ),
    gross_profit DECIMAL(20 , 2 ),
    operating_income DECIMAL(20 , 2 ),
    net_income DECIMAL(20 , 2 ),
    PRIMARY KEY (ticker , date)
);

CREATE TABLE IF NOT EXISTS balance_sheet (
    ticker VARCHAR(20),
    date DATE,
    total_assets DECIMAL(20,2),
    total_liabilities DECIMAL(20,2),
    total_equity DECIMAL(20,2),
    cash_and_cash_equivalents DECIMAL(20,2),
    short_term_debt DECIMAL(20,2),
    long_term_debt DECIMAL(20,2),
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS cash_flow (
    ticker VARCHAR(20),
    date DATE,
    operating_cash_flow DECIMAL(20,2),
    investing_cash_flow DECIMAL(20,2),
    financing_cash_flow DECIMAL(20,2),
    free_cash_flow DECIMAL(20,2),
    PRIMARY KEY (ticker, date)
);

