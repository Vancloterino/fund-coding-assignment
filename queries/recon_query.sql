WITH latest_bond_prices AS (
    SELECT 
        a.symbol,
        a.data_date,
        bp.price,
        ROW_NUMBER() OVER (
            PARTITION BY a.symbol, a.data_date
            ORDER BY bp.DATETIME DESC
        ) AS rn
    FROM 
        applebead a
    LEFT JOIN 
        bond_prices bp ON a.symbol = bp.ISIN
        AND bp.DATETIME::DATE <= a.data_date
    WHERE 
        a.financial_type = 'Government Bond'
),

latest_equity_prices AS (
    SELECT 
        a.symbol,
        a.data_date,
        eqp.price,
        ROW_NUMBER() OVER (
            PARTITION BY a.symbol, a.data_date
            ORDER BY eqp.DATETIME DESC
        ) AS rn
    FROM 
        applebead a
    LEFT JOIN 
        equity_prices eqp ON a.symbol = eqp.SYMBOL
        AND eqp.DATETIME::DATE <= a.data_date
    WHERE 
        a.financial_type = 'Equities'
)

SELECT 
    a.data_date,
    a.symbol,
    a.financial_type AS fin_type,
    CASE 
        WHEN fin_type = 'Equities' THEN COALESCE(c.price, lep.price)
        WHEN fin_type = 'Government Bond' THEN COALESCE(b.price, lbp.price)
    END AS ref_price,
    a.price AS fund_price,
    fund_price - ref_price AS diff
FROM 
    applebead a
LEFT JOIN 
    bond_prices b ON a.symbol = b.ISIN
    AND a.data_date = b.DATETIME
    AND a.financial_type = 'Government Bond'
LEFT JOIN 
    equity_prices c ON a.symbol = c.SYMBOL
    AND a.data_date = c.DATETIME
    AND a.financial_type = 'Equities'
LEFT JOIN 
    latest_bond_prices lbp ON a.symbol = lbp.symbol
    AND a.data_date = lbp.data_date
    AND lbp.rn = 1
LEFT JOIN 
    latest_equity_prices lep ON a.symbol = lep.symbol
    AND a.data_date = lep.data_date
    AND lep.rn = 1
WHERE 
    fin_type <> 'CASH'
ORDER BY 
    fin_type DESC, 
    a.data_date ASC;