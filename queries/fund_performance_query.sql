WITH consolidated_funds AS (
    SELECT * FROM applebead
    UNION ALL
    SELECT * FROM belaware
    UNION ALL
    SELECT * FROM fund_whitestone
    UNION ALL
    SELECT * FROM leeder
    UNION ALL
    SELECT * FROM magnum
    UNION ALL
    SELECT * FROM mend_report_wallington
    UNION ALL
    SELECT * FROM report_of_gohen
    UNION ALL
    SELECT * FROM rpt_catalysm
    UNION ALL
    SELECT * FROM tt_monthly_trustmind
    UNION ALL
    SELECT * FROM virtous
),

bond_prices_bmc AS (
    SELECT
        DATETIME,
        ISIN,
        PRICE
    FROM (
        SELECT
            DATETIME,
            ISIN,
            PRICE,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    ISIN,
                    DATE_TRUNC('month', DATETIME::DATE)
                ORDER BY DATETIME::DATE ASC
            ) AS rn
        FROM bond_prices
    ) sub
    WHERE rn = 1
),

equity_prices_bmc AS (
    SELECT
        DATETIME,
        SYMBOL,
        PRICE
    FROM (
        SELECT
            DATETIME,
            SYMBOL,
            PRICE,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    SYMBOL,
                    DATE_TRUNC('month', DATETIME::DATE)
                ORDER BY DATETIME::DATE ASC
            ) AS rn
        FROM equity_prices
    ) sub
    WHERE rn = 1
),

cte4 AS (
    SELECT 
        source,
        DATE_TRUNC('month', data_date::DATE) AS date_trunc,
        SUM(a.realised_pl) AS total_pl,
        SUM(a.market_value) AS fund_mv_end,
        SUM(
            CASE 
                WHEN a.financial_type = 'Government Bond' THEN a.quantity * b.price
                WHEN a.financial_type = 'Equities' THEN a.quantity * c.price
            END
        ) AS fund_mv_start
    FROM consolidated_funds a
    LEFT JOIN bond_prices_bmc b
        ON a.SYMBOL = b.ISIN
        AND a.FINANCIAL_TYPE = 'Government Bond'
    LEFT JOIN equity_prices_bmc c
        ON a.SYMBOL = c.SYMBOL
        AND a.FINANCIAL_TYPE = 'Equities'
    WHERE a.financial_type <> 'CASH'
    GROUP BY source, date_trunc
    ORDER BY source, date_trunc
),

cte5 AS (
    SELECT 
        date_trunc,
        source,
        ((fund_mv_end - fund_mv_start + total_pl) / fund_mv_start) * 100 AS rate_of_return,
        ROW_NUMBER() OVER (
            PARTITION BY 
                date_trunc
            ORDER BY rate_of_return DESC
        ) AS rn
    FROM cte4
)

SELECT 
    date_trunc AS date_month,
    source AS fund_name,
    rate_of_return
FROM cte5
WHERE rn = 1
ORDER BY date_trunc;