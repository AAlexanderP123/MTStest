WITH 
days AS (
    SELECT TRUNC(TO_DATE('2024', 'YYYY'), 'YYYY') + (ROWNUM - 1) AS dt
    FROM dual
    CONNECT BY ROWNUM <= TRUNC(SYSDATE) - TRUNC(TO_DATE('2024', 'YYYY'), 'YYYY') + 1
),
first_monday AS (
    SELECT MIN(dt) AS first_mon
    FROM days
    WHERE TO_CHAR(dt, 'D') = '2'
),
weeks AS (
    SELECT
        ROWNUM AS dt_n,
        first_mon + (ROWNUM - 1) * 7 AS dt
    FROM first_monday
    CONNECT BY first_mon + (ROWNUM - 1) * 7 <= TRUNC(SYSDATE, 'IW')
)
SELECT
    dt_n, 
    dt, 
    TO_CHAR(dt, 'DD.MM.YYYY') || ' - ' || TO_CHAR(dt + 6, 'DD.MM.YYYY') AS dt_week,
    TO_CHAR(dt, 'Q') AS quart
FROM weeks
WHERE (dt + 6) <= TRUNC(SYSDATE, 'd')
ORDER BY dt;


