SELECT COUNT(*), SUM(transaction_sum), AVG(transaction_sum), 
MIN(transaction_date*1000000 + transaction_time) / 1000000 AS first_date, 
MIN(transaction_date*1000000 + transaction_time) % 1000000 AS first_time
FROM transactions
GROUP BY (id_client)
ORDER BY id_client;
