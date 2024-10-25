SELECT transactions.id_client, COUNT(*)/COUNT(DISTINCT transaction_date/100), clients.limit_sum
FROM transactions
JOIN clients
ON (transactions.id_client = clients.id_client)
GROUP BY (transactions.id_client, clients.id_client)
HAVING (COUNT(*)/COUNT(DISTINCT transaction_date/100) > 5) AND (clients.limit_sum < 50000);

