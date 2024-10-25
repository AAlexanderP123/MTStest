SELECT transactions.id_client, SUM(transaction_sum), clients.limit_sum, SUM(transaction_sum)/clients.limit_sum AS per
FROM transactions
JOIN clients
ON transactions.id_client = clients.id_client
GROUP BY (transactions.id_client,clients.limit_sum)
HAVING SUM(transaction_sum)/clients.limit_sum > 0.8
ORDER BY (transactions.id_client);


