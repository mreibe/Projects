SELECT 
    p.product_name,
    c.customer_name,
    c.customer_email,
    s.sales_amount
FROM sales_fact s
JOIN customer_dimension c ON s.customer_id = c.customer_id
JOIN product_dimension p ON s.product_id = p.product_id;