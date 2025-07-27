-- PRODUTOS MAIS VENDIDOS

WITH VENDAS AS (

SELECT 
	order_id, 
	order_item_id, 
	itens.product_id,
	product_category_name,
	seller_id, 
	shipping_limit_date, 
	price, 
	freight_value,
	price + freight_value as total_value
FROM public."Tb__Itens_Ordens" as itens
INNER JOIN public."Tb_Produtos" as prod
ON itens.product_id = prod.product_id
)

SELECT
	product_category_name,
	COUNT(*) AS TOTAL_VENDAS,
	SUM(total_value) AS VALOR_TOTAL
FROM VENDAS
GROUP BY
	product_category_name
ORDER BY
	2 DESC