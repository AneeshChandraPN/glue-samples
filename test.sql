select c_customer_id, c_first_name, c_last_name from transform_customers
where start_date >= '[[ START DATE ]]' and end_date <= '[[ END DATE ]]'