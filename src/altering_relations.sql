-- Primary key for fact_table
ALTER TABLE marts.fact_table ADD PRIMARY KEY (sale_id);

-- Primary key for customer_dim
ALTER TABLE marts.customer_dim ADD PRIMARY KEY (customer_id);

-- Primary key for product_dim
ALTER TABLE marts.product_dim ADD PRIMARY KEY (product_id);

-- Primary key for date_dim
ALTER TABLE marts.date_dim ADD PRIMARY KEY (date_id);

-- Foreign key constraint on fact_table referencing customer_dim
ALTER TABLE marts.fact_table 
    ADD CONSTRAINT fk_customer_id 
    FOREIGN KEY (customer_id) 
    REFERENCES marts.customer_dim(customer_id);

-- Foreign key constraint on fact_table referencing product_dim
ALTER TABLE marts.fact_table 
    ADD CONSTRAINT fk_product_id 
    FOREIGN KEY (product_id) 
    REFERENCES marts.product_dim(product_id);

-- Foreign key constraint on fact_table referencing date_dim
ALTER TABLE marts.fact_table 
    ADD CONSTRAINT fk_date_id 
    FOREIGN KEY (date_id) 
    REFERENCES marts.date_dim(date_id);