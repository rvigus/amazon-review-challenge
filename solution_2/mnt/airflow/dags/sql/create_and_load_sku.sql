drop table dim_sku;

create table if not exists dim_sku (
	asin VARCHAR(256),
	title VARCHAR(10000),
	price DOUBLE precision,
	brand VARCHAR(1000)
);

insert into dim_sku (
	select asin, title, price, brand
	from staging_sku
);