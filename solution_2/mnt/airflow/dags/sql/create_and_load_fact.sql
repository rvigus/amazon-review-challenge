drop table fact_reviews;

create table if not exists fact_reviews (
	reviewer_id VARCHAR(256),
	asin VARCHAR(256),
	reviewername VARCHAR(256),
	helpful VARCHAR(256),
	reviewtext VARCHAR(65335),
	overall INT,
	summary VARCHAR(10000),
	reviewtime timestamp,
	link_category VARCHAR(256)
);


insert into fact_reviews (
	select
	reviewerid as reviewer_id,
	asin as asin,
	reviewername as reviewer_name,
	helpful as helpful,
	reviewtext as reviewtext,
	cast(overall as int) as overall,
	summary as summary,
	to_timestamp(unixreviewtime) as reviewtime,
	link_category as link_category
	from staging_review sr
);

ALTER TABLE fact_reviews ADD COLUMN id SERIAL PRIMARY KEY;