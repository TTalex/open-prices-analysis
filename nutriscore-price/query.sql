COPY (
    with study_categories as (
        select row_number() OVER () as ROWID, * from read_csv('cats.csv')
    ),
    unest_category as (
        select nutriscore_grade, price, unnest(categories_tags) as category, cast(product_quantity as float) as product_quantity
        from '/mnt/f/prices_food_merge.parquet'
        where categories_tags is not null and location_osm_address_country_code = 'FR' and currency = 'EUR' and product_quantity not null and price_is_discounted is False
    ),
    avgs as (
        select ROWID, StudyCategory, Subcategory, category, nutriscore_grade, median(price), median(price/product_quantity)*1000 as price_per_kg, count(*) as obs
        from study_categories
        left join unest_category on taxo = category
        where taxo not null
        group by ROWID, StudyCategory, Subcategory, category, nutriscore_grade
        having obs > 10
    )
    SELECT
        StudyCategory, Subcategory, category,
        first(obs) FILTER (WHERE nutriscore_grade = 'a') AS a_obs,
        first(price_per_kg) FILTER (WHERE nutriscore_grade = 'a') AS a_price,
        first(obs) FILTER (WHERE nutriscore_grade = 'b') AS b_obs,
        first(price_per_kg) FILTER (WHERE nutriscore_grade = 'b') AS b_price,
        first(obs) FILTER (WHERE nutriscore_grade = 'c') AS c_obs,
        first(price_per_kg) FILTER (WHERE nutriscore_grade = 'c') AS c_price,
        first(obs) FILTER (WHERE nutriscore_grade = 'd') AS d_obs,
        first(price_per_kg) FILTER (WHERE nutriscore_grade = 'd') AS d_price,
        first(obs) FILTER (WHERE nutriscore_grade = 'e') AS e_obs,
        first(price_per_kg) FILTER (WHERE nutriscore_grade = 'e') AS e_price,
        first(obs) FILTER (WHERE nutriscore_grade > 'e' OR nutriscore_grade is null) AS other_obs,
        first(price_per_kg) FILTER (WHERE nutriscore_grade > 'e' OR nutriscore_grade is null) AS other_price
    FROM avgs
    GROUP BY StudyCategory, Subcategory, category
    ORDER BY ANY_VALUE(ROWID)
) TO 'res.csv' (HEADER, DELIMITER ',');