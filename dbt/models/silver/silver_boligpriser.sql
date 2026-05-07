with bronze as (
    select * from {{ ref('bronze_boligpriser') }}
),

pivoted as (
    select
        ejendomskategori,
        aar,
        max(case when salg_type = 'Samlet salg (antal)'            then vaerdi end) as salg_antal,
        max(case when salg_type = 'Salg ved prisberegning (antal)' then vaerdi end) as salg_prisberegning_antal,
        max(case when salg_type = 'Prisindeks, almindelig fri handel' then vaerdi end) as prisindeks
    from bronze
    where aar is not null
    group by ejendomskategori, aar
)

select *
from pivoted
where salg_antal is not null
order by ejendomskategori, aar
