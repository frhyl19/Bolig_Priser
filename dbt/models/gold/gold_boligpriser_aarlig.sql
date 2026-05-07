with silver as (
    select * from {{ ref('silver_boligpriser') }}
)

select
    aar,
    ejendomskategori,
    round(avg(salg_antal), 0)       as gns_salg_antal,
    round(avg(prisindeks), 1)       as gns_prisindeks
from silver
group by aar, ejendomskategori
order by aar, ejendomskategori
