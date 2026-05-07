with source as (
    select * from {{ source('raw', 'raw_boligpriser') }}
)

select
    "SALG"                                    as salg_type,
    "EJKAT20"                                 as ejendomskategori,
    "TID"::integer                            as aar,
    replace("INDHOLD", ',', '.')::numeric     as vaerdi
from source
