with min_max as
         (select min(payload::json ->> 'recordDateTime')::date kleinstes_datum,
                 max(payload::json ->> 'recordDateTime')::date größtes_datum
          from mqtt_logger
          where 1 = 1
            and payload::json ->> 'manmod' = 'samsung_SM-S928B'
            and payload::json ->> 'deleted' = 'false'
            and topic like '%clientcosts%'),
     monate_tage as (select extract(YEAR from age(größtes_datum, kleinstes_datum)) * 12 +
                            extract(MONTH from age(größtes_datum, kleinstes_datum)) monate,
                            größtes_datum - kleinstes_datum                         tage
                     from min_max),
    costs_per_cat as (select payload::json ->> 'type' type, sum((payload::json ->> 'costs')::DOUBLE PRECISION) costs
                     from mqtt_logger
                     where 1 = 1
                       and payload::json ->> 'manmod' = 'samsung_SM-S928B'
                       and payload::json ->> 'deleted' = 'false'
                       and topic like '%clientcosts%'
                     group by payload::json ->> 'type'
                     )
select costs_per_cat.type, monate, costs, costs / monate from monate_tage, costs_per_cat;

select payload::json ->> 'comment' "Comment",
       (payload::json ->> 'costs')::DOUBLE PRECISION "Costs"
from mqtt_logger
where 1 = 1
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and payload::json ->> 'deleted' = 'false'
  and topic like '%clientcosts%'
  and payload::json ->> 'type' = 'sonst'
order by 2 desc;