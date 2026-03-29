select topic,
       logtime,
       payload::json ->> 'manmod'                       "Model",
       payload::json ->> 'type'                         "type",
       to_number(payload::json ->> 'costs', '99999.99') costs,
       payload::json ->> 'comment'                      "comment",
       payload::json ->> 'recordDateTime'               recordDateTime,
       payload::json ->> 'uniqueID'                     uniqueID,
       payload::json ->> 'deleted'                      "deleted",
       payload::json ->> 'id'                           idjson,
       id
from mqtt_logger
where 1 = 1
--and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'deleted' = 'false'
--and payload like '%text%'
-- and payload::json ->> 'comment' like '%oral%'
--and  upper(payload::json->>'type') = 'SONST'
--and payload::json ->> 'costs' = '9'
order by recordDateTime desc
--and to_date(payload::json ->> 'recordDateTime', 'YYYY-MM-DD') = to_date('2025-12-23', 'YYYY-MM-DD')
--order by to_number(payload::json ->> 'costs', '9999999.99') desc
;

/* Kippen */

select topic,
       logtime,
       payload::json ->> 'manmod'                       "Model",
       payload::json ->> 'type'                         "type",
       to_number(payload::json ->> 'costs', '99999.99') costs,
       payload::json ->> 'comment'                      "comment",
       payload::json ->> 'recordDateTime'               recordDateTime,
       payload::json ->> 'uniqueID'                     uniqueID,
       payload::json ->> 'deleted'                      "deleted",
       payload::json ->> 'id'                           idjson,
       id
from mqtt_logger
where 1 = 1
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and payload::json ->> 'deleted' = 'false'
  and topic = 'expanses/clientcosts'
  and lower(payload::json ->> 'comment') like '%kippen%'

select (sum((payload::json ->> 'costs')::DOUBLE PRECISION)) kippencosts
from mqtt_logger
where 1 = 1
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and payload::json ->> 'deleted' = 'false'
  and topic = 'expanses/clientcosts'
  and lower(payload::json ->> 'comment') like '%kippen%'

with min_max_kippen as (select min(payload::json ->> 'recordDateTime')::date kleinstes_datum,
                               max(payload::json ->> 'recordDateTime')::date größtest_datum
                        from mqtt_logger
                        where 1 = 1
                          and payload::json ->> 'manmod' = 'samsung_SM-S928B'
                          and payload::json ->> 'deleted' = 'false'
                          and topic like '%clientcosts%'
                          and lower(payload::json ->> 'comment') like '%kippen%'
)
select extract(YEAR from age(größtest_datum, kleinstes_datum)) * 12 +
                                   extract(MONTH from age(größtest_datum, kleinstes_datum)) + 1 monate_kippen,
                                   größtest_datum - kleinstes_datum                         tage_kippen
                            from min_max_kippen