/* Anzahl an Einträgen */
select count(*)
from mqtt_logger_expanses_with_allo_28_12_2025
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B';

select count(*)
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientincome'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B';

select payload::json ->> 'manmod',
       payload::json ->> 'id',
       payload::json ->> 'type',
       payload::json ->> 'costs',
       payload::json ->> 'recordDateTime',
       payload::json ->> 'uniqueID',
       payload::json ->> 'deleted',
       payload
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  --and payload::json ->> 'type' = 'allo';
order by payload::json ->> 'recordDateTime' desc;

select payload::json ->> 'manmod',
       payload::json ->> 'id',
       payload::json ->> 'income',
       payload::json ->> 'recordDateTime',
       payload::json ->> 'uniqueID',
       payload::json ->> 'deleted',
       payload,*
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientincome'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
order by payload::json ->> 'id';

/* sonstige Kosten absteigend nach preis */
select payload::json ->> 'manmod',
       payload::json ->> 'id',
       payload::json ->> 'type',
       payload::json ->> 'comment',
       payload::json ->> 'costs',
       payload::json ->> 'recordDateTime',
       payload::json ->> 'uniqueID',
       payload::json ->> 'deleted',
       payload
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
and payload::json ->> 'type' = 'sonst'
and payload::json ->> 'deleted' = 'false'
order by to_number(payload::json->>'costs', '9999999.99') desc;

select sum(to_number(payload::json->>'costs', '9999999.99'))
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and payload::json ->> 'deleted' = 'false'
and payload::json ->> 'type' = 'sonst';

update mqtt_logger set topic = replace(topic, 'monthlycosts/clientincome', 'expanses/clientincome')
where topic = 'monthlycosts/clientincome';

update mqtt_logger set topic = replace(topic, 'monthlycosts/clientcosts', 'expanses/clientcosts')
where topic = 'monthlycosts/clientcosts';

create table mqtt_logger_expanses_with_allo_28_12_2025 as select * from mqtt_logger;