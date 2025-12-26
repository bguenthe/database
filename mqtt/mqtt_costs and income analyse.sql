/* Anzahl an Einträgen */
select count(*)
from mqtt_logger
where 1 = 1
  and topic = 'monthlycosts/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'

select count(*)
from mqtt_logger
where 1 = 1
  and topic = 'monthlycosts/clientincome'
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
  and topic = 'monthlycosts/clientcosts'
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
  and topic = 'monthlycosts/clientincome'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
order by payload::json ->> 'id';

update mqtt_logger set topic = replace(topic, 'monthlycosts/clientincome', 'expanses/clientincome')
where topic = 'monthlycosts/clientincome';

update mqtt_logger set topic = replace(topic, 'monthlycosts/clientcosts', 'expanses/clientcosts')
where topic = 'monthlycosts/clientcosts';
