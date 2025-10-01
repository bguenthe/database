select topic,
       logtime,
       payload::json ->> 'manmod'         "Model",
       payload::json ->> 'type'           "type",
       to_number(payload::json ->> 'costs', '99999.99')          costs,
       payload::json ->> 'comment'        "comment",
       payload::json ->> 'recordDateTime' recordDateTime,
       payload::json ->> 'uniqueID'       uniqueID,
       payload::json ->> 'deleted'        "deleted",
       payload::json ->> 'id'             idjson,
       id
from mqtt_logger
where 1 = 1
--and payload::json ->> 'manmod' = 'samsung_SM-S928B'
and topic = 'monthlycosts/clientcosts'
and payload::json ->> 'deleted' = 'false'
--and payload like '%text%'
-- and payload::json ->> 'comment' like '%oral%'
--and  upper(payload::json->>'type') = 'SONST'
--and payload::json ->> 'costs' = '9'
order by to_number(payload::json->>'costs', '9999999.99') desc;

SELECT payload::json ->> 'costs', count(*)
from mqtt_logger where topic = 'monthlycosts/clientcosts'
and payload::json->>'manmod' = 'samsung_SM-S928B'
group by 1
having count(*) > 1;

select sum(to_number(payload::json->>'costs', '9999999.99')) costs
from mqtt_logger
where 1 = 1
and payload::json ->> 'manmod' like 'sam%'
and topic = 'monthlycosts/clientcosts'
and payload::json ->> 'deleted' = 'false'
--and payload::json ->> 'comment' like '%Dell%'
and  payload::json->>'type' = 'rest';
--and payload::json->>'uniqueID' = '4eb3afa2-4d0a-464c-878e-fadb0dc5c18c';

/* Anzahl an Einträgen */
select * from mqtt_logger where topic = 'monthlycosts/clientcosts'
--and payload::json->>'manmod' = 'samsung_SM-S928B'
order by id desc;

select * from mqtt_logger where lower(payload) like '%tex%'

SELECT payload::json ->> 'id' id, payload::json ->> 'uniqueID' uniqueid, payload::json ->> 'recordDateTime' recordDateTime, payload::json ->> 'costs' costs, payload::json ->> 'deleted'        "deleted"
from mqtt_logger where topic = 'monthlycosts/clientcosts'
and payload::json->>'manmod' = 'samsung_SM-S928B'
order by payload::json ->> 'recordDateTime';

SELECT payload::json ->> 'id' id, payload::json ->> 'uniqueID' uniqueid, payload::json ->> 'recordDateTime' recordDateTime, payload::json ->> 'income' costs, payload::json ->> 'deleted'        "deleted"
from mqtt_logger where topic = 'monthlycosts/clientincome'
                   and payload::json->>'manmod' = 'samsung_SM-S928B'
order by payload::json ->> 'recordDateTime';

SELECT payload::json ->> 'uniqueID', payload::json ->> 'recordDateTime', count(*)
from mqtt_logger where topic = 'monthlycosts/clientcosts'
                   and payload::json->>'manmod' = 'samsung_SM-S928B'
group by payload::json ->> 'uniqueID', payload::json ->> 'recordDateTime'
having count(payload::json ->> 'uniqueID') > 1;

SELECT payload::json ->> 'uniqueID', payload::json ->> 'recordDateTime', count(*)
from mqtt_logger where topic = 'monthlycosts/income'
                   and payload::json->>'manmod' = 'samsung_SM-S928B'
group by payload::json ->> 'uniqueID', payload::json ->> 'recordDateTime'
having count(payload::json ->> 'uniqueID') > 1;

SELECT sum((payload::json ->> 'costs')::DOUBLE PRECISION), payload::json ->> 'type'
from mqtt_logger where topic = 'monthlycosts/clientcosts'
and payload::json->>'manmod' = 'samsung_SM-S928B'
group by payload::json ->> 'sonst';

SELECT *, to_number(payload::json ->> 'costs', '99999.99') costs,
       payload::json ->> 'comment'        "comment", to_date(payload::json->>'recordDateTime', 'YYYY-MM-DD')
from mqtt_logger where topic = 'monthlycosts/clientcosts'
                   and payload::json->>'manmod' = 'samsung_SM-S928B'
and payload::json ->> 'type' = 'sonst'
and payload::json ->> 'deleted' = 'false'
    and payload::json ->> 'comment' like 'Bas%'
and to_date(payload::json->>'recordDateTime', 'YYYY-MM') = to_date ('2023-08', 'YYYY-MM')--order by to_number(payload::json ->> 'costs', '99999.99') desc;

delete from mqtt_logger where topic = 'monthlycosts/clientcosts'
                   and payload::json->>'manmod' = 'samsung_SM-S928B'
                   and to_date(payload::json->>'recordDateTime', 'YYYY-MM') = to_date ('2023-08', 'YYYY-MM');

create table mqtt_logger_save as select * from mqtt_logger;

delete from mqtt_logger where id = 530448;

SELECT sum(to_number(payload::json ->> 'costs', '99999.99')) costs
from mqtt_logger where topic = 'monthlycosts/clientcosts'
                   and payload::json->>'manmod' = 'samsung_SM-S928B'
                   and payload::json ->> 'type' = 'sonst'
                   and payload::json ->> 'deleted' = 'false'
                   and to_date(payload::json->>'recordDateTime', 'YYYY-MM') = to_date ('2023-08', 'YYYY-MM');

order by to_number(payload::json ->> 'costs', '99999.99') desc;

select payload::json ->> 'id', payload::json ->> 'recordDateTime', payload::json ->> 'income'
    from mqtt_logger where topic = 'monthlycosts/clientincome'
and payload::json ->> 'manmod' ='samsung_SM-S928B'
order by payload::json ->> 'recordDateTime' desc ;

select payload::json ->> 'id', payload::json ->> 'uniqueID', payload::json ->> 'recordDateTime', payload::json ->> 'income', payload::json ->> 'deleted', *
from mqtt_logger where topic = 'monthlycosts/clientincome'
                   and payload::json ->> 'manmod' ='samsung_SM-S928B'
order by logtime desc;

delete from mqtt_logger where payload::json ->> 'id' in ('1684', '1682', '1681')
and topic = 'monthlycosts/clientincome'
and payload::json ->> 'manmod' ='samsung_SM-S928B';

select sum((payload::json ->> 'income')::DOUBLE PRECISION)
from mqtt_logger where topic = 'monthlycosts/clientincome'
                   and payload::json ->> 'manmod' ='samsung_SM-S928B';

with monthlyincome as (
    select substr(payload::json ->> 'recordDateTime', 1, 7) ym, sum((payload::json ->> 'income')::DOUBLE PRECISION) sum
    from mqtt_logger where topic = 'monthlycosts/clientincome'
    and payload::json ->> 'manmod' ='samsung_SM-S928B'
    and payload::json ->> 'deleted' = 'false'
    group by substr(payload::json ->> 'recordDateTime', 1, 7)
), monthlycosts as (
    select substr(payload::json ->> 'recordDateTime', 1, 7) ym, sum((payload::json ->> 'costs')::DOUBLE PRECISION) sum
    from mqtt_logger where topic = 'monthlycosts/clientcosts'
    and payload::json ->> 'manmod' ='samsung_SM-S928B'
    and payload::json ->> 'deleted' = 'false'
    group by substr(payload::json ->> 'recordDateTime', 1, 7)
)
select monthlycosts.ym, monthlyincome.sum, monthlycosts.sum, (monthlyincome.sum - monthlycosts.sum - 797) from monthlyincome join monthlycosts on monthlyincome.ym = monthlycosts.ym
order by 1;

with monthlyincome as (
    select sum((payload::json ->> 'income')::DOUBLE PRECISION) sum
    from mqtt_logger where topic = 'monthlycosts/clientincome'
                       and payload::json ->> 'manmod' ='samsung_SM-S928B'
                       and payload::json ->> 'deleted' = 'false'
), monthlycosts as (
    select sum((payload::json ->> 'costs')::DOUBLE PRECISION) sum
    from mqtt_logger where topic = 'monthlycosts/clientcosts'
                       and payload::json ->> 'deleted' = 'false'
                       and payload::json ->> 'manmod' ='samsung_SM-S928B'
)
select monthlyincome.sum, monthlycosts.sum, ((monthlyincome.sum - monthlycosts.sum) / 48) - 797 from monthlyincome join monthlycosts on 1=1

-- Income 138160 in 48 Monaten
-- Costs  081561 in 48 Monaten

/* Monatlicher Durchschnitt */
with monthcount as (
    select payload::json ->> 'type', sum(to_number(payload::json->>'costs', '9999999.99')) / count(*)
    from mqtt_logger
    where 1=1
    and payload::json->>'deleted' = 'false'
    and topic = 'monthlycosts/clientcosts'
    and payload::json ->> 'manmod' ='samsung_SM-S928B'
    group by payload::json ->> 'type'
)
select sum(to_number(payload::json->>'costs', '9999999.99')) from mqtt_logger
where 1=1
  --and payload::json->>'deleted' = 'false'
  and topic = 'monthlycosts/clientcosts'
  and payload::json ->> 'manmod' ='samsung_SM-S928B';

select max(to_date(payload::json->>'recordDateTime', 'YYYY-MM-DD')), min(to_date(payload::json->>'recordDateTime', 'YYYY-MM-DD'))
from mqtt_logger
where 1=1
and payload::json->>'deleted' = 'false'
and topic = 'monthlycosts/clientcosts'
and payload::json ->> 'manmod' ='samsung_SM-S928B';

/* Einträge per Type */
select payload::json ->> 'type', count(*)
from mqtt_logger
where 1=1
  and payload::json->>'deleted' = 'false'
  and topic = 'monthlycosts/clientcosts'
  and payload::json ->> 'manmod' ='samsung_SM-S928B'
group by payload::json ->> 'type';

select payload::json ->> 'type', sum(to_number(payload::json->>'costs', '9999999.99')) / count(*)
from mqtt_logger
where 1=1
  and payload::json->>'deleted' = 'false'
  and topic = 'monthlycosts/clientcosts'
  and payload::json ->> 'manmod' ='samsung_SM-S928B'
group by payload::json ->> 'type';

/* Anzahl costs */
select count(*) from mqtt_logger
where 1=1
  and topic = 'monthlycosts/clientcosts'
  and payload::json ->> 'manmod' ='samsung_SM-S928B';

/* Anzahl income */
select count(*) from mqtt_logger
where 1=1
  and topic = 'monthlycosts/clientincome'
  and payload::json ->> 'manmod' ='samsung_SM-S928B';

select logtime,
       payload::json ->> 'manmod'         "Model",
       payload::json ->> 'recordDateTime' recordDateTime,
       payload::json ->> 'income'       income,
       payload::json ->> 'uniqueID'       uniqueID,
       *
from mqtt_logger where 1=1
    and payload::json->>'deleted' = 'false'
    and topic = 'monthlycosts/clientincome'
    and payload::json ->> 'manmod' ='samsung_SM-S928B'
order by payload::json ->> 'recordDateTime';

/* delete all costs */
delete
from mqtt_logger where 1=1
                   and topic = 'monthlycosts/clientcosts'
                   and payload::json ->> 'manmod' ='samsung_SM-S928B';

/* delete all income */
delete
from mqtt_logger where 1=1
                   and topic = 'monthlycosts/clientincome'
                   and payload::json ->> 'manmod' ='samsung_SM-S928B';

select * from mqtt_logger;

SELECT sum((payload::json ->> 'costs')::double precision)
FROM public.mqtt_logger
WHERE payload like '%Kippen%';