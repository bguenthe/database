drop view if exists costs_view;

create or replace view costs_view as
select id as id_db,
       topic as topic,
       mqtt_logger.logtime as logtime,
       payload::json ->> 'manmod' as manmod,
       payload::json ->> 'id' as id,
       payload::json ->> 'type' as type,
       payload::json ->> 'comment' as comment,
       (payload::json ->> 'costs')::DOUBLE PRECISION as costs,
       payload::json ->> 'recordDateTime' as recordDateTime,
       payload::json ->> 'uniqueID' as uniqueID,
       payload::json ->> 'deleted' as deleted
from mqtt_logger
where topic = 'expanses/clientcosts'
  and payload::json ->> 'deleted' = 'false'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B';

drop view if exists income_view;
create or replace view income_view as
select id as id_db,
       topic as topic,
       mqtt_logger.logtime as logtime,
       payload::json ->> 'manmod' as manmod,
       payload::json ->> 'id' as id,
       (payload::json ->> 'income')::DOUBLE PRECISION as income,
       payload::json ->> 'recordDateTime' as recordDateTime,
       payload::json ->> 'uniqueID' as uniqueID,
       payload::json ->> 'deleted' as deleted
from mqtt_logger
where topic = 'expanses/clientincome'
  and payload::json ->> 'deleted' = 'false'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B';

select * from income_view order by recordDateTime desc;