select *, date_trunc('day', logtime) from mqtt_logger where topic in ('monthlycosts/costs/new', 'monthlycosts/costs/delete') order by logtime desc;

select logtime, id, payload::json->>'id' idjson, payload::json->>'costs' costs, payload::json->>'comment' "comment", payload::json->>'recordDateTime' recordDateTime,
       payload::json->>'uniquedID' uniqueid, payload::json->>'type' "type", payload::json->>'deleted' "deleted"
from mqtt_logger
where 1=1
and topic in ('value/costs/new', 'monthlycosts/costs/delete')
order by id, idjson;


select 'database.costsDao().add(Costs("' || cast(payload::json->>'type' as varchar) || '",' || 'LocalDateTime.ofInstant(Instant.ofEpochSecond('
      || extract('epoch' from cast(payload::json->>'recordDateTime' as date )) || '), ZoneId.systemDefault()), ' || cast(payload::json->>'costs' as varchar) || ',"'
      || cast(payload::json->>'comment' as varchar) || '"))'
       , payload::json->>'comment' "comment", payload::json->>'recordDateTime', payload::json->>'recordDateTime' recordDateTime,
       payload::json->>'uniquedID' uniqueid, payload::json->>'type' "type", payload::json->>'deleted' "deleted"
from mqtt_logger
where 1=1
  and topic in ('monthlycosts/costs/new', 'monthlycosts/costs/delete')
  --and date_trunc('day', logtime) = '2018-12-03' and id <= 484203;

--database.costsDao().add(Costs("test", LocalDateTime.ofInstant(Instant.ofEpochSecond(1), ZoneId.systemDefault()), 10.0, ""))

--database.costsDao().add(Costs("lbm",LocalDateTime.ofInstant(Instant.ofEpochSecond(1537056000), ZoneId.systemDefault()), 209.89,""))

select * from mqtt_logger;

select cast(payload::json->>'id' as int), payload::json->>'type', cast(payload::json->>'recordDateTime' as date), payload::json->>'costs'
from mqtt_logger
where 1=1
  and topic in ('monthlycosts/costs/new')
order by id;

select to_char(cast(payload::json->>'recordDateTime' as date), 'YYYY-MM') datum, payload::json->>'type' as type, sum(cast(payload::json->>'costs' as real)) kosten
from mqtt_logger
where 1=1
  and topic in ('monthlycosts/costs/new')
group by datum, type
order by datum, type;

select * from mqtt_logger
where 1=1
--and payload::json->>'costs' = '13'
order by logtime desc

delete from mqtt_logger;

select *
from mqtt_logger
where 1=1
  and topic in ('monthlycosts/costs/new', 'monthlycosts/costs/delete')
  and payload::json->>'type' = 'sonst'
  and to_char(cast(payload::json->>'recordDateTime' as date), 'YYYY-MM') = '2018-10';

