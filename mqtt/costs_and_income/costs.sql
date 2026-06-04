select * from mqtt.public.costs_view
where 1=1
and comment like '%Kippen%';

select * from mqtt.public.income_view
where 1=1
--and comment like '%Kippen%';
order by recorddatetime desc;

select * from mqtt_logger where topic = 'expanses/clientcosts';

select * from mqtt_logger where topic = 'expanses/clientincome';

select count(*) from mqtt_logger where topic = 'expanses/clientcosts';
select count(*) from mqtt_logger_save_2026 where topic = 'expanses/clientcosts';

select count(*) from mqtt_logger where topic = 'expanses/clientincome';

select count(*) from mqtt_logger_save_2026 where topic = 'expanses/clientincome';

delete from mqtt_logger where id = 1187;