select * from mqtt.public.costs_view
where 1=1
and comment like '%Kippen%';

select * from mqtt.public.costs_view
where 1=1
--and comment like '%Kippen%';
order by costs_view.recorddatetime desc;
