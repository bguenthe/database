select * from mqtt_logger order by logtime desc;

delete from mqtt_logger where id in (480726,480722,480723);