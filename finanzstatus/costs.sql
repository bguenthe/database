delete from costs;

select count(*) from costs;

insert into costs (type, costs, uniqueid, comment) values ('LBM', 12.0, '42be6a5e-25e3-44f8-88cd-65fee3638ae5', '');

alter table costs alter column recorddatetime type timestamp using recorddatetime::timestamp;

alter table costs alter column recorddatetime set default now();

commit;

create unique index costs_uniqueid_uindex
    on costs (uniqueid);

select * from costs where to_char(recorddatetime, 'YYYY') = '2020' and type = 'sonst';

select * from mqtt_logger;

delete from mqtt_logger;