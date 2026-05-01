Select * from fixcosts_view order by type;

select sum(yearly_costs) / 12 as monatlich, sum(yearly_costs) as jährlich from fixcosts_view;

alter table fixcosts
    alter column creation_time type timestamp with time zone using creation_time::timestamp with time zone;

show timezone;

drop database fixcosts