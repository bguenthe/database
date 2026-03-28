Select * from fixcosts order by type;

select sum(yearly_costs) / 12 as monatlich, sum(yearly_costs) as jährlich from fixcosts_current_view;

select now() AT TIME ZONE 'CET';

alter table fixcosts
    alter column creation_time type timestamp with time zone using creation_time::timestamp with time zone;

show timezone;

drop database fixcosts