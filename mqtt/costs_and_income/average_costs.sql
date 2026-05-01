select * from costs_view order by recorddatetime desc;

/* Monatliche Kosten bei Type */
select type, sum(costs)
from costs_view
         group by type;

SELECT type,
    EXTRACT(YEAR FROM recorddatetime::timestamp) AS jahr,
    EXTRACT(MONTH FROM recorddatetime::timestamp) AS monat,
    sum(costs) AS costs
FROM costs_view
where type = 'beauty'
GROUP BY type, jahr, monat
ORDER BY jahr, monat ;

select * from costs_view where type = 'beauty' order by costs desc;
