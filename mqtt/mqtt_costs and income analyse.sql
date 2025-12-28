/* Anzahl an Einträgen */
select count(*)
from mqtt_logger_expanses_with_allo_28_12_2025
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B';

select count(*)
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientincome'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B';

select payload::json ->> 'manmod',
       payload::json ->> 'id',
       payload::json ->> 'type',
       payload::json ->> 'costs',
       payload::json ->> 'recordDateTime',
       payload::json ->> 'uniqueID',
       payload::json ->> 'deleted',
       payload
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
--and payload::json ->> 'type' = 'allo';
order by payload::json ->> 'recordDateTime' desc;

select payload::json ->> 'manmod',
       payload::json ->> 'id',
       payload::json ->> 'income',
       payload::json ->> 'recordDateTime',
       payload::json ->> 'uniqueID',
       payload::json ->> 'deleted',
       payload,
       *
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientincome'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
order by payload::json ->> 'id';

/* sonstige Kosten absteigend nach preis */
select payload::json ->> 'manmod',
       payload::json ->> 'id',
       payload::json ->> 'type',
       payload::json ->> 'comment',
       payload::json ->> 'costs',
       payload::json ->> 'recordDateTime',
       payload::json ->> 'uniqueID',
       payload::json ->> 'deleted',
       payload
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and payload::json ->> 'type' = 'sonst'
  and payload::json ->> 'deleted' = 'false'
order by to_number(payload::json ->> 'costs', '9999999.99') desc;

select sum(to_number(payload::json ->> 'costs', '9999999.99'))
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientcosts'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and payload::json ->> 'deleted' = 'false'
  and payload::json ->> 'type' = 'sonst';

update mqtt_logger
set topic = replace(topic, 'monthlycosts/clientincome', 'expanses/clientincome')
where topic = 'monthlycosts/clientincome';

update mqtt_logger
set topic = replace(topic, 'monthlycosts/clientcosts', 'expanses/clientcosts')
where topic = 'monthlycosts/clientcosts';

create table mqtt_logger_expanses_with_allo_28_12_2025 as
select *
from mqtt_logger;

/* Durchschnittliche, monatliche Ausgaben */
with monthlyincome as (and
    select substr(payload::json ->> 'recordDateTime', 1, 7) ym, sum((payload::json ->> 'income')::DOUBLE PRECISION) sum
    from mqtt_logger where topic like '%clientincome%'
                       and payload::json ->> 'manmod' ='samsung_SM-S928B'
                       and payload::json ->> 'deleted' = 'false'
    group by substr(payload::json ->> 'recordDateTime', 1, 7)),
     monthlycosts as (select substr(payload::json ->> 'recordDateTime', 1, 7)   ym,
                             sum((payload::json ->> 'costs')::DOUBLE PRECISION) sum
                      from mqtt_logger
                      where topic like '%clientcosts%'
                        and payload::json ->> 'manmod' = 'samsung_SM-S928B'
                        and payload::json ->> 'deleted' = 'false'
                      group by substr(payload::json ->> 'recordDateTime', 1, 7))
select monthlycosts.ym, monthlyincome.sum, monthlycosts.sum, (monthlyincome.sum - monthlycosts.sum - 797)
from monthlyincome
         join monthlycosts on monthlyincome.ym = monthlycosts.ym
order by 1;

select payload::json ->> 'recordDateTime' recordDateTime
from mqtt_logger
where 1 = 1
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
  and payload::json ->> 'deleted' = 'false'
  and topic like '%clientcosts%'
order by payload::json ->> 'recordDateTime';

with leistungsrate as (select 508 as leistungsrate),
     fixcosts as (select 327 as fixcosts),
     income as (select (sum((payload::json ->> 'income')::DOUBLE PRECISION)) income
                from mqtt_logger
                where 1 = 1
                  and topic = 'expanses/clientincome'
                  and payload::json ->> 'manmod' = 'samsung_SM-S928B'),
     min_max as
         (select min(payload::json ->> 'recordDateTime')::date kleinstes_datum,
                 max(payload::json ->> 'recordDateTime')::date größtest_datum
          from mqtt_logger
          where 1 = 1
            and payload::json ->> 'manmod' = 'samsung_SM-S928B'
            and payload::json ->> 'deleted' = 'false'
            and topic like '%clientcosts%'),
     monate_tage as (select extract(YEAR from age(größtest_datum, kleinstes_datum)) * 12 +
                            extract(MONTH from age(größtest_datum, kleinstes_datum)) monate,
                            größtest_datum - kleinstes_datum                         tage
                     from min_max),
     costs as (select (sum((payload::json ->> 'costs')::DOUBLE PRECISION)) costs
               from mqtt_logger
               where 1 = 1
                 and payload::json ->> 'manmod' = 'samsung_SM-S928B'
                 and payload::json ->> 'deleted' = 'false'
                 and topic = 'expanses/clientcosts'
                 and payload::json ->> 'type' != 'allo'
               )
select leistungsrate,
       fixcosts,
       income / monate                                                   "Monatliche Einnahmen",
       costs / monate                                                    "Kosten pro Monat",
       (costs / monate) + fixcosts                                       "Kosten pro Monat (inkl. Fixkosten)",
       (costs / monate) + fixcosts + leistungsrate                       "Kosten pro Monat (inkl. Fixkosten) und Leistungsrate",
       costs / tage                                                      "Kosten pro Tag",
       (income / monate) - ((costs / monate) + fixcosts + leistungsrate) "Monatliches Geld über",
       (income / monate) - ((costs / monate) + fixcosts)                 "Monatliches Geld über mit 65 Jahren (ohne Leistungsrate)"
from min_max,
     costs,
     monate_tage,
     income,
     fixcosts,
     leistungsrate;