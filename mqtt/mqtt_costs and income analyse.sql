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

/* Durchschnittliche, monatliche Ausgaben
   SOLLTE AM Besten am Monatsanfang ausgeührt werden (wegen Monatsabrundung)
   */
--create table assets_and_costs_and_montly_average as (
insert into assets_and_costs_and_montly_average (
with regelrente as (select 2804.25 as regelrente),
     vorgezogene_rente as (select 2329.64 as vorgezogene_rente),
     leistungsrate as (select 508 as leistungsrate),
     fixcosts as (select 337.36 as fixcosts),
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
                 and payload::json ->> 'type' != 'allo'),
     min_max_kippen as
         (select min(payload::json ->> 'recordDateTime')::date kleinstes_datum,
                 max(payload::json ->> 'recordDateTime')::date größtest_datum
          from mqtt_logger
          where 1 = 1
            and payload::json ->> 'manmod' = 'samsung_SM-S928B'
            and payload::json ->> 'deleted' = 'false'
            and topic like '%clientcosts%'
            and lower(payload::json ->> 'comment') like '%kippen%'),
     monate_tage_kippen as (select extract(YEAR from age(größtest_datum, kleinstes_datum)) * 12 +
                                   extract(MONTH from age(größtest_datum, kleinstes_datum)) monate_kippen,
                                   größtest_datum - kleinstes_datum                         tage_kippen
                            from min_max_kippen),
     kippencosts as (select (sum((payload::json ->> 'costs')::DOUBLE PRECISION)) kippencosts
                     from mqtt_logger
                     where 1 = 1
                       and payload::json ->> 'manmod' = 'samsung_SM-S928B'
                       and payload::json ->> 'deleted' = 'false'
                       and topic = 'expanses/clientcosts'
                       and lower(payload::json ->> 'comment') like '%kippen%'),
     monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin as (
    select 200000 / (extract(YEAR from age('2045-09-01'::DATE, now()::DATE)) * 12 + extract(MONTH from age('2045-09-01'::DATE, now()::DATE))) monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin
    )
select now(),
    leistungsrate,
       fixcosts,
       income / monate                                                   "Monatliche Einnahmen",
       costs / monate                                                    "Kosten pro Monat",
       (costs / monate) + fixcosts                                       "Kosten pro Monat (inkl. Fixkosten)",
       (costs / monate) + fixcosts + leistungsrate                       "Kosten pro Monat (inkl. Fixkosten) und Leistungsrate",
       costs / tage                                                      "Kosten pro Tag",
       (income / monate) - ((costs / monate) + fixcosts + leistungsrate) "Monatliches Geld über",
       (income / monate) - ((costs / monate) + fixcosts)                 "Monatliches Geld über mit 65 Jahren (ohne Leistungsrate)",
       (vorgezogene_rente) - ((costs / monate) + fixcosts)               "Monatliches Geld über mit vorgezogener Rente",
       (regelrente) - ((costs / monate) + fixcosts)                      "Monatliches Geld über mit regulärer Rente",
       ((regelrente) - ((costs / monate) + fixcosts)) -
       ((vorgezogene_rente) - ((costs / monate) + fixcosts))             "Differnz Regelrente zu vorgezogener Rente",
        kippencosts / monate_tage_kippen.monate_kippen "Monatliche Kosten für Kippen",
       monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin
from min_max,
     costs,
     monate_tage,
     income,
     fixcosts,
     leistungsrate,
     vorgezogene_rente,
     regelrente, monate_tage_kippen, kippencosts, monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin
);

/* Stand am 28.12.2025 */
insert into MY_TABLE (leistungsrate, fixcosts, Monatliche Einnahmen, Kosten pro Monat,
                      Kosten pro Monat (inkl. Fixkosten), Kosten pro Monat (inkl. Fixkosten) und Leistungsrate,
                      Kosten pro Tag, Monatliches Geld über, Monatliches Geld über mit 65 Jahren (ohne Leistungsrate),
                      Monatliches Geld über mit vorgezogener Rente, Monatliches Geld über mit regulärer Rente,
                      Differnz Regelrente zu vorgezogener Rente)
values (508, 337.36, 3174.9861445783145, 1721.1367108433676, 2058.4967108433675, 2566.4967108433675, 55.955482569525856,
        608.4894337349469, 1116.489433734947, 271.14328915663236, 745.7532891566325, 474.6100000000001);