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
--insert into assets_and_costs_and_montly_average (;

insert into assets_and_costs_and_montly_average_new_version (
with regelrente as (select 2804.25 as regelrente),
     vorgezogene_rente as (select 2329.64 as vorgezogene_rente),
     leistungsrate as (select 508 as leistungsrate),
     fixcosts as (select 343.31 as fixcosts),
     income as (select (sum((payload::json ->> 'income')::DOUBLE PRECISION)) income
                from mqtt_logger
                where 1 = 1
                  and topic = 'expanses/clientincome'
                  and payload::json ->> 'deleted' = 'false'
                  and payload::json ->> 'manmod' = 'samsung_SM-S928B'),
     income_last_year as (select (sum((payload::json ->> 'income')::DOUBLE PRECISION)) income_last_year
                          from mqtt_logger
                          where 1 = 1
                            and topic = 'expanses/clientincome'
                            and payload::json ->> 'deleted' = 'false'
                            and payload::json ->> 'manmod' = 'samsung_SM-S928B'
                            and (payload::json ->> 'recordDateTime')::date >=
                                date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
                            and (payload::json ->> 'recordDateTime')::date < date_trunc('year', CURRENT_DATE)),
     min_max as
         (select min(payload::json ->> 'recordDateTime')::date kleinstes_datum,
                 max(payload::json ->> 'recordDateTime')::date größtes_datum
          from mqtt_logger
          where 1 = 1
            and payload::json ->> 'manmod' = 'samsung_SM-S928B'
            and payload::json ->> 'deleted' = 'false'
            and topic like '%clientcosts%'),
     monate_tage as (select extract(YEAR from age(größtes_datum, kleinstes_datum)) * 12 +
                            extract(MONTH from age(größtes_datum, kleinstes_datum)) monate,
                            größtes_datum - kleinstes_datum                         tage
                     from min_max),
     costs_ohne_allo as (select (sum((payload::json ->> 'costs')::DOUBLE PRECISION)) costs_ohne_allo
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
     monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin
         as (select 200000 / (extract(YEAR from age('2045-09-01'::DATE, now()::DATE)) * 12 +
                              extract(MONTH from age('2045-09-01'::DATE, now()::DATE))) monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin)
select now(),
       leistungsrate,
       fixcosts,
       income / monate                                                   "Monatliche Einnahmen",
       income_last_year / 12                                             "Monatliche Einnahmen der letzen 12 Monate",
       costs_ohne_allo / monate                                          "Kosten pro Monat",
       (costs_ohne_allo / monate) + fixcosts                             "Kosten pro Monat (inkl. Fixkosten)",
       (costs_ohne_allo / monate) + fixcosts + leistungsrate             "Kosten pro Monat (inkl. Fixkosten und Leistungsrate)",
       kippencosts / monate_tage_kippen.tage_kippen * 30                    "Monatliche Kosten für Kippen",
       (income_last_year / 12) -
       ((costs_ohne_allo / monate) + fixcosts + leistungsrate)           "Monatliches Geld über (Fixkosten und Leistungsrate",
       (income_last_year / 12) - ((costs_ohne_allo / monate) + fixcosts) "Monatliches Geld über mit 65 Jahren (ohne Leistungsrate)",
       (vorgezogene_rente) - ((costs_ohne_allo / monate) + fixcosts)     "Monatliches Geld über mit vorgezogener Rente",
       (regelrente) - ((costs_ohne_allo / monate) + fixcosts)            "Monatliches Geld über mit regulärer Rente",
       (regelrente) - ((costs_ohne_allo / monate) + fixcosts) +
       (kippencosts / monate_tage_kippen.tage_kippen * 30) "Monatliches Geld über mit regulärer Rente (und Nichtraucher)",
       ((regelrente) - ((costs_ohne_allo / monate) + fixcosts)) -
       ((vorgezogene_rente) - ((costs_ohne_allo / monate) + fixcosts))   "Differnz Regelrente zu vorgezogener Rente",
       monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin
from min_max,
     costs_ohne_allo,
     monate_tage,
     income,
     income_last_year,
     fixcosts,
     leistungsrate,
     vorgezogene_rente,
     regelrente,
     monate_tage_kippen,
     kippencosts,
     monatliches_geld_durch_aktuelles_vermögen_bis_ich_85_bin);

select (payload::json ->> 'recordDateTime')::date
from mqtt_logger
where 1 = 1
  and topic = 'expanses/clientincome'
  and payload::json ->> 'deleted' = 'false'
  and payload::json ->> 'manmod' = 'samsung_SM-S928B'
--  and (payload::json ->> 'recordDateTime')::date >= '2025-01-01'
--  and (payload::json ->> 'recordDateTime')::date <= '2025-12-31'
  and (payload::json ->> 'recordDateTime')::date >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
  and (payload::json ->> 'recordDateTime')::date < date_trunc('year', CURRENT_DATE);
