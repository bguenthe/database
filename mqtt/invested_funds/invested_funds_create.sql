CREATE TYPE type AS ENUM ('Aktien', 'Aktien Anlagekonto', 'Tagesgeld', 'Girokonto', 'Otto Genussrecht');

create table invested_funds
(
    id            uuid      default gen_random_uuid() not null,
    creation_time timestamp default now()             not null,
    name          text                                not null,
    type          type                                not null,
    account       text                                not null,
    amount        double precision                    not null,
    interest_rate double precision,
    comment       text
);

comment on table invested_funds is 'Tabelle meines angelegten Geldes';

comment on column invested_funds.creation_time is 'Datum der Anlage des Datensatzes';
comment on column invested_funds.name is 'Name der Anlage';
comment on column invested_funds.type is 'Type der Anlage (lt. enum type)';
comment on column invested_funds.account is 'Kontonummer';
comment on column invested_funds.amount is 'Angelegter Betrag';
comment on column invested_funds.interest_rate is 'Zinssatz (wenn vorhanden)'
comment on column invested_funds.comment is 'Kommentare';

alter table invested_funds
    owner to postgres;