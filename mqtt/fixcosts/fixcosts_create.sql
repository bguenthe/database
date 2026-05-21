create table fixcosts
(
    id            uuid                     default gen_random_uuid() not null,
    creation_time timestamp with time zone default now()             not null,
    type          text                                               not null,
    yearly_costs  double precision                                   not null,
    comment       text
);

comment on table fixcosts is 'Tabelle meiner Fixkosten';

comment on column fixcosts.creation_time is 'Datum der Anlage des Datensatzes';

comment on column fixcosts.yearly_costs is 'Jährliche Kosten';

comment on column fixcosts.comment is 'Kommentare zu den Kosten';

alter table fixcosts
    owner to postgres;

