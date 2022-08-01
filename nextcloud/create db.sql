create user nextcloud with password '%dudelsack48%';

create database nextcloud with owner nextcloud;

grant all privileges on database nextcloud to nextcloud;

drop database nextcloud;

drop user nextcloud;

SELECT datname FROM pg_database;


