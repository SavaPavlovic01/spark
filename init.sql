drop table if exists Day;

create table Day (
    datum varchar(64) not null,
    Cnt integer not null,
    min_popularity integer not null,
    max_popularity integer not null,
    average_popularity integer not null,
    genres varchar(100) not null,
    album_types varchar(50) not null
);

insert into Day(datum, Cnt, min_popularity, max_popularity, average_popularity, genres, album_types) values ("neki datum", 5, 5, 5, 5, "genres", "types");