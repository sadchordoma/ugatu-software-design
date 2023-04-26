create table source_files (
    id integer primary key,
    filename varchar(255) NOT NULL,
    processed datetime
);

create table crypto (
    id integer primary key,
    exchange varchar(255),
    asset1 varchar(255),
    asset2 varchar(255),
    price real,
    source_file integer NOT NULL,
    CONSTRAINT fk_source_files
    FOREIGN KEY (source_file)
    REFERENCES source_files(id)
    ON DELETE CASCADE
);
