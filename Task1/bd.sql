CREATE DATABASE IF not EXISTS Campus;

CREATE TABLE if not exists Campus.rooms(
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE if not exists Campus.students (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birthday DATE,
    room INT,
    sex CHAR(1),
    FOREIGN KEY (room) REFERENCES rooms(id)
);


CREATE INDEX idx_rooms ON Campus.students(room);
CREATE INDEX idx_birthday ON Campus.students(birthday);

