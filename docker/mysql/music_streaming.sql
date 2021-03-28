-- Create the database that we'll use to populate data and watch the effect in the binlog
CREATE DATABASE music_streaming;
GRANT ALL PRIVILEGES ON music_streaming.* TO 'mysqluser'@'%';

USE music_streaming;

CREATE TABLE songs (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  author VARCHAR(255) NOT NULL,
  title VARCHAR(255) NOT NULL
);
ALTER TABLE songs AUTO_INCREMENT = 1;

INSERT INTO songs
VALUES (default, "The Beatles", "Yesterday"),
       (default, "The Rolling Stones", "Paint It Black"),
       (default, "The Rolling Stones", "Let It Bleed"),
       (default, "Abba", "Dancing Queen"),
       (default, "Adele", "Rolling in the Deep"),
       (default, "Queen", "I want it all"),
       (default, "Katy Perry", "California Gurls"),
       (default, "Pink Floyd", "High Hopes"),
       (default, "Queen", "Bohemian Rhapsody"),
       (default, "Queen", "I want to break free");

CREATE TABLE users (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  country VARCHAR(255) NOT NULL
);
ALTER TABLE users AUTO_INCREMENT = 1;

INSERT INTO users
VALUES (default, "Harry", "Kane", "Great Britain"),
       (default, "Delle", "Ali", "Great Britain"),
       (default, "Robert", "Lewandowski", "Poland"),
       (default, "Arkadiusz", "Milik", "Poland"),
       (default, "Kylian", "Mbappe", "France"),
       (default, "Hugo", "Lloris", "France"),
       (default, "Manuel", "Neuer", "Germany"),
       (default, "Marco", "Reus", "Germany"),
       (default, "Sergio", "Ramos", "Spain"),
       (default, "Thiago", "Alcantara", "Spain");
