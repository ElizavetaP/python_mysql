create database meteo;
use meteo;
create table prognoz3 (date DATE, temperature VARCHAR(15), pressure VARCHAR(10), nebulosity VARCHAR(15), humidity VARCHAR(10), station VARCHAR(10));
CREATE USER 'meteouser'@'localhost' IDENTIFIED BY 'meteo';
GRANT ALL PRIVILEGES ON meteo.prognoz3 TO 'meteouser'@'localhost';

