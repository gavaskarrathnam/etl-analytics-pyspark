# etl-analytics-pyspark
Simple ETL processing and analysing data with PySpark (Apache Spark), Python, MySQL


## database and tables

```
CREATE DATABASE IF NOT EXISTS autos;

USE autos;

DROP TABLE IF EXISTS `cars`;

CREATE TABLE cars (
	name VARCHAR(255) NOT NULL, 
	price int(11) NOT NULL, 
	abtest VARCHAR(255) NOT NULL, 
	vehicleType VARCHAR(255), 
	yearOfRegistration VARCHAR(4) NOT NULL, 
	gearbox VARCHAR(255), 
	powerPS int(11) NOT NULL, 
	model VARCHAR(255), 
	kilometer int(11), 
	monthOfRegistration VARCHAR(255) NOT NULL, 
	fuelType VARCHAR(255), 
	brand VARCHAR(255) NOT NULL, 
	notRepairedDamage VARCHAR(255), 
	dateCreated DATE NOT NULL, 
	postalCode VARCHAR(255) NOT NULL	
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


```
