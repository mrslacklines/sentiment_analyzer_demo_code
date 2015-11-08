#!/bin/sh


rethinkdb-import -f cities15000.txt -c 172.17.0.30 --format csv --delimiter '\t' --table geonames.geonames --no-header --custom-header geonameid,name,asciiname,alternatenames,latitude,longitude,feature_class,feature_code,country_code,cc2,admin1_code,admin2_code,admin3_code,admin4_code,population,elevation,dem,timezone,modification_date

