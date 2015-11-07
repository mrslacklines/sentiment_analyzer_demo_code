#!/bin/sh


rethinkdb-import --force -f cities15000.txt -c rethinkdb --format csv --delimiter \t --table geonames.geonames --no-header --custom-header geonameid,name,asciiname,alternatenames,latitude,longitude,featureclass,feature_code,country_code,cc2,admin1_code,admin2_code,admin3_code,admin4_code,population,elevation,gtopo30,timezone,modification_date
