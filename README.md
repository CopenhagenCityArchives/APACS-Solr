# APACS (Archival Presentation And Crowdsourcing System)
[![Build Status](https://travis-ci.org/CopenhagenCityArchives/APACS-solr.svg?branch=master)](https://travis-ci.org/CopenhagenCityArchives/APACS-solr)
Copenhagen City Archives' configurable backend system used to present and crowdsource digitized collections.


Solr server and indexing scripts
Known issues: Schema is not updated when the Docker image is built.


It can be copied manually on the docker-machine:
``
cp /opt/solr/server/solr/configsets/apacs-config/conf/managed-schema /var/solr/data/apacs_core/conf
``


# Statistics

Indsættes hvis banana-int coren nulstilles, eksempelvis ved rebuild af Solr.
Husk at lave schemaet om, så dashboard-feltet er string og ikke multivalued (må ikke være et array), EFTER at dokumentet er indsat.
Indsættes her: https://aws.kbhkilder.dk/solr/#/banana-int
Statistikken kan ses her: http://kbhkilder.dk/stats/#/dashboard/solr/Brugerstats
Credentials: kbharkiv og samme kodeord som APACS-databasen.