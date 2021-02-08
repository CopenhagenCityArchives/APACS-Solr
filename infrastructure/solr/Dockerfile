FROM library/solr:8.7

# Disable config edits
#RUN "set SOLR_OPTS=%SOLR_OPTS% -Ddisable.configEdit=true"

# Copy web config
COPY web.xml /opt/solr/server/solr-webapp/webapp/WEB-INF/web.xml

# Copy apacs-core config
COPY apacs-config /opt/solr/server/solr/configsets/apacs-config

# Copy security settings to SOLR_HOME
COPY --chown=solr:solr security.json /var/solr/data/security.json

# Copy Banana dashboard data
COPY banana-int /var/solr/banana-int

ENTRYPOINT ["bash", "-c", "precreate-core banana-int; precreate-core apacs_core /opt/solr/server/solr/configsets/apacs-config; exec solr start -f"]