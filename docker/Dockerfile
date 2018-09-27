FROM gographite/go-graphite

EXPOSE 80 2003 2004

# install bucky
COPY bucky /usr/local/bin

# install and launch buckyd
COPY buckyd /usr/local/bin/buckyd
# COPY buckyd.service /etc/systemd/system/buckyd.service
ADD run /etc/service/buckyd/run
