FROM httpd:2.4
MAINTAINER Tim Hansen timothy.hansen@one.verizon.com

ADD cassandra* /usr/local/apache2/htdocs/
ADD cassandra-executor.zip /usr/local/apache2/htdocs/
COPY server-jre-8u74-linux-x64.tar.gz /usr/local/apache2/htdocs/
ADD apache* /usr/local/apache2/htdocs/

ENTRYPOINT ["/usr/local/apache2/bin/httpd"]
CMD ["-D", "FOREGROUND"]
