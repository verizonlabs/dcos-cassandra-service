cat >Dockerfile <<EOL
FROM nginx:alpine
RUN wget http://downloads.mesosphere.com/cassandra/assets/1.0.12-2.2.5/server-jre-8u74-linux-x64.tar.gz -O /usr/share/nginx/html/server-jre-8u74-linux-x64.tar.gz
COPY ./cassandra-bin-tmp/apache-cassandra-3.0.8-bin-dcos.tar.gz /usr/share/nginx/html/
COPY ./cassandra-executor/build/distributions/executor.zip /usr/share/nginx/html/
COPY ./cassandra-scheduler/build/distributions/scheduler.zip /usr/share/nginx/html/
EOL

docker build --rm -t maikai-prov.verizon.com:9000/montana/cassandra-nginx:$(git rev-parse --short HEAD) .
docker login -u montana -p password -e . maikai-prov.verizon.com:9000
docker push maikai-prov.verizon.com:9000/montana/cassandra-nginx:$(git rev-parse --short HEAD)

