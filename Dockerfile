FROM ubuntu

RUN apt-get update && \
    apt-get -y install curl openjdk-7-jdk

#Install Spark 1.6
ENV SPARK_VERSION 1.6.0-bin-hadoop2.6
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-$SPARK_VERSION spark
ENV SPARK_HOME /usr/local/spark
ENV PATH $SPARK_HOME/bin:$PATH

ADD target/scala-2.10/hashtag-battle-assembly-1.0.jar /

ENTRYPOINT ["spark-submit", "--master", "local[2]", "--class", "com.svds.hashtag.Battle", "hashtag-battle-assembly-1.0.jar"]
