FROM flink:1.12.1-scala_2.11-java8

# Hadoop dependencies
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
  tar xzf hadoop-3.2.1.tar.gz && \
  mv hadoop-3.2.1 /opt/hadoop
ENV PATH /opt/hadoop/bin:$PATH
ENV HADOOP_CLASSPATH "/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/lib/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*"

# Flink S3 setup
RUN mkdir /opt/flink/plugins/s3-fs-hadoop
RUN cp /opt/flink/opt/flink-s3-fs-hadoop-1.12.1.jar /opt/flink/plugins/s3-fs-hadoop/

# Flink S3 config
RUN echo 's3.endpoint: http://minio:9000' >> /opt/flink/conf/flink-conf.yaml
RUN echo 's3.path.style.access: true' >> /opt/flink/conf/flink-conf.yaml
RUN echo 's3.access-key: minio' >> /opt/flink/conf/flink-conf.yaml
RUN echo 's3.secret-key: minio123' >> /opt/flink/conf/flink-conf.yaml

ENTRYPOINT ["/docker-entrypoint.sh"]