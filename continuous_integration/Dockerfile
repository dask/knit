FROM sequenceiq/hadoop-docker:2.7.1

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN curl -L https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/anaconda
RUN rm /tmp/miniconda.sh
ENV PATH /usr/local/hadoop-2.7.1/bin:/opt/anaconda/bin:$PATH
RUN conda install -y -q pip coverage pytest requests apache-maven lxml -c anaconda-cluster
RUN conda create -y -q -n py26 coverage python=2.6 pytest requests apache-maven lxml -c anaconda-cluster
RUN conda create -y -q -n py3 coverage python=3 pytest requests apache-maven lxml -c anaconda-cluster
RUN /opt/anaconda/bin/pip install coveralls
RUN /opt/anaconda/envs/py26/bin/pip install coveralls
RUN /opt/anaconda/envs/py3/bin/pip install coveralls

ENV HADOOP_PREFIX=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
ENV JAVA_HOME /usr/java/default

ADD docker-files/start.sh /tmp/start.sh
ADD docker-files/*.xml $HADOOP_CONF_DIR/

EXPOSE 8020
EXPOSE 8088
EXPOSE 50070

CMD ["bash", "/tmp/start.sh"]

