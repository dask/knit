FROM ubuntu:trusty

# anaconda
RUN apt-get update && apt-get install -y -q curl bzip2 zip git
RUN curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/anaconda
RUN rm /tmp/miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
RUN conda install -y -q pip coverage pytest requests apache-maven lxml -c anaconda-cluster
RUN conda create -y -q -n py26 coverage python=2.6 pytest requests apache-maven lxml -c anaconda-cluster
RUN conda create -y -q -n py3 coverage python=3 pytest requests apache-maven lxml -c anaconda-cluster
RUN /opt/anaconda/bin/pip install coveralls
RUN /opt/anaconda/envs/py26/bin/pip install coveralls
RUN /opt/anaconda/envs/py3/bin/pip install coveralls

# Cloudera repositories
RUN curl -s http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key | apt-key add -
RUN echo 'deb [arch=amd64] http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' > /etc/apt/sources.list.d/cloudera.list
RUN echo 'deb-src http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' >> /etc/apt/sources.list.d/cloudera.list
ADD docker-files/cloudera.pref /etc/apt/preferences.d/cloudera.pref

# Install CDH5 in a single node: Pseudo Distributed
ADD docker-files/cdh5-install.sh /tmp/cdh5-install.sh
RUN bash /tmp/cdh5-install.sh

CMD ["ls"]
ADD docker-files/start.sh /tmp/start.sh
