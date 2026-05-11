FROM ubuntu

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt update
RUN apt install python3.8 pip -y
RUN apt-get install python3.8-distutils --reinstall -y
RUN apt-get install curl tar net-tools nano htop wget openjdk-11-jdk -y

RUN useradd -ms /bin/bash ubuntu
USER ubuntu

RUN python3.8 -m pip install pandas elasticsearch==5.3.0 scipy joblib tqdm nltk

WORKDIR /home/ubuntu
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.17.8-linux-x86_64.tar.gz
RUN tar -xf elasticsearch-7.17.8-linux-x86_64.tar.gz
RUN rm elasticsearch-7.17.8-linux-x86_64.tar.gz

COPY ./ ./

ENTRYPOINT ['./elasticsearch-7.17.8/bin/elasticsearch']
