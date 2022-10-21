FROM openjdk:8-jdk-stretch
MAINTAINER dev <dev@linktime.cloud>

WORKDIR /datax

#RUN touch 1
#COPY target/datax-*/datax .

ARG SOURCES_LIST="sources.list"
COPY docker/${SOURCES_LIST} /etc/apt/sources.list

ENV USER root
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get clean && \
    apt-get update --fix-missing &&  \
    apt-get install --fix-missing -y bash \
    vim locales \
    wget sudo  && \
    locale-gen "en_US.UTF-8" && \
    echo "LC_ALL=\"en_US.UTF-8\"" >> /etc/default/locale


ARG AUTH=""
ARG ARTIFACT_HOST=""

ENV JDK_FILE jdk-8u311-linux-x64.tar.gz
ENV JDK_VERSION jdk1.8.0_311
ENV PYTHON_VERSION "3.6.5"

COPY docker/${JDK_FILE} ${WORKDIR}

RUN tar -xzvf ${JDK_FILE} && \
    rm -fr ${JAVA_HOME} && \
    mv ${JDK_VERSION} ${JAVA_HOME}

COPY target/datax/datax .
