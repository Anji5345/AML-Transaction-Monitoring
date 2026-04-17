FROM jenkins/jenkins:lts-jdk17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

USER jenkins

RUN jenkins-plugin-cli --plugins \
    workflow-aggregator \
    git \
    credentials-binding \
    pipeline-stage-view \
    blueocean

COPY init.groovy.d/ /usr/share/jenkins/ref/init.groovy.d/

USER jenkins
