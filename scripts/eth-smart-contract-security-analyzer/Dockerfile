FROM docker:dind
LABEL maintainer.0="CIBR-QCRI Team"

RUN apt-get update

# Install Docker
RUN apt install -y apt-transport-https ca-certificates curl software-properties-common && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg  \
    | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg]  \
    https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"  \
    | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt update && \
    apt install -y docker-ce

RUN apt-get -y install git python3-pip

RUN mkdir /smartbugs
RUN git clone https://github.com/smartbugs/smartbugs.git /smartbugs
RUN pip3 install -r /smartbugs/requirements.txt

CMD ["sleep", "1d"]