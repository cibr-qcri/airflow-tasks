FROM python:3.8
LABEL maintainer.0="CIBR-QCRI Team"

WORKDIR /usr/src/job

ARG modified_file
ARG gp_host
ARG gp_port
ARG gp_username
ARG gp_password
ARG gp_db

ENV PYTHONUNBUFFERED=1
ENV GREENPLUM_HOST=$gp_host
ENV GREENPLUM_PORT=$gp_port
ENV GREENPLUM_USERNAME=$gp_username
ENV GREENPLUM_PASSWORD=$gp_password
ENV GREENPLUM_DB=$gp_db

COPY dependencies ./dependencies
COPY requirements.txt ./
COPY $modified_file ./script.py

RUN pip3 install -r requirements.txt

CMD [ "stdbuf", "-oL", "python3", "script.py" ]
