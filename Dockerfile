FROM python:3.8

ARG modified_file

WORKDIR /usr/src/job

COPY requirements.txt ./
COPY $modified_file ./script.py

RUN pip3 install -r requirements.txt

CMD [ "python3", "script.py" ]