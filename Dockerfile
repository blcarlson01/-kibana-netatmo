FROM python:slim  

COPY requirements.txt /app/requirements.txt
COPY src/netatmo_elastic.py /app/netatmo_elastic.py
COPY src/backup_data/ /app/backup_data

RUN mkdir /app/ca_certs && \
    python3 -m pip install --no-cache-dir --trusted-host pypi.python.org -r /app/requirements.txt 

CMD [ "python3", "/app/netatmo_elastic.py" ]