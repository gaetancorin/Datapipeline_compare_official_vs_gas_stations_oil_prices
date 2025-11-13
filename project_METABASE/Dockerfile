FROM python:3.11.9

WORKDIR /Flask_API
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN apt-get update && apt-get install -y docker.io

COPY run.py run.py
COPY metabase_db_example/ metabase_db_example/
COPY env/ env/
RUN rm env/.env && cp env/.env_prod env/.env
COPY App/ App/

# see logs in real times
ENV PYTHONUNBUFFERED=1

ENTRYPOINT [ "python" ]
CMD [ "run.py" ]
