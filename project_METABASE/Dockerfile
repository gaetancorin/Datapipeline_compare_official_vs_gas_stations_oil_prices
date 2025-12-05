FROM python:3.11.9

WORKDIR /Flask_API
COPY requirements.txt .
RUN pip install -r requirements.txt

# ---------------------------------------------------------
# REMOVE OLD DOCKER CLIENT (docker.io) AND INSTALL NEW ONE
# ---------------------------------------------------------
RUN apt-get update && apt-get remove -y docker.io docker-doc docker-compose \
    docker-compose-v2 podman-docker containerd runc || true

# install official docker-ce-cli
RUN apt-get update && apt-get install -y curl
RUN curl -fsSL https://get.docker.com | sh

# ---------------------------------------------------------
# END DOCKER CLIENT FIX
# ---------------------------------------------------------

COPY run.py run.py
COPY metabase_db_example/ metabase_db_example/
COPY env/ env/
RUN rm env/.env && cp env/.env_prod env/.env
COPY App/ App/

# see logs in real times
ENV PYTHONUNBUFFERED=1

ENTRYPOINT [ "python" ]
CMD [ "run.py" ]
