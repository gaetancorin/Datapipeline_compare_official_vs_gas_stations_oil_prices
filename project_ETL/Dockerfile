FROM python:3.11.9-slim

WORKDIR /project_master_ETL

# Install Chrome ou Chromium
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    libu2f-udev \
    libvulkan1 \
    unzip \
    chromium

# Install MongoDB tools (mongodump, mongorestore, etc.)
RUN apt-get update && apt-get install -y wget ca-certificates tar
RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian11-x86_64-100.12.2.tgz -O /tmp/mongodb-tools.tgz
RUN tar -xzf /tmp/mongodb-tools.tgz -C /tmp
RUN cp /tmp/mongodb-database-tools-*/bin/* /usr/local/bin/

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY run.py run.py
COPY env/ env/
RUN rm env/.env && cp env/.env_prod env/.env
COPY App/ App/

# see logs in real times
ENV PYTHONUNBUFFERED=1

ENTRYPOINT [ "python" ]
CMD [ "run.py" ]
