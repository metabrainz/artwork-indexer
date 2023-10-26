FROM metabrainz/python:3.11-20231006

RUN useradd --create-home --shell /bin/bash art

WORKDIR /home/art/artwork-indexer

RUN chown art:art /home/art/artwork-indexer && \
    apt-get update && \
    apt-get install \
        --no-install-recommends \
        --no-install-suggests \
        -y \
        sudo && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=art:art requirements.txt ./

RUN sudo -E -H -u art pip install --user -r requirements.txt

COPY --chown=art:art \
    docker/config.ini.ctmpl \
    docker/

COPY --chown=art:art \
    handlers.py \
    handlers_base.py \
    indexer.py \
    ./

COPY docker/artwork-indexer \
    /usr/local/bin/

COPY docker/indexer.service \
    /etc/service/indexer/run

RUN chmod 755 \
    /usr/local/bin/artwork-indexer \
    /etc/service/indexer/run

COPY docker/consul-template.conf \
    /etc/consul-template-indexer.conf
