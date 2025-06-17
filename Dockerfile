FROM metabrainz/python:3.13-20250616

RUN useradd --create-home --shell /bin/bash art

WORKDIR /home/art/artwork-indexer

COPY --chown=art:art pyproject.toml poetry.lock ./

RUN chown art:art /home/art/artwork-indexer && \
    apt-get update && \
    apt-get install \
        --no-install-recommends \
        --no-install-suggests \
        -y \
        # build-essential \
        sudo && \
    # Explicit references to /usr/local/bin/python are used in case the
    # Ubuntu-packaged Python 3.10 is temporarily installed via
    # build-essential.
    sudo -E -H -u art /usr/local/bin/python -m pip install --user --no-warn-script-location 'pipx==1.7.1' && \
    sudo -E -H -u art env PATH="/home/art/.local/bin:$PATH" pipx install --python /usr/local/bin/python 'poetry==2.1.3' && \
    sudo -E -H -u art env PATH="/home/art/.local/bin:$PATH" poetry install && \
    # apt-get purge --auto-remove -y build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=art:art \
    docker/config.ini.ctmpl \
    docker/

COPY --chown=art:art \
    handlers.py \
    handlers_base.py \
    indexer.py \
    pg_conn_wrapper.py \
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
