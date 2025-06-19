FROM postgres:15

# Install build dependencies
RUN apt-get update && \
    apt-get install -y postgresql-server-dev-15 git make gcc

# Install pg_cron
RUN git clone --branch v1.5.2 https://github.com/citusdata/pg_cron.git /tmp/pg_cron && \
    cd /tmp/pg_cron && \
    make && make install && \
    rm -rf /tmp/pg_cron

# Clean up
RUN apt-get remove --purge -y git make gcc postgresql-server-dev-15 && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Ensure UTF-8 environment
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8