FROM debian:bullseye

# Install wget and unzip
RUN apt-get update && apt-get install -y \
    wget unzip \
    && rm -rf /var/lib/apt/lists/*

# Download and install DuckDB CLI
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-amd64.zip

# Copy files into the container
COPY data/source.csv /data/source.csv
COPY requirements.txt requirements.txt
COPY src src    

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install -r requirements.txt

CMD ["bash"]