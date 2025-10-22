FROM debian:bullseye-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends osmium-tool python3 python3-pip git python3-gdal && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip install --upgrade pip
RUN pip install git+https://github.com/SpeciesConservationLandscapes/task_base.git
RUN pip install \
    requests==2.31.0 \
    pytest==7.4.3 \
    six==1.16.0 \
    gitpython==3.1.40 \
    pyproj==3.6.1 \
    shapely==1.8.5 \
    rasterio==1.3.9 \
    numba==0.56.4 \
    earthengine-api==0.1.379

WORKDIR /app
COPY src ./src