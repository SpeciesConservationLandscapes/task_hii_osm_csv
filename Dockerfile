FROM debian:buster-slim


RUN apt-get update
RUN apt-get install -y osmium-tool python3 python3-pip git

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip

RUN pip install git+https://github.com/SpeciesConservationLandscapes/task_base.git
RUN pip install requests==2.25.0 pytest==6.1.2 six==1.15.0 gitpython==3.1.11 pyproj==3.0.0.post1 shapely==1.7.1

WORKDIR /app
COPY $PWD/src .