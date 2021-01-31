FROM ubuntu:20.04

ENV DEBIAN_FRONTEND="noninteractive"

RUN apt-get update && apt-get upgrade -y \
  && useradd -ms /bin/bash smruser \
  && apt-get install -y \
    g++ \
    cmake \
    openmpi-bin \
    libopenmpi-dev \
    openmpi-common

WORKDIR /home/smruser
  
ADD . .

RUN mkdir -p build app/build \
  && cd build \
  && cmake .. \
  && make -j \
  && cd ../app/build \
  && cmake .. \
  && make -j \
  && chown smruser /home/smruser/app

USER smruser