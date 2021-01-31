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

RUN mkdir -p build \
  && cd build \
  && cmake -DSIMPLEMR_BUILD_APP=ON .. \
  && make -j \
  && chown smruser /home/smruser/app

USER smruser