ARG BASE_IMAGE=ubuntu:20.04
ARG USERNAME=smruser

FROM ${BASE_IMAGE} AS build

ENV DEBIAN_FRONTEND="noninteractive"

ARG USERNAME
ENV USERNAME=${USERNAME}

RUN echo "username: ${USERNAME}"

RUN apt-get -qq update \
  && apt-get install -y \
    g++ \
    cmake \
    openmpi-bin \
    libopenmpi-dev \
    openmpi-common \
    libtbb-dev

WORKDIR /home/${USERNAME}
  
ADD . .

RUN mkdir -p build \
  && cd build \
  && cmake -DSIMPLEMR_BUILD_APP=ON .. \
  && make -j

FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND="noninteractive"

ARG USERNAME
ENV USERNAME=${USERNAME}

RUN apt-get -qq update && apt-get install -y \
    openmpi-bin \
    libopenmpi-dev \
    openmpi-common \
  && useradd -ms /bin/bash ${USERNAME} \
  && mkdir -p /home/${USERNAME}/build/

WORKDIR /home/${USERNAME}

COPY --from=build /home/${USERNAME}/run_task /home/${USERNAME}
COPY --from=build /home/${USERNAME}/build/libsimplemapreduce.so /home/${USERNAME}/build/

RUN chown ${USERNAME} /home/${USERNAME}/run_task

USER ${USERNAME}
