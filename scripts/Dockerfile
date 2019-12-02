FROM golang:latest

ARG GIT_COMMIT
RUN apt-get update -y
WORKDIR /usr/src
RUN wget https://github.com/edenhill/librdkafka/archive/v1.1.0.tar.gz -O librdkafka.tar.gz
RUN tar -xvzf librdkafka.tar.gz
RUN cd librdkafka-1.1.0 && ./configure --install-deps && make && make install
RUN rm librdkafka.tar.gz
ENV GO111MODULE on
ENV LD_LIBRARY_PATH /usr/local/lib
RUN go get -v github.com/gojekfarm/kafqa
