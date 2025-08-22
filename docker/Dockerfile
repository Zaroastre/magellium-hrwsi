FROM postgres:latest AS base

LABEL maintainers="nicolas.metivier@magellium.fr"

COPY init_database /docker-entrypoint-initdb.d/

FROM base AS dev

RUN apt update && apt-get install --yes\
  build-essential \
  libicu-dev \
  unzip \
  wget

WORKDIR /home
RUN wget https://api.pgxn.org/dist/pgtap/1.3.3/pgtap-1.3.3.zip && \
  unzip pgtap-1.3.3.zip && \
  cd pgtap-1.3.3 && \
  cpan TAP::Parser::SourceHandler::pgTAP && \
  cpan TAP::Harness::Archive &&\
  make && \
  make install

RUN apt update && apt-get install --yes\
 postgis \
 postgresql-16-postgis-3 \
 postgresql-16-postgis-scripts