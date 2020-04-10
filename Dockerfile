
FROM python:3.7-alpine as base
FROM base as builder
RUN apk add --update build-base python-dev libffi-dev openssl-dev git musl-dev \
libxslt-dev gcc libffi-dev openssl-dev git
RUN mkdir /install
ADD requirements.txt /install/requirements.txt
RUN pip install -r /install/requirements.txt
RUN pip install --install-option="--prefix=/install" -r /install/requirements.txt
