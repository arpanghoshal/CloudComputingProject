FROM python:3.7-slim-stretch

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
RUN mkdir -p /code
COPY . /code
WORKDIR /code
RUN pip install -r requirements.txt
