FROM python:3.7-slim-stretch
RUN mkdir -p /code
COPY . /code
ENV TEAM_NAME="CC_0240_0251_0648"
WORKDIR /code
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
RUN pip install -r requirements.txt
