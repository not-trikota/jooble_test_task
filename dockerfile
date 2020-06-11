FROM python:3.7

RUN apt-get update
RUN apt-get install -y python3-dev

RUN mkdir -p /code
WORKDIR /code
COPY . .

RUN ls && python setup.py install
CMD python main.py
