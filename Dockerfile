FROM python:3.9

WORKDIR /thesis-data-converter

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ADD . /thesis-data-converter/
ENV PYTHONPATH "${PYTHONPATH}:/thesis-data-converter/"
