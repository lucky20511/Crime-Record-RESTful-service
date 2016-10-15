FROM python:2.7.10
COPY . /lab2
WORKDIR /lab2
RUN pip install -r requirements.txt
EXPOSE 8000
CMD python lab2.py