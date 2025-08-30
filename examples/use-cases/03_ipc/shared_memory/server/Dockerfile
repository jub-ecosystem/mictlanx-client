FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r /app/requirements.txt
COPY src /app
CMD ["python3","/app/main.py"]