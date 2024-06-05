FROM python:3.11
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY preentrega_pedro_toledo.py .
COPY config.env .

CMD ["python", "-u", "preentrega_pedro_toledo.py"]