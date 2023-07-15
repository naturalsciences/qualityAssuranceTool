FROM python:3.11

WORKDIR /app
COPY requirements.txt .
RUN pip install --compile -r requirements.txt && rm -rf /root/.cache
ADD src /app/src
ADD tests /app/tests
ADD tests/conf /app/tests/conf
RUN ls /app
ADD __init__.py /app/
ENV PYTHONPATH "${PYTHONPATH}:/app:/app/src/:/app/tests/"
RUN pytest /app/tests/