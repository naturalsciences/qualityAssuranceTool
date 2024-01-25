FROM python:3.11-slim AS builder
ARG ID_U
ENV TZ="Europe/Brussels"


WORKDIR /app
COPY requirements.txt .
RUN apt-get update \ 
    && apt-get -y install libpq-dev gcc \
    && pip install -v -r requirements.txt \
    && rm -rf /root/.cache
ADD src /app/src
# ADD tests /app/tests
# ADD tests/conf /app/tests/conf
# ADD resources /app/resources
ADD __init__.py /app/

FROM python:3.11-slim
ARG ID_U
ENV TZ="Europe/Brussels"

WORKDIR /app
COPY --from=builder /app .
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr /usr

# ENV PYTHONPATH "${PYTHONPATH}:/app:/app/src/:/app/tests/"
# RUN pytest /app/tests/

ENTRYPOINT [ "python", "/app/src/main.py" ]