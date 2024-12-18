FROM python:3.12-slim AS builder
ENV TZ="Europe/Brussels"


WORKDIR /app
COPY requirements.txt .
RUN apt-get update \ 
    && apt-get -y install libpq-dev gcc rsync\
    && pip install -v -r requirements.txt \
    && rm -rf /root/.cache
RUN mkdir -p /folder_to_copy/usr/local/lib/python3.12/site-packages \ 
    && rsync -a  /usr/local/lib/python3.12/site-packages /folder_to_copy/usr/local/lib/python3.12 \
    && rsync -a /usr/local/bin /folder_to_copy/usr/local \
    && rsync -a /usr/bin /folder_to_copy/usr \
    && rsync -a /usr/lib /folder_to_copy/usr
ADD src /app/src
ADD tests /app/tests
ADD qc_historical /app/qc_historical
ADD __init__.py /app/

FROM python:3.12-slim
ARG IMAGE_TAG GIT_HASH
ENV TZ="Europe/Brussels" PYTHONPATH="${PYTHONPATH}:/app:/app/src/:/app/tests/" IMAGE_TAG="${IMAGE_TAG}" GIT_HASH="${GIT_HASH}"

WORKDIR /app
COPY --from=builder /app .
COPY --from=builder /folder_to_copy /

# RUN pytest /app/tests/

ENTRYPOINT [ "python", "/app/src/main.py" ]
