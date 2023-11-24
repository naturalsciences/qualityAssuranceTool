FROM python:3.11
ARG ID_U
ENV TZ="Europe/Brussels"

WORKDIR /app

COPY requirements.txt .
RUN pip install --compile -r requirements.txt && rm -rf /root/.cache
ADD src /app/src
ADD tests /app/tests
ADD tests/conf /app/tests/conf
ADD resources /app/resources
RUN ls /app
ADD __init__.py /app/

RUN groupadd -g $ID_U usergroup && useradd -m -u $ID_U -g $ID_U myuser && chown -R myuser /app
USER myuser

ENV PYTHONPATH "${PYTHONPATH}:/app:/app/src/:/app/tests/"
RUN pytest /app/tests/

ENTRYPOINT [ "python", "/app/src/main.py" ]