FROM python:3.11
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

# groupid and userid are hardcoded
# option to change to `id -g` and `id -u` from cli?
RUN groupadd -g 1001 usergroup && useradd -m -u 1001 -g 1001 myuser && chown -R myuser /app
USER myuser

ENV PYTHONPATH "${PYTHONPATH}:/app:/app/src/:/app/tests/"
RUN pytest /app/tests/

ENTRYPOINT [ "python", "/app/src/main.py" ]