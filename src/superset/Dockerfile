FROM apache/superset:3.0.1
USER root
RUN pip install --no-cache-dir psycopg2-binary
COPY init.sh bootstrap.py /app/superset_init/
RUN sed -i 's/\r$//' /app/superset_init/init.sh \
    && chmod +x /app/superset_init/init.sh
USER superset
