FROM apache/spark:3.5.1
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget bzip2 \
    && wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm /tmp/miniconda.sh \
    && /opt/conda/bin/pip install --no-cache-dir joblib "numpy>=2.0" "scikit-learn>=1.6,<1.7" pandas pymongo \
    && apt-get purge -y wget bzip2 \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Model + pandas only on the driver; workers keep the image default Python.
ENV PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python
ENV PATH=/opt/conda/bin:${PATH}
