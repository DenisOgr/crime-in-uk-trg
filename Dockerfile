FROM docker.io/bitnami/spark:3.3

USER root
RUN pip install tabulate==0.8.10