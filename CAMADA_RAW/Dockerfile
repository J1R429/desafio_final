ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim as base



WORKDIR /app


RUN --mount=type=cache,target=/root/.cache/pip \
    pip install boto3

COPY . .


CMD ["python", "script_filmes_series.py"]
