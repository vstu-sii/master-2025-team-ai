FROM apache/airflow:2.9.1-python3.10

COPY airflow_service/requirements.txt /requirements.txt
COPY for_pip_install/* .

RUN pip install --upgrade pip

RUN pip uninstall keras
RUN pip uninstall tensorflow
RUN pip cache purge

RUN pip install ./train_and_valid_model-0.2.16-py3-none-any.whl
RUN pip install ./preprocess_data-0.1.16-py3-none-any.whl

RUN pip install -r /requirements.txt

ARG DAGSHUB_USER=build
ARG DAGSHUB_PASSWORD=build
ARG DAGSHUB_TOKEN=build

ENV DAGSHUB_TOKEN=${DAGSHUB_TOKEN}

RUN export MLFLOW_TRACKING_USERNAME=${DAGSHUB_USER}
RUN export MLFLOW_TRACKING_PASSWORD=${DAGSHUB_PASSWORD}