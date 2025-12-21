FROM python:3.10

COPY fastapi_service/requirements.txt fastapi_service/setup.py /workdir/
COPY fastapi_service/app/ /workdir/app/
COPY fastapi_service/ml/ /workdir/ml/

WORKDIR /workdir

RUN pip install -U -e .

COPY for_pip_install/* /workdir/
RUN pip install ./train_and_valid_model-0.2.16-py3-none-any.whl
RUN pip install ./preprocess_data-0.1.16-py3-none-any.whl

RUN export MLFLOW_TRACKING_USERNAME=$USER
RUN export MLFLOW_TRACKING_PASSWORD=$PASSWORD

# Run the application
CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "80"]