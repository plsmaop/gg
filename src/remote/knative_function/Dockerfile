FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY . .

EXPOSE 8080

CMD [ "uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "8080" ]