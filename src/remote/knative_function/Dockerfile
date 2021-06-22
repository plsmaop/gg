FROM python:3.9-slim

ENV PYTHONUNBUFFERED True

COPY . .

EXPOSE 8080

RUN pip install Flask

CMD [ "python", "main2.py" ]