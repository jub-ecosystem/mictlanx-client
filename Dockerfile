FROM python:3.9-alpine
WORKDIR /app
RUN pip install mkdocs \
    mkdocstrings[python] \ 
    mkdocs-material \
    pymdown-extensions 
RUN pip install mkdocstrings

COPY ./mictlanx /app/mictlanx
COPY ./mkdocs.yml .
COPY ./docs /app/docs

EXPOSE 8000
CMD ["mkdocs", "serve","--dev-addr=0.0.0.0:8000"]
