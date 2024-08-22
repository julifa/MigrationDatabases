# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.10-slim-buster
WORKDIR /app
COPY . /app
RUN python -m pip install -r requirements.txt
EXPOSE 5000
# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD python ./main.py
