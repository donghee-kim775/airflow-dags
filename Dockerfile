# 베이스 이미지를 Python 3.9 슬림으로 설정
FROM python:3.9-slim

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

# 작업 디렉토리를 설정
WORKDIR /app

# boto3 설치
RUN pip install boto3 requests pandas selenium beautifulsoup4 webdriver-manager

# Firefox 관련 설치
RUN apt-get update && apt-get install -y firefox-esr

# 최신 Geckodriver 설치 (현재 Firefox 버전과 호환되는 최신 Geckodriver를 사용)
RUN apt-get install -y wget
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.33.0/geckodriver-v0.33.0-linux64.tar.gz
RUN tar -xvzf geckodriver-v0.33.0-linux64.tar.gz
RUN mv geckodriver /usr/local/bin/

# 현재 디렉토리의 pythonscripts 폴더를 컨테이너의 /app/pythonscripts로 복사
COPY pythonscript ./pythonscript
