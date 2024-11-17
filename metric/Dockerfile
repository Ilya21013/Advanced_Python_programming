FROM python:3.9
WORKDIR /usr/src/app
COPY ./src ./
COPY ./requirements.txt ./

# Проверка соединения с pypi.org
RUN curl -I https://pypi.org

# Обновляем pip и setuptools
RUN pip install --upgrade pip setuptools wheel

RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "./metric.py"]

