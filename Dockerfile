FROM aptplatforms/oraclelinux-python

WORKDIR /app

COPY . .

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 6000

CMD python app.py