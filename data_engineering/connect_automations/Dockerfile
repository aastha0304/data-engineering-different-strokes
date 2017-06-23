FROM python:3

RUN pip install mysqlclient && pip install requests && pip install requests_toolbelt

ADD config.py /
ADD table_tscolumn.tsv /
ADD source_initer.py /
ADD sink_initer_mysql.py /
CMD [ "python", "./sink_initer_mysql.py" ]