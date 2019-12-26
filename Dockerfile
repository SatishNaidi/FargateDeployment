FROM alpine:latest

RUN apk add --update python3  \
	&& pip3 install --upgrade pip \
	&& pip3 install boto3 -U \
        && ln -sv /usr/bin/python3 /usr/bin/python

ENTRYPOINT [ "python3" ]

COPY ./ /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD [ "run.py" ]
