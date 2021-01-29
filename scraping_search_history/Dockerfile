# FROM gcr.io/google-appengine/python
FROM python:3.9
# Adding trusting keys to apt for repositories
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -

# Adding Google Chrome to the repositories
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'

#update and install dependencies
RUN apt-get -y update
RUN apt-get install -y xvfb
RUN apt-get install -y google-chrome-stable


#RUN mount -t tmpfs -o rw,nosuid,nodev,noexec,relatime,size=512M tmpfs /dev/shm
# setting up the python environment
#LABEL python_version=python
# RUN virtualenv --no-download /env -p python
# ENV VIRTUAL_ENV /env
# ENV PATH /env/bin:$PATH
# ADD requirements.txt /app/
COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt
# ADD . /app/
CMD exec gunicorn -b :$PORT main:app --timeout 900 --log-level debug
