FROM jupyter/base-notebook:python-3.8.8

WORKDIR /app/

USER $NB_USER

RUN python -V
RUN python -m pip install virtualenv pipdeptree

USER root

RUN ["/bin/bash", "-c", "python -m venv venv"]

USER $NB_USER

RUN ["/bin/bash", "-c", "source venv/bin/activate"]
RUN ["/bin/bash", "-c", "pip install --upgrade pip"]

USER root

RUN apt update && apt-get install -y software-properties-common curl wget yarn
RUN apt-add-repository ppa:openjdk-r/ppa
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk

USER $NB_USER

RUN wget -qO- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.2/install.sh | bash
RUN source ~/.nvm/nvm.sh && \
    nvm install node 19.0.1

RUN pip install jupyterlab-lsp "nbclassic>=0.2.8"

COPY requirements.txt ./
RUN pip install -r requirements.txt

USER root

COPY . ./
RUN chown $NB_USER ./
RUN source ~/.nvm/nvm.sh && \
    pip install .

USER $NB_USER

ENV JUPYTER_ENABLE_LAB=yes
ENV FLINK_HOME=/opt/conda/lib/python3.8/site-packages/pyflink

RUN chown -R $NB_USER:users $FLINK_HOME
USER $NB_USER
