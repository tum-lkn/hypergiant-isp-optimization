FROM python:3.7-slim

LABEL maintainer="johannes.zerwas@tum.de"
LABEL version="1.0"
LABEL description="Basic python environment for HG-ISP topology optimization framework"

ENV GUROBI_INSTALL /opt/gurobi
ENV GUROBI_HOME $GUROBI_INSTALL/linux64
ENV PATH $PATH:$GUROBI_HOME/bin
ENV LD_LIBRARY_PATH $GUROBI_HOME/lib

ARG GUROBI_MAJOR_VERSION=8.1
ARG GUROBI_VERSION=8.1.0
ARG user=sim
ARG group=sim
ARG uid=1000
ARG gid=1000

# Add user
RUN addgroup --gid ${gid} ${group} && \
    adduser --home /home/${user} --uid ${uid} --ingroup ${group} --shell /bin/bash ${user}
RUN apt-get update && \
    apt-get install -y wget git pkg-config build-essential cmake autoconf libtool zlib1g-dev lsb-release make \
     swig python3-dev python3-wheel python3-setuptools python3-six

# Download and install Gurobi
RUN mkdir -p ${GUROBI_INSTALL} && \
    wget -P /home/${user}/ http://packages.gurobi.com/${GUROBI_MAJOR_VERSION}/gurobi${GUROBI_VERSION}_linux64.tar.gz && \
    tar xvfz /home/${user}/gurobi${GUROBI_VERSION}_linux64.tar.gz                                                    && \
    mv /gurobi810/linux64/ ${GUROBI_INSTALL} && \
    cd ${GUROBI_HOME} && \
    python setup.py install && rm /home/${user}/gurobi${GUROBI_VERSION}_linux64.tar.gz
COPY gurobi.lic /home/${user}/gurobi.lic

COPY cplex_studio129.tar.xz cplex_studio129.tar.xz
RUN tar -xjf cplex_studio129.tar.xz --directory /opt/ && rm cplex_studio129.tar.xz

RUN git clone --branch v7.3 https://github.com/google/or-tools /opt/or-tools
RUN chown -R ${user} /opt/or-tools
USER ${user}
RUN cd /opt/or-tools && make third_party && \
    echo "UNIX_CPLEX_DIR=/opt/cplex_studio129" >> /opt/or-tools/Makefile.local && \
    echo "UNIX_GUROBI_DIR=/opt/gurobi" >> /opt/or-tools/Makefile.local && \
    echo "GUROBI_LIB_VERSION=81" >> /opt/or-tools/Makefile.local && make python && make install_python

RUN pip install networkx numpy pandas tables matplotlib statsmodels scipy

CMD ["/bin/bash"]