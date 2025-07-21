# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11 AS build

# Set working directory
WORKDIR /core

# Copy all projects under core to /core
COPY core/ .

# Update system and install dependencies
RUN apt-get update && apt-get install -y \
    netcat \
    unzip \
    libpq-dev \
    && apt-get clean

WORKDIR /core
# Add .git for runtime calls to jgit from OPversion
COPY .git ../.git

RUN sbt clean WorkflowExecutionService/dist

# Unzip the texera binary
RUN unzip amber/target/universal/texera-0.1-SNAPSHOT.zip -d amber/target/

FROM eclipse-temurin:11-jre-jammy AS runtime

WORKDIR /core/amber

COPY --from=build /core/amber/r-requirements.txt /tmp/r-requirements.txt
COPY --from=build /core/amber/requirements.txt /tmp/requirements.txt
COPY --from=build /core/amber/operator-requirements.txt /tmp/operator-requirements.txt
COPY --from=build /core/python_compiling_service/requirements.txt /tmp/udf-compiling-requirements.txt

# Install Python & R runtime dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    libpq-dev \
    gfortran \
    curl \
    build-essential \
    libreadline-dev \
    libncurses-dev \
    libssl-dev \
    libxml2-dev \
    xorg-dev \
    libbz2-dev \
    liblzma-dev \
    libpcre++-dev \
    libpango1.0-dev \
     libcurl4-openssl-dev \
    unzip \
    && apt-get clean

# Install R and needed libraries
ENV R_VERSION=4.3.3
RUN curl -O https://cran.r-project.org/src/base/R-4/R-${R_VERSION}.tar.gz && \
    tar -xf R-${R_VERSION}.tar.gz && \
    cd R-${R_VERSION} && \
    ./configure --prefix=/usr/local \
                --enable-R-shlib \
                --with-blas \
                --with-lapack && \
    make -j 4 && \
    make install && \
    cd .. && \
    rm -rf R-${R_VERSION}* && R --version && pip3 install --upgrade pip setuptools wheel && \
    pip3 install -r /tmp/requirements.txt && \
    pip3 install -r /tmp/operator-requirements.txt && \
    pip3 install -r /tmp/r-requirements.txt && \
    pip3 install -r /tmp/udf-compiling-requirements.txt
RUN Rscript -e "options(repos = c(CRAN = 'https://cran.r-project.org')); \
                install.packages(c('coro', 'arrow', 'dplyr'), \
                                 Ncpus = parallel::detectCores())"
ENV LD_LIBRARY_PATH=/usr/local/lib/R/lib:$LD_LIBRARY_PATH

# Copy the built texera binary from the build phase
COPY --from=build /.git /.git
COPY --from=build /core/amber/target/texera-0.1-SNAPSHOT /core/amber
# Copy resources directories under /core from build phase
COPY --from=build /core/amber/src/main/resources /core/amber/src/main/resources
COPY --from=build /core/workflow-core/src/main/resources /core/workflow-core/src/main/resources
COPY --from=build /core/file-service/src/main/resources /core/file-service/src/main/resources
# Copy code for python & R UDF
COPY --from=build /core/amber/src/main/python /core/amber/src/main/python
# Copy the UDF compiling service
COPY --from=build /core/python_compiling_service /core/python_compiling_service
# Copy the needed scripts
COPY --from=build /core/amber/*.py /core/amber/

# Create startup script to run both services
RUN echo '#!/bin/bash' > /core/amber/start-services.sh && \
    echo 'echo "Starting UDF Compiling Service..."' >> /core/amber/start-services.sh && \
    echo 'cd /core/python_compiling_service' >> /core/amber/start-services.sh && \
    echo 'python3 src/udf_compiling_service.py 2>&1 | sed "s/^/[UDF-COMPILE] /" &' >> /core/amber/start-services.sh && \
    echo 'UDF_PID=$!' >> /core/amber/start-services.sh && \
    echo 'cd /core/amber' >> /core/amber/start-services.sh && \
    echo 'echo "Starting Computing Unit Master..."' >> /core/amber/start-services.sh && \
    echo 'bin/computing-unit-master 2>&1 | sed "s/^/[COMPUTE-UNIT] /" &' >> /core/amber/start-services.sh && \
    echo 'COMPUTE_PID=$!' >> /core/amber/start-services.sh && \
    echo 'echo "Both services started:"' >> /core/amber/start-services.sh && \
    echo 'echo "  UDF Compiling Service PID: $UDF_PID"' >> /core/amber/start-services.sh && \
    echo 'echo "  Computing Unit Master PID: $COMPUTE_PID"' >> /core/amber/start-services.sh && \
    echo 'echo "UDF Compiling Service available at http://localhost:9999"' >> /core/amber/start-services.sh && \
    echo 'echo "Computing Unit Master available at http://localhost:8085"' >> /core/amber/start-services.sh && \
    echo 'wait $UDF_PID $COMPUTE_PID' >> /core/amber/start-services.sh && \
    chmod +x /core/amber/start-services.sh

CMD ["/core/amber/start-services.sh"]

EXPOSE 8085 9999
