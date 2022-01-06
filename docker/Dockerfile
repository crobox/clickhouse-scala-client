FROM ubuntu:20.04

# Setup timezone data
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Add common items
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		curl unzip openjdk-11-jdk libsnappy-java libsnappy-dev \
        git ssh libssl-dev gpg gpg-agent zlib1g-dev liblzma-dev \
	&& rm -rf /var/lib/apt/lists/*

# Install SBT
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee -a /etc/apt/sources.list.d/sbt.list \
  && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add \
  	&& apt-get update \
  	&& apt-get install -y --no-install-recommends sbt \
  	&& rm -rf /var/lib/apt/lists/*

# Setup SBT
RUN cd /tmp && sbt -Dsbt.version=1.4.9 -batch clean \
  && sbt -Dsbt.version=1.5.5 -batch clean \
  && cd /

# Install Docker
ENV DOCKER_VERSION 20.10.7
RUN set -x \
	&& curl -fSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_VERSION}.tgz" -o docker.tgz \
	&& tar -xzvf docker.tgz \
	&& mv docker/* /usr/local/bin/ \
	&& rmdir docker \
	&& rm docker.tgz \
	&& docker -v

# Add Docker User
RUN groupadd docker && adduser --disabled-password --gecos "" gitlab \
	&& usermod -a -G docker gitlab

# Install Maven
ENV MAVEN_VERSION 3.8.4
ENV MAVEN_HOME /usr/share/maven
RUN mkdir -p /usr/share/maven \
  && curl -fsSL https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    | tar -xzC /usr/share/maven --strip-components=1 \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ADD scripts /scripts/

# Initialize environment variables and start the run command or the default one
ENTRYPOINT ["/scripts/entrypoint.sh"]

# Default command passed to the entrypoint script
CMD ["/bin/bash"]
