FROM nunopreguica/sd2526tpbase

# working directory inside docker image
WORKDIR /home/sd

ADD hibernate.cfg.xml .
ADD messages.props .

# Generate TLS keystores inside Docker to avoid binary corruption from Windows git
COPY generate-keystores.sh .
RUN chmod +x generate-keystores.sh && ./generate-keystores.sh && rm generate-keystores.sh

# copy the jar created by assembly to the docker image
COPY target/sd*.jar sd2526.jar
