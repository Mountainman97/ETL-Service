# get shiny serves plus tidyverse packages image
FROM rocker/shiny-verse:4.3.2

RUN apt-get update -y && apt-get upgrade -y

# install needed/wanted ubuntu packages
RUN apt-get install -y curl
RUN apt-get install -y iputils-ping
RUN apt-get install -y net-tools
RUN apt-get install -y nano

RUN curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# install needed/wanted R Packages
COPY install_R_packages.sh /srv/shiny-server/
RUN ["chmod", "+x", "/srv/shiny-server/install_R_packages.sh"]
RUN /srv/shiny-server/install_R_packages.sh

# add a shiny user for shiny app usage
RUN adduser shiny --disabled-password --gecos '' shiny
RUN adduser shiny sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoer

RUN echo "options('repos' = 'http://cran.rstudio.com/')" >> /usr/local/lib/R/etc/Rprofile.site

RUN apt-get update && apt-get install -y locales \
 && locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# copy the app to the image
COPY app.R /srv/shiny-server/etl_app/

# Shiny Server konfigurieren
COPY shiny-server.conf /etc/shiny-server/shiny-server.conf

# select port
EXPOSE 4444

# Shiny Server starten
CMD ["/usr/bin/shiny-server"]
