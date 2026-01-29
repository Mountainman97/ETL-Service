#!/bin/bash

packages=(
    "shiny" \
    "shinymanager" \
    "tidyverse" \
    "visNetwork" \
    "DBI" \
    "odbc" \
    "bslib" \
    "argonR" \
    "DT" \
    "shinyWidgets" \
    "hms" \
    "timevis" \
    "glue" \
    "htmlwidgets" \
    "data.tree" \
    "shinyTree" \
    "shinyBS" \
)

for package in "${packages[@]}"
do
    R -e "install.packages(\"${package}\")"
done
