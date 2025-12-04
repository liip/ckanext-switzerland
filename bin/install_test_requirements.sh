#!/bin/bash

# Install requirements and ckanext
pip install -e /__w/ckanext-switzerland/ckanext-switzerland/[dev]

# Install ckanext dependencies
pip install -e git+https://github.com/ckan/ckanext-dcat.git#egg=ckanext-dcat
pip install -e git+https://gitlab.liip.ch/odp_oev_schweiz/ckanext-harvest.git#egg=ckanext-harvest
pip install -r https://gitlab.liip.ch/odp_oev_schweiz/ckanext-harvest/-/raw/main/requirements.txt
pip install -e git+https://github.com/ckan/ckanext-scheming.git#egg=ckanext-scheming
pip install -e git+https://github.com/ckan/ckanext-fluent.git#egg=ckanext-fluent
pip install -r https://raw.githubusercontent.com/ckan/ckanext-fluent/master/requirements.txt
pip install -e git+https://github.com/ckan/ckanext-showcase.git#egg=ckanext-showcase

# Replace default path to CKAN core config file with the one on the container
sed -i -e 's/use = config:.*/use = config:\/srv\/app\/src\/ckan\/test-core.ini/' /__w/ckanext-switzerland/ckanext-switzerland/test.ini

# Init db and re-enable required plugins
ckan -c /__w/ckanext-switzerland/ckanext-switzerland/test.ini db init
