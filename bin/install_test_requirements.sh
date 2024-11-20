#!/bin/bash

# Install requirements and ckanext
pip install -r /__w/ckanext-switzerland/ckanext-switzerland/requirements.txt
pip install --ignore-installed -r /__w/ckanext-switzerland/ckanext-switzerland/dev-requirements.txt
pip install -e /__w/ckanext-switzerland/ckanext-switzerland/

# Install ckanext dependencies
pip install -e git+https://github.com/ckan/ckanext-dcat.git#egg=ckanext-dcat
pip install -r https://raw.githubusercontent.com/ckan/ckanext-dcat/master/requirements.txt
pip install -e git+https://gitlab.liip.ch/odp_oev_schweiz/ckanext-harvest.git#egg=ckanext-harvest
pip install -r https://gitlab.liip.ch/odp_oev_schweiz/ckanext-harvest/-/raw/main/requirements.txt
pip install -e git+https://github.com/ckan/ckanext-scheming.git#egg=ckanext-scheming
pip install -e git+https://github.com/ckan/ckanext-fluent.git#egg=ckanext-fluent
pip install -r https://raw.githubusercontent.com/ckan/ckanext-fluent/master/requirements.txt

# Init db and re-enable required plugins
ckan -c /__w/ckanext-switzerland/ckanext-switzerland/test.ini db init
ckan -c /__w/ckanext-switzerland/ckanext-switzerland/test.ini db pending-migrations --apply
