#!/bin/bash

# Install requirements and ckanext
pip install -r /__w/ckanext-switzerland/ckanext-switzerland/requirements.txt
pip install --ignore-installed -r /__w/ckanext-switzerland/ckanext-switzerland/dev-requirements.txt
pip install -e /__w/ckanext-switzerland/ckanext-switzerland/

# Install ckanext dependencies
pip install -e git+https://github.com/ckan/ckanext-dcat.git@v1.5.1#egg=ckanext-dcat
pip install -r https://raw.githubusercontent.com/ckan/ckanext-dcat/v1.5.1/requirements.txt
pip install -e git+https://gitlab.liip.ch/odp_oev_schweiz/ckanext-harvest.git@v1.0.2#egg=ckanext-harvest
pip install -r https://gitlab.liip.ch/odp_oev_schweiz/ckanext-harvest/-/raw/v1.0.2/requirements.txt
pip install -e git+https://github.com/ckan/ckanext-scheming.git@release-3.0.0#egg=ckanext-scheming
pip install -e git+https://github.com/ckan/ckanext-fluent.git#egg=ckanext-fluent
pip install -r https://raw.githubusercontent.com/ckan/ckanext-fluent/master/requirements.txt

# Replace default path to CKAN core config file with the one on the container
sed -i -e 's/use = config:.*/use = config:\/srv\/app\/src\/ckan\/test-core.ini/' /__w/ckanext-switzerland/ckanext-switzerland/test.ini

# Init db and re-enable required plugins
ckan config-tool /__w/ckanext-switzerland/ckanext-switzerland/test.ini "ckan.plugins = "
ckan -c /__w/ckanext-switzerland/ckanext-switzerland/test.ini db init
ckan config-tool /__w/ckanext-switzerland/ckanext-switzerland/test.ini "ckan.plugins = ogdch ogdch_pkg ogdch_res ogdch_group ogdch_org harvest sbb_harvester timetable_harvester datastore fluent scheming_datasets scheming_groups scheming_organizations"
