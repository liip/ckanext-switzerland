#!/usr/bin/env bash
# run this inside the vagrant box
source /home/vagrant/pyenv/bin/activate
cd /var/www/ckanext/ckanext-switzerland

# hack to ignore ckanext-harvest strings
mv ckanext/switzerland/templates/source/ source/
python setup.py extract_messages
mv source/ ckanext/switzerland/templates/source/
