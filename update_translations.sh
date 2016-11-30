#!/usr/bin/env bash
# run this inside the vagrant box
source /home/vagrant/pyenv/bin/activate
cd /var/www/ckanext/ckanext-switzerland

# hack to ignore ckanext-harvest strings
mv ckanext/switzerland/templates/source/ source/
python setup.py extract_messages
mv source/ ckanext/switzerland/templates/source/

msgmerge -U i18n/de/LC_MESSAGES/ckanext-switzerland.po i18n/ckanext-switzerland.pot
msgmerge -U i18n/fr/LC_MESSAGES/ckanext-switzerland.po i18n/ckanext-switzerland.pot
msgmerge -U i18n/it/LC_MESSAGES/ckanext-switzerland.po i18n/ckanext-switzerland.pot

msgcat -u i18n/de/LC_MESSAGES/ckanext-switzerland.po ../../ckan/ckan/i18n/de/LC_MESSAGES/ckan.po -o i18n/de/LC_MESSAGES/ckanext-switzerland.po
msgcat -u i18n/fr/LC_MESSAGES/ckanext-switzerland.po ../../ckan/ckan/i18n/fr/LC_MESSAGES/ckan.po -o i18n/fr/LC_MESSAGES/ckanext-switzerland.po
msgcat -u i18n/it/LC_MESSAGES/ckanext-switzerland.po ../../ckan/ckan/i18n/it/LC_MESSAGES/ckan.po -o i18n/it/LC_MESSAGES/ckanext-switzerland.po

rm i18n/de/LC_MESSAGES/ckanext-switzerland.po~
rm i18n/fr/LC_MESSAGES/ckanext-switzerland.po~
rm i18n/it/LC_MESSAGES/ckanext-switzerland.po~
