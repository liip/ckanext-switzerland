ckanext-switzerland
===================

CKAN extension for DCAT-AP Switzerland, templates and different plugins for [opentransportdata.swiss](https://opentransportdata.swiss).

## Requirements

- CKAN 2.8+
- ckanext-scheming
- ckanext-fluent

## Update translations

To generate a new ckanext-switzerland.pot file use the following command::

    vagrant ssh
    source /home/vagrant/pyenv/bin/activate
    cd /var/www/ckanext/ckanext-switzerland/
    ./update_translations.sh

After that open every .po files in the i18n directory and do an Catalog => Update from POT file.

## Installation

To install ckanext-switzerland:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-switzerland Python package into your virtual environment::

     pip install ckanext-switzerland

3. Add ``switzerland`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload


## Config Settings

This extension uses the following config options (.ini file)

    # the URL of the WordPress AJAX interface
    ckanext.switzerland.wp_ajax_url = https://wp/wp-admin/admin-ajax.php
    ckanext.switzerland.wp_ajax_url = http://wp/cms/wp-admin/admin-ajax.php
    ckanext.switzerland.wp_template_url = http://wp/cms/wp-admin/admin-post.php?action=get_nav
    ckanext.switzerland.wp_url = http://wp

    # piwik config
    piwik.site_id = 1
    piwik.url = stats.opentransportdata.swiss


## Development Installation

To install ckanext-switzerland for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/ogdch/ckanext-switzerland.git
    cd ckanext-switzerland
    python setup.py develop
    pip install -r dev-requirements.txt
    pip install -r requirements.txt
