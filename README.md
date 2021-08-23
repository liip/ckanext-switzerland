ckanext-switzerland
===================

CKAN extension for DCAT-AP Switzerland, templates and different plugins for [opentransportdata.swiss](https://opentransportdata.swiss).

## Requirements

- CKAN 2.8+
- ckanext-fluent
- ckanext-harvest
- ckanext-scheming

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

    # matomo config
    matomo.site_id = 1
    matomo.url = stats.opentransportdata.swiss


## Development Installation

To install ckanext-switzerland for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/ogdch/ckanext-switzerland.git
    cd ckanext-switzerland
    python setup.py develop
    pip install -r dev-requirements.txt
    pip install -r requirements.txt


## Commands

### Command to cleanup the datastore database.
[Datastore currently does not delete tables](https://github.com/ckan/ckan/issues/3422) when the corresponding resource is deleted.
This command finds these orphaned tables and deletes its rows to free the space in the database.
It is meant to be run regularly by a cronjob.

```bash
paster --plugin=ckanext-ogdchcommands ogdch cleanup_datastore -c /var/www/ckan/development.ini
```

### Command to cleanup the harvest jobs.
This commands deletes the harvest jobs and objects per source and overall leaving only the latest n,
where n and the source are optional arguments. The command is supposed to be used in a cron job to
provide for a regular cleanup of harvest jobs, so that the database is not overloaded with unneeded data
of past job runs. It has a dryrun option so that it can be tested what will get be deleted in the
database before the actual database changes are performed.

```bash
paster --plugin=ckanext-ogdchcommands ogdch cleanup_harvestjobs [{source_id}] [--keep={n}}] [--dryrun] -c /var/www/ckan/development.ini
```
