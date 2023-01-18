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
paster --plugin=ckanext-switzerland ogdch cleanup_datastore -c /var/www/ckan/development.ini
```

### Command to cleanup the harvest jobs.
This commands deletes the harvest jobs and objects per source and overall leaving only the latest n,
where n and the source are optional arguments. The command is supposed to be used in a cron job to
provide for a regular cleanup of harvest jobs, so that the database is not overloaded with unneeded data
of past job runs. It has a dryrun option so that it can be tested what will get be deleted in the
database before the actual database changes are performed.

```bash
paster --plugin=ckanext-switzerland ogdch cleanup_harvestjobs [{source_id}] [--keep={n}}] [--dryrun] -c /var/www/ckan/development.ini
```

### Harvester Storage Adapters
Currently, CKAN-Switzerland support two types of remote storages for harvesting data:
- FTP
- AWS S3 Buckets

## Configurations

There are two types of configuration: the harvester configuration that comes from the Database, and the storage configuration, this comes from the `.ini` configuration file of CKAN.

### The storage configuration
Located in the common `ini` file of CKAN, we can there configure accesses to the different storage. 

For a FTP, the configuration looks like
```ini
ckan.ftp.mainserver.username = TESTUSER
ckan.ftp.mainserver.password = TESTPASS
ckan.ftp.mainserver.keyfile =
ckan.ftp.mainserver.host = ftp-secure.liip.ch
ckan.ftp.mainserver.port = 990
ckan.ftp.mainserver.localpath = /tmp/ftpharvest/tests/
ckan.ftp.mainserver.remotedirectory = /
```

Where the part of the key located after the ftp is considered as the storage identifier. In other words, all the properties for the FTP server identified as `mainserver` will all start with `ckan.ftp.mainserver`

A FTP configuration requires the following properties :

- `username` : the user name used to connect to the FTP
- `password` : the password, in clear, associated to the user
- `keyfile` : the path of the key file that would be used instead of username/password to connect
- `host` : the DNS name, or IP Address, of the FTP server
- `port` : the port on which to connect to the FTP server
- `localpath` : the path on the server where to store temporary files during the harvest process
- `remotedirectory` : the remote directory to consider as root

For a S3, the configuration look like 
```ini
ckan.s3.main_bucket.bucket_name = test-bucket
ckan.s3.main_bucket.access_key = test-access-key
ckan.s3.main_bucket.secret_key = test-secret-key
ckan.s3.main_bucket.region_name = eu-central-1
ckan.s3.main_bucket.localpath = /tmp/s3harvest/
ckan.s3.main_bucket.remotedirectory = /a/
```

Following the same schema, the identifier of this S3 bucket will be `main_bucket`. A S3 configuration requires the following properties :

- `bucket_name` : the name of the bucket on AWS
- `access_key` : the access key provided by AWS to log in to their API (see AWS console)
- `secret_key` : the secret key associated to the `access_key (see AWS console)
- `region_name` : the AWS region where the bucket is located
- `localpath` : the path on the server where to store temporary files during the harvest process
- `remotedirectory` : the remote directory to consider as root

### The harvester configuration
This configuration is a JSON object, that can be modified in the UI, in the harvester administration. 

In this configuration, it is mandatory to specify some information in order for the harvester to connect to the correct remote storage

In order to use a FTP, the configuration should look like 

```json
{   
    "storage_adapter": "FTP",
    "ftp_server": "mainserver",
    // [...]
}
```

The newly introduced property is `storage_adapter`. It allows to specify if this harvester should use a FTP or a S3 Bucket. For legacy support reasons, if this property is not defined, the harvester will consider that it uses FTP. The property is case insensitive

For a FTP server, the property `ftp_server` is mandatory. If the property is not set, an exception will be raised.


In order to use a S3, the configuration should look like
```json
{   
    "storage_adapter": "S3",
    "bucket": "main_bucket",
    // [...]
}
```
For a S3 server, the property `bucket` is mandatory. If the property is not set, an exception will be raised.

### Validation
The `StorageAdapterBase` holds the logic for loading and validating the configuration. This logic is able to verify that a certain property is present, read the value, convert the value to the given type (eg: a port number is an `int`) and check some constraints on the value (eg: the `port` should be greater than 0).

Each Storage Adapter is responsible to define the configuration properties it needs, and define the type, the name, the constraints... This is done through the class `ConfigKey`. This class allows to define the name of the configuration property, its type, if it's a mandatory configuration, a validation function and a custom message.

## How it works

The harvester will receive its configuration from the Database. This configuration will be passed to the `StorageAdapterFactory`. This object has for sole responsibility to read the configuration, and decide which Storage Adapter to instantiate. 

THe two possibilities are `S3StorageAdapter` or `FTPStorageAdapter`. Both classes extends the `StorageAdapterBase` class. In this base class, designed as an abstract class (meaning all methods are there, and the ones that have to be reimplement just throw `NotImplementedException`), are located the method specific to the interactions with the storage itself, but also some common function to manage the local folder used by the harvesters during the harvest process.

Each implementation (`S3StorageAdapter` and `FTPStorageAdapter`), are responsible to get their storage configuration, from the storage identifier received from the harvester configuration. Each implementation is also unit tested, see respectively `TestS3StorageAdapter` and `TestFTPStorageAdapter` classes.
