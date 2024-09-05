ckanext-switzerland
===================

CKAN extension for DCAT-AP Switzerland, templates and different plugins including FTP- and S3-Harvester for [opentransportdata.swiss](https://opentransportdata.swiss).

## Requirements

- CKAN 2.8+
- ckanext-fluent
- ckanext-harvest
- ckanext-scheming

## Installation

To install ckanext-switzerland:

1. Activate your CKAN virtual environment, for example:
     ```
     . /usr/lib/ckan/default/bin/activate
     ```
3. Install the ckanext-switzerland Python package into your virtual environment:
     ```
     pip install ckanext-switzerland
     ```
4. Add ``switzerland`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

5. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:
     ```
     sudo service apache2 reload
     ```
   
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

## Schema

We use [ckanext-scheming](https://github.com/ckan/ckanext-scheming) to define our schemas for datasets, groups and
organizations. The schemas are defined in the following files:

- ckanext/switzerland/dcat-ap-switzerland_scheming.json (dataset schema)
- ckanext/switzerland/multilingual_group_scheming.json
- ckanext/switzerland/multilingual_organization_scheming.json

**NB**: If you update the dataset schema to add any fields that are not simple strings, you will also need to update
the `OgdchPackagePlugin.before_dataset_index` method to convert the value of those fields to strings, so that SOLR
can consume our datasets and index them properly.

## FTP- and S3-Harvester 

Currently, CKAN-Switzerland support two types of remote storages for harvesting data:
- FTP/SFTP storage 
- AWS S3 Buckets

### Configurations

There are two types of configuration: the _harvester configuration_ that comes from the Database, 
and the _storage configuration_, this comes from the `.ini` configuration file of CKAN.

#### The storage configuration

Located in the common `ini` file of CKAN, and is used to configure accesses to the different storage. 


A _FTP configuration_ requires the following properties :

- `username` : the user name used to connect to the FTP
- `password` : the password, in clear, associated to the user
- `keyfile` : the path of the key file that would be used instead of username/password to connect
- `host` : the DNS name, or IP Address, of the FTP server
- `port` : the port on which to connect to the FTP server
- `localpath` : the path on the server where to store temporary files during the harvest process
- `remotedirectory` : the remote directory to consider as root

An example of FTP storage configuration:
```ini
ckan.ftp.mainserver.username = TESTUSER
ckan.ftp.mainserver.password = TESTPASS
ckan.ftp.mainserver.keyfile =
ckan.ftp.mainserver.host = ftp-secure.liip.ch
ckan.ftp.mainserver.port = 990
ckan.ftp.mainserver.localpath = /tmp/ftpharvest/tests/
ckan.ftp.mainserver.remotedirectory = /
```

Where the part of the key located after the ftp is considered as the storage identifier. 
For example, all the properties for the FTP server identified as `mainserver` will all start with `ckan.ftp.mainserver`

 
A _S3 configuration_ requires the following properties :

- `bucket_name` : the name of the bucket on AWS
- `access_key` : the access key provided by AWS to log in to their API (see AWS console)
- `secret_key` : the secret key associated to the `access_key` (see AWS console)
- `region_name` : the AWS region where the bucket is located
- `localpath` : the path on the server where to store temporary files during the harvest process
- `remotedirectory` : the remote directory to consider as root

An example of S3 Bucket storage configuration:
```ini
ckan.s3.main_bucket.bucket_name = test-bucket
ckan.s3.main_bucket.access_key = test-access-key
ckan.s3.main_bucket.secret_key = test-secret-key
ckan.s3.main_bucket.region_name = eu-central-1
ckan.s3.main_bucket.localpath = /tmp/s3harvest/
ckan.s3.main_bucket.remotedirectory = /a/
```
Following the same schema, the identifier of this S3 bucket will be `main_bucket`.

#### The harvester configuration
This configuration is a JSON object, that can be modified in the UI, in the harvester administration. 

In this configuration, it is mandatory to specify some information in order for the harvester
to connect to the correct remote storage.

In order to use a FTP Harvester, the configuration should look like:

```json
{   
    "storage_adapter": "FTP",
    "ftp_server": "mainserver",
    // [...]
}
```

`storage_adapter` allows to specify which harvester type should be used a FTP or a S3 Bucket. 
For legacy support reasons, if this property is not defined, the harvester will consider that it uses FTP. 
The property is case insensitive.

For a FTP server, the property `ftp_server` is mandatory. 
If the property is not set, an exception will be raised.


In order to use a S3 Harvester, the configuration should look like

```json
{   
    "storage_adapter": "S3",
    "bucket": "main_bucket",
    // [...]
}
```
For a S3 server, the property `bucket` is mandatory. 
If the property is not set, an exception will be raised.

#### Validation
The `StorageAdapterBase` holds the logic for loading and validating the configuration. 
This logic is able to verify that a certain property is present, 
read the value, convert the value to the given type (eg: a port number is an `int`) 
and check some constraints on the value (eg: the `port` should be greater than 0).

Each Storage Adapter is responsible to define the configuration properties it needs, 
and define the type, the name, the constraints.
This is done through the class `ConfigKey`. This class allows to define the name of the configuration property, 
its type, if it's a mandatory configuration, a validation function and a custom message.

### How it works

The harvester will receive its configuration from the Database. 
This configuration will be passed to the `StorageAdapterFactory`. 
This object has for sole responsibility to read the configuration, and decide which Storage Adapter to instantiate. 

THe two possibilities are `S3StorageAdapter` or `FTPStorageAdapter`. 
Both classes extends the `StorageAdapterBase` class. 
In this base class, designed as an abstract class (meaning all methods are there, 
and the ones that have to be reimplement just throw `NotImplementedException`), 
are located the method specific to the interactions with the storage itself, 
but also some common function to manage the local folder used by the harvesters during the harvest process.

Each implementation (`S3StorageAdapter` and `FTPStorageAdapter`), are responsible to get their storage configuration, 
from the storage identifier received from the harvester configuration. 
Each implementation is also unit tested, see respectively `TestS3StorageAdapter` and `TestFTPStorageAdapter` classes.
