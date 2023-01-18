class StorageAdapterConfigurationException(Exception):
    def __init__(self, keys):
        message = 'The storage adapter configuration is missing the following keys: {keys}'.format(keys=keys)
        super(StorageAdapterConfigurationException, self).__init__(message)