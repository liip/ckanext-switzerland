class StorageAdapterConfigurationException(Exception):
    def __init__(self, errors):
        if len(errors) == 0:
            message = "The storage adapter configuration has some unknown errors."
        else:
            message = "The storage adapter configuration has the following errors:"

        for error in errors:
            message = message + "\n\t{error}".format(error=error)

        super(StorageAdapterConfigurationException, self).__init__(message)
