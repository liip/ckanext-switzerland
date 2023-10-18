import configparser


class MockConfigResolver(object):
    _config = None
    _section = ""

    def __init__(self, ini_file_path=None, section="DEFAULT"):
        self._config = configparser.ConfigParser()
        if ini_file_path:
            self._config.read(ini_file_path)

        self._section = section

    def get(self, key, default_value):
        try:
            return self._config[self._section][key]
        except KeyError:
            return default_value
