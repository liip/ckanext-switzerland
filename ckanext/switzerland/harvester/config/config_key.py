class ConfigKey():
    name = None
    type = str
    is_mandatory = False
    custom_error_message = None

    def __init__(self, name, type = str, is_mandatory = False, is_valid_func = None, custom_error_message = None ):
        self.name = name

        if type != str:
            self.type = type

        if is_mandatory:
            self.is_mandatory = is_mandatory

        if custom_error_message is not None:
            self.custom_error_message = custom_error_message
        
        if is_valid_func is not None:
            self.is_valid = is_valid_func

    def is_valid(self, value):
        return True

