""" Mock CKAN server """

import os

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import TLS_FTPHandler
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

import logging
log = logging.getLogger(__name__)


# CERTFILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "keycert.pem"))


class MockFTPServer():

    server = None

    def __init__(self, config, user=None):

        log.debug("Starting FTP server: %s" % str(config))

        if self.server:
            self.teardown()

        authorizer = DummyAuthorizer()
        if user:
            authorizer.add_user(**user)
        else:
            authorizer.add_anonymous('.')

        handler = FTPHandler

        # handler.certfile = CERTFILE
        handler.authorizer = authorizer
        # requires SSL for both control and data channel
        # handler.tls_control_required = True
        # handler.tls_data_required = True

        self.server = FTPServer(config, handler)
        # self.server.serve_forever(blocking=False)
        # raise Exception(str(self.server))

    def teardown(self):

        self.server.close_all()
        self.server = None

