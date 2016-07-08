""" Mock CKAN server """

PORT = 990
HOST = ''

import os

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import TLS_FTPHandler
from pyftpdlib.servers import FTPServer

CERTFILE = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        "keycert.pem"))


class FTPServer(object):

    server = None

    def setupFTPServer(config, user=None):

        authorizer = DummyAuthorizer()
        authorizer.add_user(**user)
        # authorizer.add_anonymous('.')

        handler = TLS_FTPHandler
        # handler.certfile = CERTFILE
        handler.authorizer = authorizer
        # requires SSL for both control and data channel
        handler.tls_control_required = True
        handler.tls_data_required = True

        self.server = FTPServer(config, handler)
        self.server.serve_forever()


    def teardownFTPServer():

        self.server.close_all()

