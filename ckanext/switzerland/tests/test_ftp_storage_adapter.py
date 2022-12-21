# encoding: utf-8

'''Tests for the ckanext.switzerland.ftp_helper.py '''

import unittest
import os
import shutil
import ftplib

import logging
log = logging.getLogger(__name__)

from nose.tools import assert_equal, raises, nottest, with_setup
from mock import patch, Mock, MagicMock, PropertyMock
from testfixtures import Replace

from pylons import config as ckanconf

# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.ftp_storage_adapter import FTPStorageAdapter
# -----------------------------------------------------------------------



class TestFTPStorageAdapter(unittest.TestCase):

    tmpfolder = '/tmp/ftpharvest/tests/'
    ftp = None

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def setup(self):
        if os.path.exists(self.tmpfolder):
            shutil.rmtree(self.tmpfolder, ignore_errors=True)

    def teardown(self):
        # clear db
        model.repo.rebuild_db()
        # remove the tmp directory
        if os.path.exists(self.tmpfolder):
            shutil.rmtree(self.tmpfolder, ignore_errors=True)

    # ---------------------------------------------------------------------

    def test_FTPStorageAdapter__init__(self):
        """ FTPStorageAdapter class correctly stores the ftp configuration from the ckan config """
        remotefolder = '/test/'
        ftph = FTPStorageAdapter(remotefolder)

        assert_equal(ftph._config['username'], 'TESTUSER')
        assert_equal(ftph._config['password'], 'TESTPASS')
        assert_equal(ftph._config['host'], 'ftp-secure.sbb.ch')
        assert_equal(ftph._config['port'], 990)
        assert_equal(ftph._config['remotedirectory'], '/')
        assert_equal(ftph._config['localpath'], '/tmp/ftpharvest/tests/')

        assert_equal(ftph.remote_folder, remotefolder.rstrip('/'))

        assert os.path.exists(ftph._config['localpath'])

    def test_get_top_folder(self):
        foldername = "ftp-secure.sbb.ch:990"
        ftph = FTPStorageAdapter('/test/')
        assert_equal(foldername, ftph.get_top_folder())

    def test_mkdir_p(self):
        ftph = FTPStorageAdapter('/test/')
        ftph._mkdir_p(self.tmpfolder)
        assert os.path.exists(self.tmpfolder)

    def test_create_local_dir(self):
        ftph = FTPStorageAdapter('/test/')
        ftph.create_local_dir(self.tmpfolder)
        assert os.path.exists(self.tmpfolder)

    # FTP tests -----------------------------------------------------------

    @patch('ftplib.FTP_TLS', autospec=True)
    def test_connect(self, MockFTP_TLS):

        # get ftplib instance
        mock_ftp = MockFTP_TLS.return_value

        # run
        ftph = FTPStorageAdapter('/')
        ftph._connect()

        # constructor was called
        vars = {
            'host': ckanconf.get('ckan.ftp.host', ''),
            'username': ckanconf.get('ckan.ftp.username', ''),
            'password': ckanconf.get('ckan.ftp.password', ''),
        }
        MockFTP_TLS.assert_called_with(vars['host'], vars['username'], vars['password'])

        # login method was called
        # self.assertTrue(mock_ftp.login.called)
        # prot_p method was called
        self.assertTrue(mock_ftp.prot_p.called)

    @patch('ftplib.FTP', autospec=True)
    @patch('ftplib.FTP_TLS', autospec=True)
    def test_connect_sets_ftp_port(self, MockFTP_TLS, MockFTP):
        # run
        ftph = FTPStorageAdapter('/')
        ftph._connect()
        # the port was changed by the _connect method
        assert_equal(MockFTP.port, int(ckanconf.get('ckan.ftp.port', False)))

    @patch('ftplib.FTP', autospec=True)
    @patch('ftplib.FTP_TLS', autospec=True)
    def test_disconnect(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # connect
        ftph = FTPStorageAdapter('/')
        ftph._connect()
        # disconnect
        ftph._disconnect()
        # quit was called
        self.assertTrue(mock_ftp_tls.quit.called)

    @patch('ftplib.FTP', autospec=True)
    @patch('ftplib.FTP_TLS', autospec=True)
    def test_cdremote(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # connect
        ftph = FTPStorageAdapter('/')
        ftph._connect()
        ftph.cdremote('/foo/')
        self.assertTrue(mock_ftp_tls.cwd.called)
        mock_ftp_tls.cwd.assert_called_with('/foo/')

    @patch('ftplib.FTP', autospec=True)
    @patch('ftplib.FTP_TLS', autospec=True)
    def test_cdremote_default_folder(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # connect
        ftph = FTPStorageAdapter('/')
        ftph._connect()
        ftph.cdremote()
        self.assertTrue(mock_ftp_tls.cwd.called)
        remotefolder = ckanconf.get('ckan.ftp.remotedirectory', False)
        assert_equal(remotefolder, ftph._config['remotedirectory'])
        mock_ftp_tls.cwd.assert_called_with('')

    @patch('ftplib.FTP', autospec=True)
    @patch('ftplib.FTP_TLS', autospec=True)
    def test_enter_FTPStorageAdapter(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # run test
        with FTPStorageAdapter('/hello/') as ftph:
            pass
        # check results
        self.assertTrue(mock_ftp_tls.cwd.called)
        mock_ftp_tls.cwd.assert_called_with('/hello')
        # class was instantiated with the correct values
        vars = {
            'host': ckanconf.get('ckan.ftp.host', ''),
            'username': ckanconf.get('ckan.ftp.username', ''),
            'password': ckanconf.get('ckan.ftp.password', ''),
        }
        MockFTP_TLS.assert_called_with(vars['host'], vars['username'], vars['password'])
        # prot_p method was called
        self.assertTrue(mock_ftp_tls.prot_p.called)
        # quit was called
        self.assertTrue(mock_ftp_tls.quit.called)

    # spec
    class FTP_TLS:
        host = ''
        username = None
        password = None
        def __init__(self, host='', username=None, password=None):
            self.host = host
            self.username = username
            self.password = password
        def prot_p(self):
            return 'Connected'
        def nlst(self, folder=None):
            if folder == '/empty/':
                return []
            else:
                return ['.', '..', 'filea.txt', 'fileb.zip', 'uploading.tmp', 'stilluploading.TMP']
        def cwd(self, folder=None):
            return 'cwd into %s' % str(folder)
        def quit(self):
            return 'Disconnected'
        def retrbinary(self, remotepath, filepointer):
            return ( remotepath, filepointer )

    @patch('ftplib.FTP', autospec=True)
    def test_get_remote_dirlist(self, MockFTP):
        # mock ftplib.FTP_TLS
        with Replace('ftplib.FTP_TLS', self.FTP_TLS):
            # run test
            with FTPStorageAdapter('/') as ftph:
                # get directory listing
                dirlist = ftph.get_remote_dirlist('/myfolder/')
        # check results
        # a filtered directory list was returned
        assert_equal(dirlist, ['filea.txt', 'fileb.zip'])

    @patch('ftplib.FTP', autospec=True)
    def test_get_local_dirlist(self, MockFTP):
        with Replace('ftplib.FTP_TLS', self.FTP_TLS):
            with FTPStorageAdapter('/') as ftph:
                dirlist = ftph.get_local_dirlist(localpath="./ckanext/switzerland/tests/fixtures/testdir")
        assert_equal(type(dirlist), list)
        assert_equal(len(dirlist), 3)

    @patch('ftplib.FTP', autospec=True)
    def test_is_empty_dir(self, MockFTP):
        # mock ftplib.FTP_TLS
        with Replace('ftplib.FTP_TLS', self.FTP_TLS):
            # run test
            with FTPStorageAdapter('/') as ftph:
                # get directory listing
                num = ftph.is_empty_dir('/empty/')
        # check results
        assert_equal(num, 0)

    # TODO
    # @patch('ftplib.FTP', autospec=True)
    # def test_is_nonempty_dir(self, MockFTP):
    #     # mock ftplib.FTP_TLS
    #     with Replace('ftplib.FTP_TLS', self.FTP_TLS):
    #         # run test
    #         with FTPStorageAdapter('/') as ftph:
    #             # get directory listing
    #             num = ftph.is_empty_dir('/nonemptydir/')
    #     # check results
    #     self.assertTrue(num > 0)

    @patch('ftplib.FTP', autospec=True)
    def test_fetch(self, MockFTP):
        filename = 'foo.txt'
        testfile = '/tmp/foo.txt'
        # mock ftplib.FTP_TLS
        with Replace('ftplib.FTP_TLS', self.FTP_TLS):
            # connect
            with FTPStorageAdapter('/') as ftph:
                ftph._connect()
                # fetch remote file
                arg1, arg2 = ftph.fetch(filename, localpath=testfile)
        # tests
        assert_equal(arg1, 'RETR %s' % filename)
        log.debug(arg2)
        # assert_equal(str(type(arg2)), "<type 'builtin_function_or_method'>")
        assert_equal(str(arg2.__class__.__name__), "builtin_function_or_method")

    @patch('ftplib.FTP', autospec=True)
    @patch('ftplib.FTP_TLS', autospec=True)
    def test_unzip(self, MockFTP_TLS, MockFTP):
        currpath = os.path.dirname(os.path.realpath(__file__))
        # run test
        with FTPStorageAdapter('/') as ftph:
            num = ftph.unzip(os.path.join(currpath, 'fixtures/zip/my.zip'))
        # check results
        assert_equal(num, 2)
        # cleanup
        for filename in ['file1.txt', 'file2.txt']:
            try:
                os.remove(os.path.join(currpath, "fixtures/zip/%s" % filename))
            except:
                pass


