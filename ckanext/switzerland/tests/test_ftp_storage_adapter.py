"""Tests for the ckanext.switzerland.ftp_helper.py"""

import logging
import os
import shutil
import unittest

from ckan import model
from mock import patch
from testfixtures import Replace

from ckanext.switzerland.harvester.exceptions.storage_adapter_configuration_exception import (
    StorageAdapterConfigurationException,
)

# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.ftp_storage_adapter import FTPStorageAdapter

from .helpers.mock_config_resolver import MockConfigResolver

# -----------------------------------------------------------------------

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
log = logging.getLogger(__name__)

CONFIG_SECTION = "app:main"


class TestFTPStorageAdapter(unittest.TestCase):
    ini_file_path = os.path.join(__location__, "config", "valid.ini")
    invalid_ini_file_path = os.path.join(__location__, "config", "invalid.ini")
    tmpfolder = "/tmp/ftpharvest/tests/"
    ftp = None
    ckan_config_resolver = None
    config = {
        "ftp_server": "mainserver",
        "environment": "Test",
        "folder": "DiDok",
        "dataset": "DiDok",
        "max_resources": 30,
        "filter_regex": ".*\\.xls",
        "resource_regex": "\\d{8}-Ist-File\\.xls",
        "ist_file": True,
    }

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

    def __build_tested_object__(self, remote_dir):
        self.ckan_config_resolver = MockConfigResolver(
            self.ini_file_path, CONFIG_SECTION
        )
        return FTPStorageAdapter(self.ckan_config_resolver, self.config, remote_dir)

    def __build_tested_object_with_wrong_config__(self, remote_dir):
        self.ckan_config_resolver = MockConfigResolver(
            self.invalid_ini_file_path, CONFIG_SECTION
        )
        return FTPStorageAdapter(self.ckan_config_resolver, self.config, remote_dir)

    def test_FTPStorageAdapter__init__(self):
        """FTPStorageAdapter class correctly stores the ftp configuration from the ckan
        config
        """
        remotefolder = "/test/"
        ftph = self.__build_tested_object__(remotefolder)

        self.assertEqual(ftph._config["username"], "TESTUSER")
        self.assertEqual(ftph._config["password"], "TESTPASS")
        self.assertEqual(ftph._config["host"], "ftp-secure.sbb.ch")
        self.assertEqual(ftph._config["port"], 990)
        self.assertEqual(ftph._config["remotedirectory"], "/")
        self.assertEqual(ftph._config["localpath"], "/tmp/ftpharvest/tests/")

        self.assertEqual(ftph.remote_folder, remotefolder.rstrip("/"))

        assert os.path.exists(ftph._config["localpath"])

    def test_get_top_folder(self):
        foldername = "ftp-secure.sbb.ch:990"
        ftph = self.__build_tested_object__("/test/")
        self.assertEqual(foldername, ftph.get_top_folder())

    def test_mkdir_p(self):
        ftph = self.__build_tested_object__("/test/")
        ftph._mkdir_p(self.tmpfolder)
        assert os.path.exists(self.tmpfolder)

    def test_create_local_dir(self):
        ftph = self.__build_tested_object__("/test/")
        ftph.create_local_dir(self.tmpfolder)
        assert os.path.exists(self.tmpfolder)

    # FTP tests -----------------------------------------------------------

    @patch("ftplib.FTP_TLS", autospec=True)
    def test_connect(self, MockFTP_TLS):
        # get ftplib instance
        mock_ftp = MockFTP_TLS.return_value

        # run
        ftph = self.__build_tested_object__("/")
        ftph._connect()

        # constructor was called
        vars = {
            "host": self.ckan_config_resolver.get("ckan.ftp.mainserver.host", ""),
            "username": self.ckan_config_resolver.get(
                "ckan.ftp.mainserver.username", ""
            ),
            "password": self.ckan_config_resolver.get(
                "ckan.ftp.mainserver.password", ""
            ),
        }
        MockFTP_TLS.assert_called_with(vars["host"], vars["username"], vars["password"])

        # login method was called
        # self.assertTrue(mock_ftp.login.called)
        # prot_p method was called
        self.assertTrue(mock_ftp.prot_p.called)

    @patch("ftplib.FTP", autospec=True)
    @patch("ftplib.FTP_TLS", autospec=True)
    def test_connect_sets_ftp_port(self, MockFTP_TLS, MockFTP):
        # run
        ftph = self.__build_tested_object__("/")
        ftph._connect()
        # the port was changed by the _connect method
        self.assertEqual(
            MockFTP.port,
            int(self.ckan_config_resolver.get("ckan.ftp.mainserver.port", False)),
        )

    @patch("ftplib.FTP", autospec=True)
    @patch("ftplib.FTP_TLS", autospec=True)
    def test_disconnect(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # connect
        ftph = self.__build_tested_object__("/")
        ftph._connect()
        # disconnect
        ftph._disconnect()
        # quit was called
        self.assertTrue(mock_ftp_tls.quit.called)

    @patch("ftplib.FTP", autospec=True)
    @patch("ftplib.FTP_TLS", autospec=True)
    def test_cdremote(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # connect
        ftph = self.__build_tested_object__("/")
        ftph._connect()
        ftph.cdremote("/foo/")
        self.assertTrue(mock_ftp_tls.cwd.called)
        mock_ftp_tls.cwd.assert_called_with("/foo/")

    @patch("ftplib.FTP", autospec=True)
    @patch("ftplib.FTP_TLS", autospec=True)
    def test_cdremote_default_folder(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # connect
        ftph = self.__build_tested_object__("/")
        ftph._connect()
        ftph.cdremote()
        self.assertTrue(mock_ftp_tls.cwd.called)
        remotefolder = self.ckan_config_resolver.get(
            "ckan.ftp.mainserver.remotedirectory", False
        )
        self.assertEqual(remotefolder, ftph._config["remotedirectory"])
        mock_ftp_tls.cwd.assert_called_with("")

    @patch("ftplib.FTP", autospec=True)
    @patch("ftplib.FTP_TLS", autospec=True)
    def test_enter_FTPStorageAdapter(self, MockFTP_TLS, MockFTP):
        # get ftplib instance
        mock_ftp_tls = MockFTP_TLS.return_value
        # run test
        with self.__build_tested_object__("/hello/"):
            pass
        # check results
        self.assertTrue(mock_ftp_tls.cwd.called)
        mock_ftp_tls.cwd.assert_called_with("/hello")
        # class was instantiated with the correct values
        vars = {
            "host": self.ckan_config_resolver.get("ckan.ftp.mainserver.host", ""),
            "username": self.ckan_config_resolver.get(
                "ckan.ftp.mainserver.username", ""
            ),
            "password": self.ckan_config_resolver.get(
                "ckan.ftp.mainserver.password", ""
            ),
        }
        MockFTP_TLS.assert_called_with(vars["host"], vars["username"], vars["password"])
        # prot_p method was called
        self.assertTrue(mock_ftp_tls.prot_p.called)
        # quit was called
        self.assertTrue(mock_ftp_tls.quit.called)

    # spec
    class FTP_TLS:
        host = ""
        username = None
        password = None

        def __init__(self, host="", username=None, password=None):
            self.host = host
            self.username = username
            self.password = password

        def prot_p(self):
            return "Connected"

        def nlst(self, folder=None):
            if folder == "/empty/":
                return []
            else:
                return [
                    ".",
                    "..",
                    "filea.txt",
                    "fileb.zip",
                    "uploading.tmp",
                    "stilluploading.TMP",
                ]

        def cwd(self, folder=None):
            return "cwd into %s" % str(folder)

        def quit(self):
            return "Disconnected"

        def retrbinary(self, remotepath, filepointer):
            return (remotepath, filepointer)

    @patch("ftplib.FTP", autospec=True)
    def test_get_remote_dirlist(self, MockFTP):
        # mock ftplib.FTP_TLS
        with Replace("ftplib.FTP_TLS", self.FTP_TLS):
            # run test
            with self.__build_tested_object__("/") as ftph:
                # get directory listing
                dirlist = ftph.get_remote_dirlist("/myfolder/")
        # check results
        # a filtered directory list was returned
        self.assertEqual(dirlist, ["filea.txt", "fileb.zip"])

    @patch("ftplib.FTP", autospec=True)
    def test_get_local_dirlist(self, MockFTP):
        with Replace("ftplib.FTP_TLS", self.FTP_TLS):
            with self.__build_tested_object__("/") as ftph:
                dirlist = ftph.get_local_dirlist(
                    localpath=os.path.join(__location__, "fixtures", "testdir")
                )
        self.assertEqual(type(dirlist), list)
        self.assertEqual(len(dirlist), 3)

    @patch("ftplib.FTP", autospec=True)
    def test_is_empty_dir(self, MockFTP):
        # mock ftplib.FTP_TLS
        with Replace("ftplib.FTP_TLS", self.FTP_TLS):
            # run test
            with self.__build_tested_object__("/") as ftph:
                # get directory listing
                num = ftph.is_empty_dir("/empty/")
        # check results
        self.assertEqual(num, 0)

    # TODO
    # @patch('ftplib.FTP', autospec=True)
    # def test_is_nonempty_dir(self, MockFTP):
    #     # mock ftplib.FTP_TLS
    #     with Replace('ftplib.FTP_TLS', self.FTP_TLS):
    #         # run test
    #         with self.__build_tested_object__('/') as ftph:
    #             # get directory listing
    #             num = ftph.is_empty_dir('/nonemptydir/')
    #     # check results
    #     self.assertTrue(num > 0)

    @patch("ftplib.FTP", autospec=True)
    def test_fetch(self, MockFTP):
        filename = "foo.txt"
        testfile = "/tmp/foo.txt"
        # mock ftplib.FTP_TLS
        with Replace("ftplib.FTP_TLS", self.FTP_TLS):
            # connect
            with self.__build_tested_object__("/") as ftph:
                ftph._connect()
                # fetch remote file
                arg1, arg2 = ftph.fetch(filename, localpath=testfile)
        # tests
        self.assertEqual(arg1, "RETR %s" % filename)
        log.debug(arg2)
        self.assertEqual(str(arg2.__class__.__name__), "builtin_function_or_method")

    @patch("ftplib.FTP", autospec=True)
    @patch("ftplib.FTP_TLS", autospec=True)
    def test_unzip(self, MockFTP_TLS, MockFTP):
        currpath = os.path.dirname(os.path.realpath(__file__))
        # run test
        with self.__build_tested_object__("/") as ftph:
            num = ftph.unzip(os.path.join(currpath, "fixtures/zip/my.zip"))
        # check results
        self.assertEqual(num, 2)
        # cleanup
        for filename in ["file1.txt", "file2.txt"]:
            try:
                os.remove(os.path.join(currpath, "fixtures/zip/%s" % filename))
            except OSError:
                pass

    def test_validate_config_with_invalid_config_then_error(self):
        self.assertRaises(
            StorageAdapterConfigurationException,
            self.__build_tested_object_with_wrong_config__,
            "/",
        )

    def test_validate_config_with_valid_config_then_no_error(self):
        self.__build_tested_object__("/")

        assert True
