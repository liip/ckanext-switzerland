import datetime
from dateutil.tz import tzutc

FILES_AT_ROOT = {
    "ResponseMetadata": {
        "RequestId": "8XVDSP6S2ZR26YE9",
        "HostId": "XZ0TbGFfbC2q+T9gm27jyG8jkyzWV4oJZmKtheUZwNPD0c5/HoZnasKalnaA6Z200HDa0lJIPYQ=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "XZ0TbGFfbC2q+T9gm27jyG8jkyzWV4oJZmKtheUZwNPD0c5/HoZnasKalnaA6Z200HDa0lJIPYQ=",
            "x-amz-request-id": "8XVDSP6S2ZR26YE9",
            "date": "Wed, 21 Dec 2022 14:30:47 GMT",
            "x-amz-bucket-region": "eu-central-1",
            "content-type": "application/xml",
            "transfer-encoding": "chunked",
            "server": "AmazonS3",
        },
        "RetryAttempts": 0,
    },
    "IsTruncated": False,
    "Marker": "",
    "Contents": [
        {
            "Key": "file_01.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 52, tzinfo=tzutc()),
            "ETag": '"d5100e495ad9e4587faf8f9663677584"',
            "Size": 659119,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "file_02.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 53, tzinfo=tzutc()),
            "ETag": '"d5100e495ad9e4587faf8f9663677584"',
            "Size": 659119,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
    ],
    "Name": "bpy-odp-test",
    "Prefix": "",
    "Delimiter": "/",
    "MaxKeys": 1000,
    "CommonPrefixes": [{"Prefix": "a/"}, {"Prefix": "z/"}],
    "EncodingType": "url",
}

FILES_AT_FOLDER = {
    "ResponseMetadata": {
        "RequestId": "WR5RWMG0ANK7HBJP",
        "HostId": "RqOynGjyaE0GaP1h1TMWLYt2TFlV0AaNduBt2dGKqZpz0vgKG3lVV1rv+i4g0n6qlp9cIo/CshY=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "RqOynGjyaE0GaP1h1TMWLYt2TFlV0AaNduBt2dGKqZpz0vgKG3lVV1rv+i4g0n6qlp9cIo/CshY=",
            "x-amz-request-id": "WR5RWMG0ANK7HBJP",
            "date": "Wed, 21 Dec 2022 14:30:01 GMT",
            "x-amz-bucket-region": "eu-central-1",
            "content-type": "application/xml",
            "transfer-encoding": "chunked",
            "server": "AmazonS3",
        },
        "RetryAttempts": 0,
    },
    "IsTruncated": False,
    "Marker": "",
    "Contents": [
        {
            "Key": "a/",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 31, tzinfo=tzutc()),
            "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
            "Size": 0,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/file_03.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
            "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "Size": 418809,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/file_04.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
            "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "Size": 418809,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/a_file_05.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
            "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "Size": 418809,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
    ],
    "Name": "bpy-odp-test",
    "Prefix": "a/",
    "Delimiter": "/",
    "MaxKeys": 1000,
    "CommonPrefixes": [{"Prefix": "a/sub_a/"}],
    "EncodingType": "url",
}

ALL = {
    "ResponseMetadata": {
        "RequestId": "KKHHB7HAEM1197ST",
        "HostId": "2zVprN73/6LBDtQ3mfgG+JcWE1IMzSijOoG0JpjVi0XSz72FTD5qr+ojQgFMtZRlasAoQ34e6+0=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "2zVprN73/6LBDtQ3mfgG+JcWE1IMzSijOoG0JpjVi0XSz72FTD5qr+ojQgFMtZRlasAoQ34e6+0=",
            "x-amz-request-id": "KKHHB7HAEM1197ST",
            "date": "Wed, 21 Dec 2022 14:49:33 GMT",
            "x-amz-bucket-region": "eu-central-1",
            "content-type": "application/xml",
            "transfer-encoding": "chunked",
            "server": "AmazonS3",
        },
        "RetryAttempts": 0,
    },
    "IsTruncated": False,
    "Marker": "",
    "Contents": [
        {
            "Key": "a/",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 31, tzinfo=tzutc()),
            "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
            "Size": 0,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/file_03.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
            "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "Size": 418809,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/file_04.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
            "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "Size": 418809,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/sub_a/",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 54, 4, tzinfo=tzutc()),
            "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
            "Size": 0,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/sub_a/file_07.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()),
            "ETag": '"d4f62b395733ab379de0075f209b5aef"',
            "Size": 461428,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/sub_a/file_08.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()),
            "ETag": '"d4f62b395733ab379de0075f209b5aef"',
            "Size": 461428,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "file_01.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 52, tzinfo=tzutc()),
            "ETag": '"d5100e495ad9e4587faf8f9663677584"',
            "Size": 659119,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "file_02.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 53, tzinfo=tzutc()),
            "ETag": '"d5100e495ad9e4587faf8f9663677584"',
            "Size": 659119,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "z/",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 42, tzinfo=tzutc()),
            "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
            "Size": 0,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "z/file_05.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 49, tzinfo=tzutc()),
            "ETag": '"1f11c2cb8739d05738c1c08a111a93e5"',
            "Size": 860443,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "z/file_06.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 49, tzinfo=tzutc()),
            "ETag": '"1f11c2cb8739d05738c1c08a111a93e5"',
            "Size": 860443,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
    ],
    "Name": "bpy-odp-test",
    "Prefix": "",
    "MaxKeys": 1000,
    "EncodingType": "url",
}

ALL_AT_FOLDER = {
    "ResponseMetadata": {
        "RequestId": "BMM2NE68YHB4G9GW",
        "HostId": "bGQnwz9QCSxyoI+Nr38euRaKCptuehZlbQLj4K06AgM7biYgB1mSzs75MC70kgLaEjGyLtEjaNI=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "bGQnwz9QCSxyoI+Nr38euRaKCptuehZlbQLj4K06AgM7biYgB1mSzs75MC70kgLaEjGyLtEjaNI=",
            "x-amz-request-id": "BMM2NE68YHB4G9GW",
            "date": "Wed, 21 Dec 2022 14:52:58 GMT",
            "x-amz-bucket-region": "eu-central-1",
            "content-type": "application/xml",
            "transfer-encoding": "chunked",
            "server": "AmazonS3",
        },
        "RetryAttempts": 0,
    },
    "IsTruncated": False,
    "Marker": "",
    "Contents": [
        {
            "Key": "a/",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 31, tzinfo=tzutc()),
            "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
            "Size": 0,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/file_03.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
            "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "Size": 418809,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/file_04.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
            "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "Size": 418809,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/sub_a/",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 54, 4, tzinfo=tzutc()),
            "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
            "Size": 0,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/sub_a/file_07.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()),
            "ETag": '"d4f62b395733ab379de0075f209b5aef"',
            "Size": 461428,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
        {
            "Key": "a/sub_a/file_08.pdf",
            "LastModified": datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()),
            "ETag": '"d4f62b395733ab379de0075f209b5aef"',
            "Size": 461428,
            "StorageClass": "STANDARD",
            "Owner": {
                "ID": "eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954"
            },
        },
    ],
    "Name": "bpy-odp-test",
    "Prefix": "a/",
    "MaxKeys": 1000,
    "EncodingType": "url",
}

NO_CONTENT = {}

HEAD_FILE_AT_FOLDER = {
    "ResponseMetadata": {
        "RequestId": "N1BMBN9RRV02KCP0",
        "HostId": "Dt0lapFpP1rPL3Z9M9BdGj10IWkI8iTFBQci0bvMosJi6YaZUTkJmHeFrdANZTkx/UMIQLh8M2m1n0BzZP/2BA==",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "Dt0lapFpP1rPL3Z9M9BdGj10IWkI8iTFBQci0bvMosJi6YaZUTkJmHeFrdANZTkx/UMIQLh8M2m1n0BzZP/2BA==",
            "x-amz-request-id": "N1BMBN9RRV02KCP0",
            "date": "Wed, 21 Dec 2022 15:32:54 GMT",
            "last-modified": "Wed, 21 Dec 2022 13:53:08 GMT",
            "etag": '"0b6858a853073a7e5a3edb54a51154b1"',
            "accept-ranges": "bytes",
            "content-type": "application/pdf",
            "server": "AmazonS3",
            "content-length": "418809",
        },
        "RetryAttempts": 0,
    },
    "AcceptRanges": "bytes",
    "LastModified": datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()),
    "ContentLength": 418809,
    "ETag": '"0b6858a853073a7e5a3edb54a51154b1"',
    "ContentType": "application/pdf",
    "Metadata": {},
}

HEAD_FILE_AT_ROOT = {
    "ResponseMetadata": {
        "RequestId": "2ZYCRJQ7XK8RVGPE",
        "HostId": "5+CArigCaxO03lAWRezv5YIRXlFYtfdsuYjtNXOSpAOiNvnMUv/aO//18M4vjA4bPy7QGoNhxF4=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "5+CArigCaxO03lAWRezv5YIRXlFYtfdsuYjtNXOSpAOiNvnMUv/aO//18M4vjA4bPy7QGoNhxF4=",
            "x-amz-request-id": "2ZYCRJQ7XK8RVGPE",
            "date": "Wed, 21 Dec 2022 15:33:53 GMT",
            "last-modified": "Wed, 21 Dec 2022 13:52:52 GMT",
            "etag": '"d5100e495ad9e4587faf8f9663677584"',
            "accept-ranges": "bytes",
            "content-type": "application/pdf",
            "server": "AmazonS3",
            "content-length": "659119",
        },
        "RetryAttempts": 0,
    },
    "AcceptRanges": "bytes",
    "LastModified": datetime.datetime(2022, 12, 21, 13, 52, 52, tzinfo=tzutc()),
    "ContentLength": 659119,
    "ETag": '"d5100e495ad9e4587faf8f9663677584"',
    "ContentType": "application/pdf",
    "Metadata": {},
}

FILE_CONTENT = b"This;is;a;csv;file\n"


class BodyObjectMock:
    def read(self):
        return FILE_CONTENT


GET_OBJECT = {
    "ResponseMetadata": {
        "RequestId": "X2RVMSTRR28V1VGP",
        "HostId": "qNjPOivhpealP3A48lJcDOMaPpf6H8ewXwIn31uyxA6yc1Z/wbRHmINRl89t4oNeVqhOpsADFHQ=",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amz-id-2": "qNjPOivhpealP3A48lJcDOMaPpf6H8ewXwIn31uyxA6yc1Z/wbRHmINRl89t4oNeVqhOpsADFHQ=",
            "x-amz-request-id": "X2RVMSTRR28V1VGP",
            "date": "Wed, 21 Dec 2022 16:16:29 GMT",
            "last-modified": "Wed, 21 Dec 2022 16:13:17 GMT",
            "etag": '"9a45adfaa943bba12b0925695efd01e1"',
            "accept-ranges": "bytes",
            "content-type": "text/csv",
            "server": "AmazonS3",
            "content-length": "19",
        },
        "RetryAttempts": 0,
    },
    "AcceptRanges": "bytes",
    "LastModified": datetime.datetime(2022, 12, 21, 16, 13, 17, tzinfo=tzutc()),
    "ContentLength": 19,
    "ETag": '"9a45adfaa943bba12b0925695efd01e1"',
    "ContentType": "text/csv",
    "Metadata": {},
    "Body": BodyObjectMock(),
}
