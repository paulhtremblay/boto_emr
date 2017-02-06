import unittest
import boto_emr.parse_marc as parse_marc
import pprint
pp = pprint.PrettyPrinter(indent = 4)

class TestParseMarc(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_txt1 = """
WARC/1.0
WARC-Type: warcinfo
WARC-Date: 2016-12-08T13:00:23Z
WARC-Record-ID: <urn:uuid:f609f246-df68-46ef-a1c5-2f66e833ffd6>
Content-Length: 344
Content-Type: application/warc-fields
WARC-Filename: CC-MAIN-20161202170900-00000-ip-10-31-129-80.ec2.internal.warc.gz

robots: classic
hostname: ip-10-31-129-80.ec2.internal
software: Nutch 1.6 (CC)/CC WarcExport 1.0
isPartOf: CC-MAIN-2016-50
operator: CommonCrawl Admin
description: Wide crawl of the web for November 2016
publisher: CommonCrawl
format: WARC File Format 1.0
conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf


WARC/1.0
WARC-Type: request
WARC-Date: 2016-12-02T17:54:09Z
WARC-Record-ID: <urn:uuid:cc7ddf8b-4646-4440-a70a-e253818cf10b>
Content-Length: 220
Content-Type: application/http; msgtype=request
WARC-Warcinfo-ID: <urn:uuid:f609f246-df68-46ef-a1c5-2f66e833ffd6>
WARC-IP-Address: 217.197.115.133
WARC-Target-URI: http://1018201.vkrugudruzei.ru/blog/

GET /blog/ HTTP/1.0
Host: 1018201.vkrugudruzei.ru
Accept-Encoding: x-gzip, gzip, deflate
User-Agent: CCBot/2.0 (http://commoncrawl.org/faq/)
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8



WARC/1.0
WARC-Type: response
WARC-Date: 2016-12-02T17:54:09Z
WARC-Record-ID: <urn:uuid:4c5e6d1a-e64f-4b6e-8101-c5e46feb84a0>
Content-Length: 577
Content-Type: application/http; msgtype=response
WARC-Warcinfo-ID: <urn:uuid:f609f246-df68-46ef-a1c5-2f66e833ffd6>
WARC-Concurrent-To: <urn:uuid:cc7ddf8b-4646-4440-a70a-e253818cf10b>
WARC-IP-Address: 217.197.115.133
WARC-Target-URI: http://1018201.vkrugudruzei.ru/blog/
WARC-Payload-Digest: sha1:Y4TZFLB6UTXHU4HUVONBXC5NZQW2LYMM
WARC-Block-Digest: sha1:3J7HHBMWTSC7W53DDB7BHTUVPM26QS4B
 """

    def test_parse(self):
        x = parse_marc.parse(self.test_txt1)
        pp.pprint(x)

if __name__ == '__main__':
    unittest.main()
