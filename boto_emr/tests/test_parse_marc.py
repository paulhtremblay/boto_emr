import unittest
import boto_emr.parse_marc as parse_marc
import pprint
pp = pprint.PrettyPrinter(indent = 4)

def _record_parser(path):
    with open(path, 'r') as read_obj:
        empty_lines = 0
        my_buffer = ''
        line = 'init'
        while line:
            line = read_obj.readline()
            if line.strip() == '':
                empty_lines += 1
            if empty_lines == 3:
                yield my_buffer.strip()
                my_buffer = ''
                empty_lines = 0
            my_buffer += line

def record_parser(text):
    fields = text.split('\n\n\n')
    return [x.strip() for x in fields]


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

HTTP/1.1 302 Found
Date: Fri, 02 Dec 2016 17:14:35 GMT
Content-Type: text/html; charset=utf-8
Content-Length: 159
Connection: close
Cache-Control: no-cache
Pragma: no-cache
Expires: -1
Location: http://1018201.vkrugudruzei.ru/x/blog/all/
Server: Microsoft-IIS/8.0
X-AspNetMvc-Version: 5.1
X-AspNet-Version: 4.0.30319
X-Powered-By: ASP.NET
X-UA-Compatible: IE=edge,chrome=1
X-Frame-Options: SAMEORIGIN

<html><head><title>Object moved</title></head><body>
<h2>Object moved to <a href="http://1018201.vkrugudruzei.ru/x/blog/all/">here</a>.</h2>
</body></html>


WARC/1.0
WARC-Type: metadata
WARC-Date: 2016-12-02T17:54:09Z
WARC-Record-ID: <urn:uuid:638faf89-13b3-4a0a-96b6-f43430911591>
Content-Length: 45
Content-Type: application/warc-fields
WARC-Warcinfo-ID: <urn:uuid:f609f246-df68-46ef-a1c5-2f66e833ffd6>
WARC-Concurrent-To: <urn:uuid:4c5e6d1a-e64f-4b6e-8101-c5e46feb84a0>
WARC-Target-URI: http://1018201.vkrugudruzei.ru/blog/

fetchTimeMs: 1389
robotsCrawlDelay: 3000



WARC/1.0
WARC-Type: request
WARC-Date: 2016-12-02T18:06:46Z
WARC-Record-ID: <urn:uuid:fd081882-15e6-41f7-82a4-1271fcbe4ef3>
Content-Length: 520
Content-Type: application/http; msgtype=request
WARC-Warcinfo-ID: <urn:uuid:f609f246-df68-46ef-a1c5-2f66e833ffd6>
WARC-IP-Address: 185.65.247.9
WARC-Target-URI: http://109.te.ua/%D0%BC%D1%96%D1%81%D1%82%D0%BE_%D0%A2%D0%B5%D1%80%D0%BD%D0%BE%D0%BF%D1%96%D0%BB%D1%8C/%D1%88%D1%83%D0%BA%D0%B0%D1%82%D0%B8_%D0%BE%D1%80%D0%B3%D0%B0%D0%BD%D1%96%D0%B7%D0%B0%D1%86%D1%96%D1%8E/%D0%9B%D0%BE%D0%BC%D0%B1%D0%B0%D1%80%D0%B4_%D0%97%D0%BB%D1%83%D0%BA%D0%B8_%D0%B1%D0%B5%D0%B7_%D0%BD%D0%BE%D0%BC%D0%B5%D1%80%D0%B0

GET /%D0%BC%D1%96%D1%81%D1%82%D0%BE_%D0%A2%D0%B5%D1%80%D0%BD%D0%BE%D0%BF%D1%96%D0%BB%D1%8C/%D1%88%D1%83%D0%BA%D0%B0%D1%82%D0%B8_%D0%BE%D1%80%D0%B3%D0%B0%D0%BD%D1%96%D0%B7%D0%B0%D1%86%D1%96%D1%8E/%D0%9B%D0%BE%D0%BC%D0%B1%D0%B0%D1%80%D0%B4_%D0%97%D0%BB%D1%83%D0%BA%D0%B8_%D0%B1%D0%B5%D0%B7_%D0%BD%D0%BE%D0%BC%D0%B5%D1%80%D0%B0 HTTP/1.0
Host: 109.te.ua
Accept-Encoding: x-gzip, gzip, deflate
User-Agent: CCBot/2.0 (http://commoncrawl.org/faq/)
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8



WARC/1.0
WARC-Type: response
WARC-Date: 2016-12-02T18:06:46Z
WARC-Record-ID: <urn:uuid:128708be-4720-49d9-9d4f-5179a73d6415>
Content-Length: 868
Content-Type: application/http; msgtype=response
WARC-Warcinfo-ID: <urn:uuid:f609f246-df68-46ef-a1c5-2f66e833ffd6>
WARC-Concurrent-To: <urn:uuid:fd081882-15e6-41f7-82a4-1271fcbe4ef3>
WARC-IP-Address: 185.65.247.9
WARC-Target-URI: http://109.te.ua/%D0%BC%D1%96%D1%81%D1%82%D0%BE_%D0%A2%D0%B5%D1%80%D0%BD%D0%BE%D0%BF%D1%96%D0%BB%D1%8C/%D1%88%D1%83%D0%BA%D0%B0%D1%82%D0%B8_%D0%BE%D1%80%D0%B3%D0%B0%D0%BD%D1%96%D0%B7%D0%B0%D1%86%D1%96%D1%8E/%D0%9B%D0%BE%D0%BC%D0%B1%D0%B0%D1%80%D0%B4_%D0%97%D0%BB%D1%83%D0%BA%D0%B8_%D0%B1%D0%B5%D0%B7_%D0%BD%D0%BE%D0%BC%D0%B5%D1%80%D0%B0
WARC-Payload-Digest: sha1:V2CUZCH5H7TQHWN6TRFOWDLBCALHFXHQ
WARC-Block-Digest: sha1:XE6XMFBU5FMVJPCO7D4WCQHX2T2UNLKV

HTTP/1.1 301 Moved Permanently
Content-Length: 412
Location: https://109.te.ua/ÃÂ¼ÃÂÃÂÃÂÃÂ¾_ÃÂ¢ÃÂµÃÂÃÂ½ÃÂ¾ÃÂ¿ÃÂÃÂ»ÃÂ/ÃÂÃÂÃÂºÃÂ°ÃÂÃÂ¸_ÃÂ¾ÃÂÃÂ³ÃÂ°ÃÂ½ÃÂÃÂ·ÃÂ°ÃÂÃÂÃÂ/ÃÂÃÂ¾ÃÂ¼ÃÂ±ÃÂ°ÃÂÃÂ´_ÃÂÃÂ»ÃÂÃÂºÃÂ¸_ÃÂ±ÃÂµÃÂ·_ÃÂ½ÃÂ¾ÃÂ¼ÃÂµÃÂÃÂ°
Connection: close
Server: Apache/2.2.22 (Ubuntu)
Vary: Accept-Encoding
Date: Fri, 02 Dec 2016 18:07:15 GMT
Content-Type: text/html; charset=iso-8859-1

<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">
<html><head>
<title>301 Moved Permanently</title>
</head><body>
<h1>Moved Permanently</h1>
<p>The document has moved <a href="https://109.te.ua/Ð¼ÑÑÑÐ¾_Ð¢ÐµÑÐ½Ð¾Ð¿ÑÐ»Ñ/ÑÑÐºÐ°ÑÐ¸_Ð¾ÑÐ³Ð°Ð½ÑÐ·Ð°ÑÑÑ/ÐÐ¾Ð¼Ð±Ð°ÑÐ´_ÐÐ»ÑÐºÐ¸_Ð±ÐµÐ·_Ð½Ð¾Ð¼ÐµÑÐ°">here</a>.</p>
<hr>
<address>Apache/2.2.22 (Ubuntu) Server at 109.te.ua Port 80</address>
</body></html>


 """

    def test_parse(self):
        path = '/tmp/warc_ex'
        fields = record_parser(self.test_txt1)
        pp.pprint([parse_marc.parse_record(x) for x in fields])
        """
        x = parse_marc.parse(self.test_txt1)
        self.assertTrue(len(x) == 3)
        self.assertTrue(len(list(x[0].keys())) == 25)
        """


if __name__ == '__main__':
    unittest.main()
