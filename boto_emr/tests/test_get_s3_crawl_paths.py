import unittest
import boto_emr.get_s3_common_crawl_paths as crawl_paths
import pprint
pp = pprint.PrettyPrinter(indent = 4)

class TestGetS3CrawlPaths(unittest.TestCase):

    @unittest.skip("time")
    def test_list_objects_returns(self):
        l = crawl_paths.list_objects(bucket_name = 'paulhtremblay', prefix = 'test/')
        self.assertTrue(len(l) > 0)

    @unittest.skip("time")
    def test_list_objects_returns_empty_list_for_bad_prefix(self):
        l = crawl_paths.list_objects(bucket_name = 'paulhtremblay', prefix = '1ebc325')
        self.assertTrue(len(l) == 0)

    @unittest.skip("time")
    def test_get_segments_returns_list_greater_than_0(self):
        l = crawl_paths.get_segments()
        self.assertTrue(len(l) > 0)

    def test_get_crawl_paths_returns_list_greater_than_0(self):
        l = crawl_paths.get_crawl_paths(the_type = 'crawldiagnostics',
                recursion_limit = 10)
        pp.pprint(l)


if __name__ == '__main__':
    unittest.main()
