import unittest
import sys
sys.path.insert(0,'../')
import ftp_chunker

class TestFtpChunkder(unittest.TestCase):

    def test_get_paths_returns_generator(self):
        my_generator = ftp_chunker.get_paths(1901)
        next(my_generator)
        self.assertTrue(isinstance(next(my_generator)['size'], int))

    def test_make_chunks(self):
        chunk_size = 1000000 * 1000 #10,000k
        x = ftp_chunker.make_chunks(chunk_size)
        for i in x:
            d = {}
            for j in i:
                d[j['year']] = True
            print(sorted(list(d.keys())))


if __name__ == '__main__':
    unittest.main()
