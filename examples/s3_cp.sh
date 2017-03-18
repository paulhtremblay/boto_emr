ROOT="s3://commoncrawl/crawl-data/CC-MAIN-2017-04/segments/1484560283475.86/crawldiagnostics/"
s3-dist-cp --src ${ROOT}CC-MAIN-20170116095123-00570-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs1
s3-dist-cp --src  ${ROOT}CC-MAIN-20170116095123-00571-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs2
s3-dist-cp --src  ${ROOT}CC-MAIN-20170116095123-00572-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs3
s3-dist-cp --src  ${ROOT}CC-MAIN-20170116095123-00573-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs4
s3-dist-cp --src  ${ROOT}CC-MAIN-20170116095123-00574-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs5
s3-dist-cp --src  ${ROOT}CC-MAIN-20170116095123-00575-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs6
s3-dist-cp --src  ${ROOT}CC-MAIN-20170116095123-00576-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs7
s3-dist-cp --src  ${ROOT}CC-MAIN-20170116095123-00577-ip-10-171-10-70.ec2.internal.warc.gz --dest /mnt/temp/tmpfs8

