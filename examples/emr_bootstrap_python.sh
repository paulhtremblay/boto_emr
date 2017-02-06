cd /mnt
aws s3 cp  s3://paulhtremblay/prod/emr_boto-1.0.tar.gz .
tar -xzf  emr_boto-1.0.tar.gz
cd emr_boto-1.0
sudo python3 setup.py install
