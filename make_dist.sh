python setup.py sdist
aws --profile admin s3 cp dist/emr_boto-1.0.tar.gz s3://paulhtremblay/prod/emr_boto-1.0.tar.gz
