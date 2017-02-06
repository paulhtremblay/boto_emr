#!/bin/bash
set -e
sudo pip install boto3 
cd ~/
aws s3 cp s3://paulhtremblay/vim.tar.gz .
tar -xvf vim.tar.gz 
mkdir .swap
mkdir .backup
mkdir .undo
