#!/bin/bash
curl -s https://snap.stanford.edu/data/twitter-2010.txt.gz | gunzip -c | head -1000000 |  gzip  > ~/Downloads/twitter-2010-sample.txt.gz 
