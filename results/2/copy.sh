#!/bin/bash

scp -P 25210 shendc@amd172.utah.cloudlab.us:~/giza/latency.txt ./latency.1.txt
scp -P 25210 shendc@amd172.utah.cloudlab.us:~/giza/lattput.txt ./lattput.1.txt
scp -P 25211 shendc@amd172.utah.cloudlab.us:~/giza/latency.txt ./latency.2.txt
scp -P 25211 shendc@amd172.utah.cloudlab.us:~/giza/lattput.txt ./lattput.2.txt
