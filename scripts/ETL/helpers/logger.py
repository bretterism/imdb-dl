import boto3
import csv
import datetime
import logging
import os
import time

from subprocess import check_output

def logMessage(msg, printMsg=False):
	logMsg = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " -- "
	logMsg += msg
	logMsg += "\n"

	if printMsg:
		print(logMsg)

	return logMsg

def connect(db, region, endpointUrl):
	return boto3.resource(db, region_name=region, endpoint_url=endpointUrl)

def wc(filename):
    return int(check_output(["wc", "-l", filename]).split()[0])