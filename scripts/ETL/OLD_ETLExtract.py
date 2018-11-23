import boto3
import csv
import datetime
import gzip
import logging
import os
import shutil
import sys
import time
import urllib.request
from subprocess import check_output

sys.path.append('../helpers')
import helpers


# logging.basicConfig(filename='logs/downloadFiles.log', level=logging.INFO)

def downloader(dest, files):
	"""Downloads a list of files and places them in the destination folder

	file object: { 'name': string, 'url': string }

	Args:
		dest (str): Folder location where the files should be downloaded
		files (list): A list of files to download

	Returns:
		True if all files downloaded successfully
		False if any file failed
	"""
	successCount = 0
	for f in files:
		loc = dest + '/' + f['name']
		try:
			timerStart = time.time()
			res = urllib.request.urlopen(f['url'])
			with open(loc, 'wb') as localFile:

				fileSizeBytes = int(res.getheader('Content-Length'))
				fileSizeMB = fileSizeBytes / 1024.0 / 1024.0

				downloadMsg = "Downloading %s: %.2f MB" % (f['name'], fileSizeMB)
				# logging.info(helpers.logMessage(downloadMsg, True))

				blockSize = 8192
				fileSizeDL = 0
				counter = 0
				while True:
					buf = res.read(blockSize)
					if not buf:
						timerEnd = time.time()
						successCount += 1
						
						finishedMsg = "File finished: %s %.2fMB in %.2fs" % (f['name'], fileSizeDL_MB, timerEnd-timerStart)
						# logging.info(helpers.logMessage(finishedMsg, True))
						break

					fileSizeDL += len(buf)
					fileSizeDL_MB = fileSizeDL / 1024.0 / 1024.0
					localFile.write(buf)

					counter += 1
					if counter % 250 == 0:
						status = "%.2fMB [%3.2f%%]" % (fileSizeDL_MB, fileSizeDL * 100. / fileSizeBytes)
						status = status + chr(8)*(len(status)+1)
						print(status)

		except KeyboardInterrupt:
			errorMsg = "Downloading file %s failed.\n" % (f['name'])
			errorMsg += "Keyboard Interrupt detected. Continuing to next file."
			# logging.error(helpers.logMessage(errorMsg, True))

			if os.path.exists(loc):
				os.remove(loc)

		except Exception as e:
			errorMsg = "Downloading file %s failed.\n" % (f['name'])
			errorMsg += str(e)
			# logging.error(helpers.logMessage(errorMsg, True))

			if os.path.exists(loc):
				os.remove(loc)

	return successCount == len(files)


def extracter(folderLoc, compressedFiles):
	""" Extracts all .gz files in a folder """
	
	for f in compressedFiles:
		try:
			if f.endswith('.gz'):
				gzFile = os.path.join(folderLoc,f)
				destFile = os.path.join(folderLoc, os.path.splitext(f)[0])
				gzFileSizeMB = os.path.getsize(gzFile) / 1024.0 / 1024.0

				infoMsg = 'Extracting %s: %.2fMB' % (f, gzFileSizeMB)
				# logging.info(helpers.logMessage(infoMsg, True))

				blockSize = 8192
				with gzip.open(gzFile, 'rb') as g:
					with open(destFile, 'wb') as d:
						while True:
							buf = g.read(blockSize)							
							if not buf:
								break

							d.write(buf)

				infoMsg = 'Finished extracting {0}'.format(f)
				# logging.info(helpers.logMessage(infoMsg, True))
			else:
				infoMsg = 'File {0} not a .gz file. Skipping...'.format(f)
				# logging.info(helpers.logMessage(infoMsg, True))

		except KeyboardInterrupt:
			errorMsg = "Extracting file {0} failed.\n".format(f)
			errorMsg += "Keyboard Interrupt detected. Continuing to next file."
			# logging.error(helpers.logMessage(errorMsg, True))

		except Exception as e:
			errorMsg = "Extracting file {0} failed.\n".format(f)
			errorMsg += str(e)
			# logging.error(helpers.logMessage(errorMsg, True))
