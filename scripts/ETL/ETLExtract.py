import gzip
import logging
import os
import sys
import time
import urllib.request

sys.path.append('../helpers')
import helpers


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def downloader(files):
	successCount = 0
	for f in files:
		loc = os.path.join('/tmp', f['name'])
		try:
			timerStart = time.time()
			res = urllib.request.urlopen(f['url'])
			with open(loc, 'wb') as localFile:

				fileSizeBytes = int(res.getheader('Content-Length'))
				fileSizeMB = fileSizeBytes / 1024.0 / 1024.0

				downloadMsg = "Downloading %s: %.2f MB" % (f['name'], fileSizeMB)
				logger.info(helpers.logMessage(downloadMsg, True))


				blockSize = 8192
				fileSizeDL = 0
				counter = 0
				while True:
					buf = res.read(blockSize)
					if not buf:
						timerEnd = time.time()
						successCount += 1
						
						finishedMsg = "File finished: %s %.2fMB in %.2fs" % (f['name'], fileSizeDL_MB, timerEnd-timerStart)
						logger.info(helpers.logMessage(finishedMsg, True))
						break

					fileSizeDL += len(buf)
					fileSizeDL_MB = fileSizeDL / 1024.0 / 1024.0
					localFile.write(buf)

					counter += 1
		except Exception as e:
			errorMsg = "Downloading file %s failed.\n" % (f['name'])
			errorMsg += str(e)
			logger.error(helpers.logMessage(errorMsg, True))

			if os.path.exists(loc):
				os.remove(loc)

	return successCount == len(files)


def extracter(filenames):
	""" Extracts all .gz files """
	for f in filenames:
		try:
			compressedFile = os.path.join('/tmp',f)
			if compressedFile.endswith('.gz'):
				destFile = os.path.join('/tmp', os.path.splitext(f)[0])
				gzFileSizeMB = os.path.getsize(compressedFile) / 1024.0 / 1024.0

				infoMsg = 'Extracting %s: %.2fMB' % (f, gzFileSizeMB)
				logger.info(helpers.logMessage(infoMsg, True))

				blockSize = 8192
				with gzip.open(compressedFile, 'rb') as g:
					with open(destFile, 'wb') as d:
						while True:
							buf = g.read(blockSize)							
							if not buf:
								break

							d.write(buf)

				infoMsg = 'Finished extracting {0}'.format(f)
				logger.info(helpers.logMessage(infoMsg, True))
			else:
				infoMsg = 'File {0} not a .gz file. Skipping...'.format(f)
				logger.info(helpers.logMessage(infoMsg, True))

		except Exception as e:
			errorMsg = "Extracting file {0} failed.\n".format(f)
			errorMsg += str(e)
			logger.error(helpers.logMessage(errorMsg, True))
