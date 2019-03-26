import gzip
import logging
import os
import sys
import time
import urllib.request

sys.path.append('./helpers')
import helpers


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def downloader(files, downloadFolder):
	successCount = 0
	for f in files:
		loc = os.path.join(downloadFolder, f['name'])
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