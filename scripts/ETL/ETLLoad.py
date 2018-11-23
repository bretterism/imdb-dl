import csv
import logging
import os
import sys

sys.path.append('../helpers')
import helpers


logging.basicConfig(filename='logs/downloadFiles.log', level=logging.INFO)

def _episodeToJSON(data):
	episode = {
		'seriesTconst': data['seriesTconst'],
		'episodeTconst': data['episodeTconst'],
		'seasonNumber': data['seasonNumber'],
		'episodeNumber': data['episodeNumber'],
		'averageRating': data['averageRating'],
		'numVotes': data['numVotes'],
		'seriesTitle': data['seriesTitle'],
		'episodeTitle': data['episodeTitle'],
		'episodeRuntimeMinutes': data['episodeRuntimeMinutes']
	}

	return episode


def loadData(importPath, importFiles, importFileDelimiter="\t", endpointUrl='http://localhost:8000'):
	dynamodb = helpers.connect('dynamodb', 'us-west-2', endpointUrl)

	# Get list of files to import
	filesToImport = [os.path.join(importPath, f) for f in importFiles]

	for importFile in importFiles:
		fullFilePath = os.path.join(importPath,importFile)
		# open the file to import
		with open(fullFilePath, 'r') as f:
			reader = csv.DictReader(f, delimiter=importFileDelimiter)
			numRowsInFile = helpers.wc(fullFilePath)
			
			print("{0}: {1}".format(importFile, numRowsInFile))
				
			if (importFile == 'transformed.episode.tsv'):
				table = dynamodb.Table('episodes')
				with table.batch_writer() as batch:
					try:
						for idx,dataRow in enumerate(reader):
							if (idx % 250 == 0):
								status = "%d rows [%3.2f%%] complete" % (idx, idx * 100. / numRowsInFile)
								status = status + chr(8)*(len(status)+1)
								print(status)
							batch.put_item(_episodeToJSON(dataRow))
					except Exception as e:
						print(e)