import boto3
import logging
import os
import pandas as pd
import sys
from io import StringIO

sys.path.append('../helpers')
import helpers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def scrubber(compressedFiles):
	delimiter = "\t"
	scrubbedDict = {}

	for f in compressedFiles:
		try:
			filepath = os.path.join('/tmp', f)

			infoMsg = 'Scrubbing {0}'.format(f)
			logger.info(helpers.logMessage(infoMsg, True))
			if (f == 'title.basics.tsv.gz'):
				# Placing file in dataframe
				dtypes = {'startYear': str, 'endYear': str, 'runtimeMinutes': str}
				df = pd.read_csv(filepath,compression='gzip',sep=delimiter,dtype=dtypes)

				# Deleting rows that aren't tvSeries or tvEpisodes
				df = df.drop(df[~df.titleType.isin(['tvSeries','tvMiniSeries','tvEpisode'])].index)

				# Replacing the \N values for runtimeMinutes
				df.loc[df['runtimeMinutes'] == '\\N', 'runtimeMinutes'] = '-1'

				# Adding dataframe to dictionary
				scrubbedDict['title.basics.tsv'] = df
				del df


			if (f == 'title.episode.tsv.gz'):
				# Placing file in dataframe
				dtypes = {'seasonNumber': str, 'episodeNumber': str}
				df = pd.read_csv(filepath,compression='gzip',sep=delimiter,dtype=dtypes)

				# Deleting rows where episodeNumber and seasonNumber are not available
				df = df[df.episodeNumber != r'\N']
				df = df[df.seasonNumber != r'\N']

				# Renaming columns
				df = df.rename(columns={ 'tconst': 'episodeTconst', 'parentTconst': 'seriesTconst' })

				# Adding dataframe to dictionary
				scrubbedDict['title.episode.tsv'] = df
				del df

			if (f == 'title.ratings.tsv.gz'):
				# Placing file in dataframe
				dtypes = {'seasonNumber': str, 'episodeNumber': str, 'averageRating': str, 'numVotes': str}
				df = pd.read_csv(filepath,compression='gzip',sep=delimiter,dtype=dtypes)

				# Adding dataframe to dictionary
				scrubbedDict['title.ratings.tsv'] = df
				del df

			infoMsg = 'Scrubbing {0} succeeded.'.format(f)
			logger.info(helpers.logMessage(infoMsg, True))

		except Exception as e:
			errorMsg = "Scrubbing file {0} failed.\n".format(f)
			errorMsg += str(e)

			logger.error(helpers.logMessage(errorMsg, True))

	return scrubbedDict


def transformer(scrubbedDict, bucketName):
	delimiter = "\t"
	requiredFiles = ['title.basics.tsv', 'title.episode.tsv', 'title.ratings.tsv']

	try:
		for r in requiredFiles:
			if r not in scrubbedDict:
				raise FileNotFoundError(r)

		infoMsg = 'Transforming files.'
		logger.info(helpers.logMessage(infoMsg, True))

		# Getting all the episode-specific information from title.basics.tsv (dfBasics object)
		dfBasics = scrubbedDict.pop('title.basics.tsv', None)

		dfTVEpisodes = dfBasics.loc[dfBasics['titleType'] == 'tvEpisode']
		dfTVEpisodes = dfTVEpisodes.rename(columns={ 'tconst': 'episodeTconst', 'primaryTitle': 'episodeTitle', 'runtimeMinutes': 'episodeRuntimeMinutes' })
		dfTVEpisodes = dfTVEpisodes.filter(items=['episodeTconst','episodeTitle', 'episodeRuntimeMinutes'])

		# Getting all the series-specific information from title.basics.tsv (dfBasics object)
		dfTVSeries = dfBasics.loc[dfBasics['titleType'].isin(['tvSeries','tvMiniSeries'])]
		dfTVSeries = dfTVSeries.rename(columns={ 'tconst': 'seriesTconst', 'primaryTitle': 'seriesTitle' })
		dfTVSeries = dfTVSeries.filter(items=['seriesTconst','seriesTitle'])
		del dfBasics

		# Combining the episodes and ratings
		dfEpisode = scrubbedDict.pop('title.episode.tsv', None)
		dfRatings = scrubbedDict.pop('title.ratings.tsv', None)
		dfMerged = pd.merge(dfEpisode, dfRatings, left_on='episodeTconst', right_on='tconst')
		del dfEpisode
		del dfRatings

		# Dropping extra column(s)
		dfMerged = dfMerged.drop(['tconst'], axis=1)

		# Combining Episode/Series information to the episodes/ratings
		dfMerged = pd.merge(dfMerged, dfTVSeries, on='seriesTconst')
		dfMerged = pd.merge(dfMerged, dfTVEpisodes, on='episodeTconst')

		infoMsg = 'Transforming files succeeded.'
		logger.info(helpers.logMessage(infoMsg, True))

		infoMsg = 'Uploading to s3 bucket {0}.'.format(bucketName)
		logger.info(helpers.logMessage(infoMsg, True))

		# Export transformed dataframe to S3 bucket
		s3 = boto3.resource('s3')
		csvBuffer = StringIO()
		dfMerged.to_csv(csvBuffer, index=False)
		s3.Object(bucketName, 'transformed.episode.tsv').put(Body=csvBuffer.getvalue())

		infoMsg = 'Finished uploading to s3 bucket {0}.'.format(bucketName)
		logger.info(helpers.logMessage(infoMsg, True))

	except FileNotFoundError as f:
		errorMsg = 'File {0} not found in Dictionary object'.format(f)
		logger.error(helpers.logMessage(errorMsg, True))