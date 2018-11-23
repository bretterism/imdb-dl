import logging
import os
import pandas as pd
import sys

sys.path.append('../helpers')
import helpers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def scrubber(compressedFiles):
	delimiter = "\t"

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

				# Writing back to scrub file
				scrubbedFilename = 'scrubbed.'+ os.path.splitext(f)[0]
				scrubbedFilepath = os.path.join('/tmp', scrubbedFilename)
				df.to_csv(scrubbedFilepath, sep="\t")
				del df

			if (f == 'title.episode.tsv.gz'):
				# Placing file in dataframe
				dtypes = {'seasonNumber': str, 'episodeNumber': str}
				df = pd.read_csv(filepath,compression='gzip',sep=delimiter,dtype=dtypes)

				# Deleting rows where episodeNumber and seasonNumber are not available
				df = df[df.episodeNumber != r'\N']
				df = df[df.seasonNumber != r'\N']

				# Writing back to scrub file
				scrubbedFilename = 'scrubbed.'+ os.path.splitext(f)[0]
				scrubbedFilepath = os.path.join('/tmp', scrubbedFilename)
				df.to_csv(scrubbedFilepath, sep="\t", index=False)
				del df

			if (f == 'title.ratings.tsv.gz'):
				# Placing file in dataframe
				dtypes = {'seasonNumber': str, 'episodeNumber': str}
				df = pd.read_csv(filepath,compression='gzip',sep=delimiter,dtype=dtypes)

				# Nothing to scrub. Just make a scrubbed file.
				scrubbedFilename = 'scrubbed.'+ os.path.splitext(f)[0]
				scrubbedFilepath = os.path.join('/tmp', scrubbedFilename)
				df.to_csv(scrubbedFilepath, sep="\t", index=False)
				del df

			infoMsg = 'Scrubbing {0} succeeded.'.format(f)
			logger.info(helpers.logMessage(infoMsg, True))


		except Exception as e:
			errorMsg = "Scrubbing file {0} failed.\n".format(f)
			errorMsg += str(e)

			logger.error(helpers.logMessage(errorMsg, True))


def transformer():
	delimiter = "\t"
	requiredFiles = ['scrubbed.title.basics.tsv', 'scrubbed.title.episode.tsv', 'scrubbed.title.ratings.tsv']

	try:
		for file in requiredFiles:
			checkFile = os.path.join('/tmp', file)
			if not os.path.exists(checkFile):
				raise FileNotFoundError(file)

		infoMsg = 'Transforming files.'
		logger.info(helpers.logMessage(infoMsg, True))

		# Pulling back data from title.basics.tsv
		filepath = os.path.join('/tmp', 'scrubbed.title.basics.tsv')
		dtypes = {'startYear': str, 'endYear': str, 'runtimeMinutes': str}
		dfBasics = pd.read_csv(filepath,sep=delimiter,dtype=dtypes)

		# Pulling back data from title.episode.tsv
		filepath = os.path.join('/tmp', 'scrubbed.title.episode.tsv')
		dtypes = {'seasonNumber': str, 'episodeNumber': str}
		dfEpisode = pd.read_csv(filepath,sep=delimiter,dtype=dtypes)
		dfEpisode = dfEpisode.rename(columns={ 'tconst': 'episodeTconst', 'parentTconst': 'seriesTconst' })

		# Pulling back data from title.ratings.tsv
		filepath = os.path.join('/tmp', 'scrubbed.title.ratings.tsv')
		dtypes = {'averageRating': str, 'numVotes': str}
		dfRatings = pd.read_csv(filepath,sep=delimiter,dtype=dtypes)

		# Getting all the episode-specific information from title.basics.tsv (dfBasics object)
		dfTVEpisodes = dfBasics.loc[dfBasics['titleType'] == 'tvEpisode']
		dfTVEpisodes = dfTVEpisodes.rename(columns={ 'tconst': 'episodeTconst', 'primaryTitle': 'episodeTitle', 'runtimeMinutes': 'episodeRuntimeMinutes' })
		dfTVEpisodes = dfTVEpisodes.filter(items=['episodeTconst','episodeTitle', 'episodeRuntimeMinutes'])

		# Getting all the series-specific information from title.basics.tsv (dfBasics object)
		dfTVSeries = dfBasics.loc[dfBasics['titleType'].isin(['tvSeries','tvMiniSeries'])]
		dfTVSeries = dfTVSeries.rename(columns={ 'tconst': 'seriesTconst', 'primaryTitle': 'seriesTitle' })
		dfTVSeries = dfTVSeries.filter(items=['seriesTconst','seriesTitle'])
		del dfBasics

		# Combining the episodes and ratings
		dfMerged = pd.merge(dfEpisode, dfRatings, left_on='episodeTconst', right_on='tconst')
		del dfEpisode
		del dfRatings

		# Dropping extra column(s)
		dfMerged = dfMerged.drop(['tconst'], axis=1)

		# Combining Episode/Series information to the episodes/ratings
		dfMerged = pd.merge(dfMerged, dfTVSeries, on='seriesTconst')
		dfMerged = pd.merge(dfMerged, dfTVEpisodes, on='episodeTconst')

		# Export to file
		transformFilepath = os.path.join('/tmp', 'transformed.episode.tsv')
		dfMerged.to_csv(transformFilepath, sep='\t', index=False)

		infoMsg = 'Transforming files succeeded.'
		logger.info(helpers.logMessage(infoMsg, True))


	except FileNotFoundError as f:
		errorMsg = 'File {0} not found in {1}'.format(f, '/tmp')
		logger.error(helpers.logMessage(errorMsg, True))