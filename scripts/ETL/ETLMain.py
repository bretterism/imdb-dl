import sys

import ETLExtract
import ETLTransform

sys.path.append('./helpers')
import helpers


files = [
	# {'name':'name.basics.tsv.gz', 'url':'https://datasets.imdbws.com/name.basics.tsv.gz'},
	# {'name':'title.akas.tsv.gz', 'url':'https://datasets.imdbws.com/title.akas.tsv.gz'},
	{'name':'title.basics.tsv.gz', 'url':'https://datasets.imdbws.com/title.basics.tsv.gz'},
	# {'name':'title.crew.tsv.gz', 'url':'https://datasets.imdbws.com/title.crew.tsv.gz'},
	{'name':'title.episode.tsv.gz', 'url':'https://datasets.imdbws.com/title.episode.tsv.gz'},
	# {'name':'title.principals.tsv.gz', 'url':'https://datasets.imdbws.com/title.principals.tsv.gz'},
	{'name':'title.ratings.tsv.gz', 'url':'https://datasets.imdbws.com/title.ratings.tsv.gz'}
]

compressedFiles = [f['name'] for f in files]
transformFiles = ['transformed.episode.tsv']

importFolder = helpers.createDateFolder('../../import')

ETLExtract.downloader(files, importFolder)
scrubbedDict = ETLTransform.scrubber(compressedFiles, importFolder)
ETLTransform.transformer(scrubbedDict, importFolder)