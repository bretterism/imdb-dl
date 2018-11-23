import sys

from ETLExtract import downloader, extracter
from ETLTransform import scrubber, transformer

sys.path.append('../helpers')
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
uncompressedFiles = [f['name'].replace('.gz','') for f in files]
scrubbedFiles = ['scrubbed.{0}'.format(f) for f in uncompressedFiles]
transformFiles = ['transformed.episode.tsv']


importPath = 'import'
bucketName = 'imdb-dl-etl-bucket'
# archivePath = helpers.createDateFolder(importPath)

downloader(files)
extracter(compressedFiles)
scrubber(uncompressedFiles)
transformer()

# helpers.archiveFiles(importPath, archivePath)