# IMDb-DL
View TV show ratings

### What is this thing?
Search for a TV show, and see how every episode is rated on IMDb.

### How was it built?
Data files are downloaded from IMDb [here](https://datasets.imdbws.com/). The ETL scripts prep the data and boils them down to a single transformed file. This file is loaded into the [ELK Stack](https://www.elastic.co/what-is/elk-stack) installation.

### What does it look like?
![alt text](https://github.com/bretterism/imdb-dl/blob/master/img/Kibana.png?raw=true "Kibana Screenshot")


### What's next?
I'm not a huge fan of Kibana for this project. It only wants to aggregate data, so I had to do some tricks to make it work how I wanted. I plan on testing out some other visualization tools in the future.
