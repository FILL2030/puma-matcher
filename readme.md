# Puma spark

The matcher program is the part of the puma project in charge to retrieve the match between the publication and the proposal and to extract some data from the full text. This program is built on the top of apache spark.

In order to run this program you have install spark, scala, java python and the python package pillow. And configure the environment variable

The submit.sh script can help you to submit the program to a spark cluster. Two profile are available, test and prod. Don't forget to set the url of your spark cluster inside it.

# Quick start

```bash
mvn clean
mvn package

export PUMA_MATCHING_DATABASE_URL='matching_database_url'
export PUMA_CORPUS_DATABASE_URL='puma_corpus_database_url'
export PUMA_MATCHING_FINAL_SCHEMA='matching'
export PUMA_MATCHING_START_SCHEMA='matching_tmp'
export PUMA_DB_USER='puma_db_user'
export PUMA_DB_PASSWORD='puma_db_password'
export PUMA_FILES_PATH='puma_file_root'
export SPARK_RESOURCE_ROOT='path_to_the_spark-resource_dir '

./submit.sh personDeduplicator -m cluster -e prod -nb
./submit.sh documentDeduplicator -m cluster -e prod -nb
./submit.sh matcher -m cluster -e dev -nb

```

# Environment variable

|Name|Description|
|---|---|
| PUMA_MATCHING_DATABASE_URL | The url of the database to store the match and their data. The database should be a PostgreSQL database |
| PUMA_CORPUS_DATABASE_URL | The url of the database created by the matcher |
| PUMA_MATCHING_FINAL_SCHEMA | The name of the final schema (hot swap between two schema) |
| PUMA_MATCHING_START_SCHEMA |  The name of the temporary schema (erased at startup) |
| PUMA_DB_USER | PostgreSQL username |
| PUMA_DB_PASSWORD | PostgreSQL password|
| PUMA_FILES_PATH | File root of the puma corpus creator |
| SPARK_RESOURCE_ROOT | Spark resource root. Resources can be found under src/main/resource/sparkResource |

# Architecture

The Matcher is build around a configurable pipeline. A pipeline is composed of stage which and run distinct config 
foreach type of data. Each stage have multiple input and a single output. Currently the following stage are available. They can be found under : 
```
src/main/scala/eu/ill/puma/sparkmatcher/matching/stage
```
|Stage name|Function|
|---|---|
| InitialisationStage | Load the data from the datasources |
| AnalyserStage | Run the analyser specified in the current config |
| MatcherStage | Run the matcher specified in the current config |
| FilterStage | Run  filter to remove some wrong candidate result (eg: a publication released before a proposal). The filters need to be added to the stage through the addFilters method. |
| ScoringStage | Run the scorer specified in the current config, produce a dataframe of matchcandidate  |
| NormalisationStage | Normalise the score of the match candidate to fix the 98 percentile to 100 |
| MatchCandidatePersisterStage | Save the input to the matching database |
| StatisticStage | Compute some stats on the input (ranking, stdDev, distribution, mean, min, max) |
| TotalStatisticStage | Compute statistics over all type of entities (person, formula, text, images etc) |
| TrainingDataExtractionStage | Extract the matchCandidate from the training dataset |
| ViewStage | Show the inputs |
| WeightTrainerStage | Search for the best combination of the score of all entities type in order to optimise the ranking of the known matches |
| CountStage | Count the number of row of the input dataframe |
| MatchEditorStage | Run the matchEditor from the current config |

In the current implement of the matcher most of the match are produce by a config foreach entity type. Then a second 
pipeline with one config aggregate all the entity type and perform action like the WeightTrainerStage on the whole result. 
Creating multiple data can be useful when some processing has multiple dependency.

The different stage call different type of "modules" (AnalyserStage call the analyser specified by a config). The table below list these modules type

| Module type | Action |
|---|---|
| Analyser | Perform a data analysis or data mining action |
| Filter | Perform an action which filter the result |
| Match editor | Perform some modification on the match, eg: force a score to 100 |
| Matcher | Create match |
| Scorer | Score match |
