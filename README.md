# Strategies and limitations in app usage and human mobility

This repository shares the stop location algorithm, the aggregated data and the notebooks to repeat some of the experiments of the manuscript.


## Stop Location Algorithm

We code shared here is based on the paper "[Project Lachesis: Parsing and Modeling Location Histories](https://link.springer.com/chapter/10.1007/978-3-540-30231-5_8)", implemented in a distributed fashion with Apache Spark.

### Installation

We assume that you're using [Python 3.6](https://www.python.org/downloads/).

Then we assume these Python package dependencies:
* [Spark](http://spark.apache.org/) 2.2.1
* [sklearn](http://scikit-learn.org/stable/) 0.19

### Data

To have the best performances the input and output data are parquet-formatted files. The input file must be placed in [] with the following format:

```
user_id: string
timestamp: datetime.datetime
latitude: float
longitude: float
```

The output file will be placed in the path specified by `output_path`, and it will have the following format:

```
user_id: string
timestamp: datetime.datetime
lat: float
lon: float
from: datetime.datetime
to: datetime.datetime
```

### Run the code
To run the code in Spark, locally (with 10 processes, 15G each), you can run this command:

```sh
./bin/spark-submit --master local[10] --conf spark.executor.memory=15G --conf spark.driver.memory=10G pyspark_stop_locations.py
```

## Intermediate data and plots

We shared the code to produce all the plots in the manuscript and supplementary information in `notebooks/main_manuscript_plots.ipynb`. 
The aggregated data is shared in `data/plots/` and they can be used on other papers as well, citing our main paper.

