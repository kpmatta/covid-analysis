from pyspark.sql import SparkSession
import urllib.request
import os
from pyspark.sql import functions as f


def download_file(src_url: str, dest_path: str):
    with urllib.request.urlopen(src_url) as src_file, open(dest_path, 'wb') as dest_file:
        data = src_file.read()
        dest_file.write(data)


def analyse_data(file_path: str, spark: SparkSession):
    df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(file_path) \
        .withColumnRenamed('Country/Region', 'Country')

    # date_cols = [(datetime.now() - timedelta(i)).strftime('%-m/%-d/%y') for i in range(5, 0, -1)]
    date_cols =  df.columns[-5:]
    last_day = df.columns[-1]
    cols = ['Country']
    cols.extend(date_cols)
    agg_cols = [f.sum(d).alias(d) for d in date_cols]

    final_df = df.select(cols) \
                .groupBy('Country') \
                .agg(*agg_cols) \
                .orderBy(f.desc(last_day))

    final_df.show()


if __name__ == '__main__':
    root_path = os.path.dirname(os.path.abspath(__file__))
    spark = SparkSession.builder.master('local[*]').appName('covid_analysis').getOrCreate()

    src_confirmed_file = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    local_confirmed_file = root_path + "/data/time_series_covid19_confirmed_global.csv"
    download_file(src_confirmed_file, local_confirmed_file)
    print("Total confirmed cases:")
    analyse_data(local_confirmed_file, spark)

    src_deaths_file = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
    local_deaths_file = root_path + "/data/time_series_covid19_deaths_global.csv"
    download_file(src_deaths_file, local_deaths_file)
    print("Total deaths:")
    analyse_data(local_deaths_file, spark)