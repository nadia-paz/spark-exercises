import pyspark
import pandas as pd
from pydataset import data

from pyspark.sql.functions import *


#############################
#####SPARK API EXERCISES#####
#############################


# create spark session
spark = pyspark.sql.SparkSession.builder.getOrCreate()

# create spark dataframe
df = spark.createDataFrame(
        pd.DataFrame(
            {'language': ['python', 'C', 'C#', 'C++', 'Java', 'Javascript', 'Go', 'Julia', 'Scala', 'R']}
            )
        )

# print first 5 rows of the data frame
print(df.show(5))
# print schema
print(df.printSchema())


# 2 . MPG DATASET

# load mpg dataset as spark data frame  
mpg = spark.createDataFrame(data("mpg"))

#The 1999 audi a4 has a 4 cylinder engine.
mpg.select(concat(
    lit('The '), mpg.year, lit(' '), 
    mpg.manufacturer, lit(' '), mpg.model, lit(' has a '),
    mpg.cyl, lit(" cylinder engine")
    )).alias('show result').show(truncate=False)

# Transform the trans column so that it only contains either manual or auto.
mpg.select(mpg.trans, when(mpg.trans.contains('auto'), "auto")
                           .otherwise('manual')
                           .alias("transmission")).show()

# Load the tips dataset as a spark dataframe
tips = spark.createDataFrame(data('tips'))

# What percentage of observations are smokers?
print(tips.groupBy('smoker').count()) # returns value_count not smokers 151, smokers 93
# couldn't find value_counts(normalize)