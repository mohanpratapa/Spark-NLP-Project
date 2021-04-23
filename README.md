# Spark-NLP-Project

## Steps for the Spark NLP project
```
import urllib.request

urllib.request.urlretrieve("https://www.gutenberg.org/files/98/98-0.txt","/tmp/mohan.txt")
dbutils.fs.mv("file:/tmp/mohan.txt",'dbfs:/data/mohan.txt')

num_VM=4
rawRDD= sc.textFile('dbfs:/data/mohan.txt',num_VM)

from pyspark.ml.feature import StopWordsRemover
remover=StopWordsRemover()
stopwords=remover.getStopWords()
print(stopwords)

VM_RDD=rawRDD.flatMap(lambda line:line.strip().split(" "))

import re
def removePunctuation(text):
    return re.sub('([^\w\s]|_)','',text,0).strip().lower()
  

non_letter_rdd=VM_RDD.map(lambda words: removePunctuation(words))
VM_Clean_RDD=non_letter_rdd.filter(lambda word: word not in stopwords).map(lambda word: (word,1))

Output_RDD=VM_Clean_RDD.reduceByKey(lambda acc, value: acc+value)
Output=Output_RDD.collect()

print(Output)

%py
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
title = 'Top 10 Words Used in the book' 
xlabel = 'word'
ylabel = 'count'
# create Pandas dataframe from results list
df = pd.DataFrame.from_records(Output, columns =[xlabel, ylabel])
df1=df[df.word!=""]
df2=df1.nlargest(10,["count"])
print(df2)
# create plot (using matplotlib)

sns.barplot(xlabel, ylabel, data=df2, palette="Blues_d").set_title(title)
```
### Output file:

![](https://github.com/mohanpratapa/Spark-NLP-Project/blob/main/Spark-NLP-Project.PNG)
