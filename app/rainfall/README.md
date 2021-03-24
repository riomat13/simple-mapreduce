# Rainfall in Australia

In this example, use "Rain in Australia" dataset ([kaggle](https://www.kaggle.com/jsphyg/weather-dataset-rattle-package))
and grouping all the rainfall data by city in each month.

Before running the app, preprocess the data to split by year to maximize the benefit.
An example script with `pandas` is the following.

```python
import pandas as pd

df = pd.read_csv(path_to_file)

# extract all years
dates = df.Date.apply(lambda x: x.split('-')[0]).unique()

# filter by year and store as csv
for date in dates:
    df_tmp = df[df.Date.str.startswith(date)]
    df_tmp.to_csv(filename, header=True, index=False)
```

In this app, grouping by `city` and `year-month` (`CompositeKey` is used), then sorted the rainfall in each group.
The rainfall in each month will be sorted in descending order.

Output format after run the mapreduce process is:
```
${city}, ${year}-${month}     ${rainfall}
```

The script is in `main.cc`.