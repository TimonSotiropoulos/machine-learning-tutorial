#Classifier Notebook
The files contained here are the scala source code and importable notebook for the Naive Bayes Classifier implementation on Databricks. 

The notebook will handle the creation of a Dataframe from a CSV file of labelled email data. It will then proceed to normalize the data, extract the features from the dataset and train a Databricks Pipeline Model. This is then saved out to a filesystem for use in other scripts and programs.

###Usage

This notebook (.dbc) has been written specifically for the Databricks platform and can be imported directly into Databricks for use. 

However, the scala source code is also provided for anyone that wishes to utilise different parts of the notebook in their applications. Be aware that this is a notebook export of the Scala source code, so will include some specific Databricks terminology (such as MAGIC sections where the notebook is writing changing languages to SQL).