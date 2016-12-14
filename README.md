# Machine Learning Tutorial

This repository contains all the code and notebooks to support the blog post on [Building a production ready Email Classifier from scratch using Gmail, Apache Spark, Databricks, AWS and Node.js](http://seeddigital.co).
The tutorial is broken up into the following sections:

#### [Email Extractor](https://github.com/TimonSotiropoulos/machine-learning-tutorial/tree/master/Email-Extractor)

This is a python script that will label and convert a list of mBox files into a CSV file. The format of the corresponding CSV file will be rows of (label, email text). EG:

	"spam", "Click the link here to get your prize!",
	"non_spam", "Hello, I was just wanting to update out meeting",
	etc...
	
Labels can be applied to specific mBoxes and full usage instructions can be found in the README in the Email Extractor folder.

#### [Classifier Notebook](https://github.com/TimonSotiropoulos/machine-learning-tutorial/tree/master/Classifier-Notebook)

This folder contains a Databricks notebook in both a Notebook file (.dbc) and Scala source code (.scala). The notebook completes the following tasks:

- Creates a Dataframe from a (label, emailText)
- Normalises the email text
- Extracts features from the normalised emails
- Trains a Naive Bayes Classifier on a random subset provided emails
- Evaluates the performance of the classifier on a random subset of the provided emails

Basic usage instructions can be found in the README in the Classifier Notebook folder.

#### Node.js Email Classifier API

This section will relate to the connecting of a Node.js server to your Fitted Model developed using the Classifier Notebook provided in this tutorial. 

This section is coming soon...

