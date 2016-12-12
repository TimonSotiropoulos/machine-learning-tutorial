# Email Extractor

This python script is used for converting numerous different email achives in the mBox format into a comma seperated **CSV** file. This is the first step of the machine learning tutorial connecting Databricks, Apache Spark and  Node.js.

### Requirements 

To run this script you will need to have Python 2.7 installed. Download and installation instructions can be found at the official Python website:

<https://www.python.org/downloads/>

###Usage Instructions

The script requires the user to have a list of mBoxes that they wish to turn into one large CSV file. MBox archives of gmail emails can be created by following [the official instructions on Google's website.](https://gmail.googleblog.com/2013/12/download-copy-of-your-gmail-and-google.html)  

Once you have your MBox archives downloaded and setup in your labels, you are ready to update the script to run.

At the start of the script, there are two variables that need to be set, ***CSV_PATH*** and ***MBOX_LIST***

**CSV_PATH**: The csv file path that you want your mbox archives to be written to.  
**MBOX_LIST**: A list of tuples that contain as their first element, the path to your mBox archive and as their second element, the label you want applied to each of these emails.

	# ******************************************
	# SET THESE CONSTANTS BEFORE RUNNING SCRIPT
	# ******************************************
	
	CSV_PATH = 'dataFile.csv'
	MBOX_LIST = [
		('path/to/archive1.mbox', spam),
		('path/to/archive2.mbox', nonspam),
		('path/to/archive3.mbox', spam)
		# etc...
	]
	
### Running the Script

Script execution can be run by navigating to the directory that contains the script and by running the following command:

	$ python main.py

