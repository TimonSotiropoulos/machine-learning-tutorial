import mailbox
import csv
from bs4 import BeautifulSoup

# ******************************************************
# SET THESE CONSTANTS BEFORE RUNNING SCRIPT
# ******************************************************
CSV_PATH = 'outputDataFileName.csv'
MBOX_LIST = [
    ('path/to/your/first/archive.mbox', 'spam'),
    ('path/to/your/second/archive.mbox', 'not_spam'),
]

# Script can then be run from the command line as such:
# > python main.py

########################################################
# Function: getBody(msg) -> string
########################################################
# This function will extract the body of an email from
# our mbox message.
#
# NOTE: It will only traverse one layer deep
# into the recurrsive multiparts, so in some instances
# it may miss certain data from the email!
#
def getBody(msg):
    body = ''
    if msg.is_multipart():
        payload = msg.get_payload()
        for subMsg in payload:
            if not subMsg.is_multipart():
                body += subMsg.get_payload(decode=True)
    else:
        # print msg.get_content_type()
        # print payload
        body = msg.get_payload(decode=True)
    return body
    
########################################################
# Function: stripHtmlCssJS(email) -> string
########################################################
# Courtesy this SO Question -> http://stackoverflow.com/questions/30565404/how-do-i-completely-remove-all-style-scripts-and-html-tags-from-an-html-page
# This function will strip out all HTML, CSS and JS
# from a string
#
def stripHtmlCssJS(email):
    soup = BeautifulSoup(email, "html.parser") # create a new bs4 object from the html data loaded
    for script in soup(["script", "style"]): # remove all javascript and stylesheet code
        script.extract()
    # get text
    text = soup.get_text()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = ' '.join(chunk for chunk in chunks if chunk).encode('utf-8').strip()
    return text

########################################################
# Function: writeToCSV(csvPath, label, emailList)
########################################################
# This function will take a list of emails as well as a
# label and write these out to a csv file. This
#
# NOTE: This will append to the end of an existing CSV
# file. This means that we can easily have multiple mBoxes
# and write them all to the one CSV file
def writeToCSV(csvPath, label, emailList):
    with open(csvPath, 'ab') as csvFile:
        csvWriter = csv.writer(csvFile, delimiter=",")
        for email in emailList:
            csvWriter.writerow((label, email))

########################################################
# Function: mBoxtoCSV(mBoxPath, label, csvPath)
########################################################
# This function uses all of the helper functions above to
# easily convert one mBox with a specific label type into
# clean email text and then append it to the end of a
# provided CSV file
#
def mBoxToCSV(mBoxPath, label, csvPath):
    print "Loading mBox from file system..."
    box = mailbox.mbox(mBoxPath)
    print "Initializing Email List..."
    emailList = []
    for index, message in enumerate(box):
        print "Processing Email at index: " + str(index)
        email = getBody(message)
        cleanEmail = stripHtmlCssJS(email)
        emailList.append(cleanEmail)
    print "Email Cleaning Complete..."
    print "Writing to CSV File..."
    writeToCSV(csvPath, label, emailList)

########################################################
# Function: processMBoxes(mboxList, csvPath)
########################################################
# This function will process an array of tuples of mBoxes and their
# labels. An example would be the following:
#
# mboxList = [('MBOX_PATH_A', 'spam'), ('MBOX_PATH_B', 'not_spam') ...] etc!
#
def processMBoxes(mboxList, csvPath):
    for mbox in mboxList:
        mBoxToCSV(mbox[0], mbox[1], csvPath)

processMBoxes(MBOX_LIST, CSV_PATH)
