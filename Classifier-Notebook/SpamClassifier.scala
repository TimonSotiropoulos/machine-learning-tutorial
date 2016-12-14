// Databricks notebook source exported at Wed, 14 Dec 2016 02:33:04 UTC
/*
 * Initilizing Variables that we will need for our Notebook!
 */
import sqlContext.implicits._
import org.apache.spark.sql.functions._

val csvFilePath = "/FileStore/<YOUR_TABLE_FILEPATH>";

// COMMAND ----------

/*
 * Creating the Schema for our DataFrame and loading in our file
 */
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// Create the Schema that will represent our dataframe. 
val labelField = StructField("label", StringType, nullable=true)
val emailField = StructField("emailText", StringType, nullable=true)
val fields = Seq(labelField, emailField)
val frameSchema = StructType(fields)

// Use the sqlContext to create our Dataframe
val dataCollectionDF = sqlContext
    .read
    .format("com.databricks.spark.csv") // Tell the sqlContext the data we are giving it is of the .csv type, this makes loading faster!
    .option("delimited", ",") // The character that splits all of our columns in our .csv file.
    .option("header", "false") // You could pass the column names as the first row of your .csv file, however we don't.
    .option("mode", "PERMISSIVE") // Read mode of .csv file. Attempts to loal even malformed data, other options are DROPMALFORMED and FAILFAST
    .schema(frameSchema)
    .load(csvFilePath)

// Cache our Dataframe so that next time it is loaded faster
dataCollectionDF.cache()
// Finally create a temp table for our data collection so we can query it to make sure everything worked as expected
dataCollectionDF.createOrReplaceTempView("dataCollectionDF")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM dataCollectionDF

// COMMAND ----------

/*
 ************************************************************
 * STAGE 1:
 * TEXT NORMALISATION
 ************************************************************
 */

// COMMAND ----------

import org.apache.spark.sql.functions.udf
import java.util.regex.Pattern

/*
 * Normalisation Functions
 */

// Set all text to lower case so capitalised words are all treated the same. Eg. Happy = happy = hApPy
val toLowerCase = udf {
  (text: String) => { text.toLowerCase }
}

// Remove all punctuation as we don't need this information to create our features
val removePunctuationAndSpecialChar = udf {
  (text: String) =>
    val regex = "[\\.\\,\\:\\-\\!\\?\\n\\t,\\%\\#\\*\\|\\=\\(\\)\\\"\\>\\<\\/\\'\\`\\&\\{\\}\\;\\+\\-\\[\\]\\_]"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    // Remove all matches, split at whitespace )repeated whitspace is allowed) then join again
    val cleanedText = matcher.replaceAll(" ").split("[ ]+").mkString(" ")
    cleanedText

}

// Treat all different currency symbols as the same generic one
val normalizeCurrencySymbol = udf {
  (text: String) =>
    val regex = "[\\$\\€\\£]"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll(" normalizedcurrencysymbol ")
}

// Treat all emoticons as the same generic one
val normalizeEmoticon = udf {
  (text: String) => 
    val emoticons = List(":-)", ":)", ":D", ":o)", ":]", ":3", ":c)", ":>", "=]", "8)")
    val regex = "(" + emoticons.map(Pattern.quote).mkString("|") + ")"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll(" normalizedemoticon ")
}

// Treat all numbers as one generic one
val normalizeNumber = udf {
  (text: String) =>
    val regex = "\\d+"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll(" normalizednumber ")
}

// Treat all urls as one generic entry
val normalizeURL = udf {
  (text: String) =>
    val regex = "(http://|https://)?www\\.\\w+?\\.(de|com|co.uk)"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll(" normalizedurl ")
}

// Treat all emails as one generic entry
val normalizeEmailAddress = udf {
  (text: String) => 
    val regex = "\\w+(\\.|-)*\\w+@.*\\.(com|de|uk|edu.au|au|co)"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll(" normalizedemailaddress ")
}

// Remove any encoded HTML Characters 
val removeHTMLCharacterEntities = udf {
  (text: String) => 
    val HTMLCharacterEntities = List("&lt;", "&gt;", "&amp;", "&cent;", "&pound;", "&yen;", "&euro;", "&copy;", "&reg;")
    val regex = "(" + HTMLCharacterEntities.map(x => "\\" + x).mkString("|") + ")"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll("")
}

// Optional
// Remove any common html elements that may have slipped through our initial clean up process
val removeHTMLElements = udf {
  (text: String) => 
    val HTMLCharacterEntities = List("abbr", "applet", "aside", "basefont", "blank", "block", "blockquote", "canvas", "caption", "code", "colgroup", "datalist", "embed", "fieldset", "figcaption", "font", "footer", "frameset", "http", "https", "header", "href", "html", "iframe", "img", "input", "kbd", "keygen", "menu", "menuitem", "meta", "meter", "noframes", "noscript", "optgroup", "org", "output", "param", "ruby", "samp", "span", "style", "table", "tbody", "textarea", "tfoot", "thead", "www")
    val regex = "(" + HTMLCharacterEntities.map(x => "\\b" + x).mkString("\\b|") + ")"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll("")
}

// Optional
// Remove any common CSS elements that may have slipped through our initial clean up process
val removeCSSElements = udf {
    (text: String) => 
    val CSSCharacterEntities = List("auto", "align", "alt", "background", "bgcolor", "border", "cellspacing", "cellpadding", "class", "clear", "clip", "color", "cursor", "display", "ffffff", "fff", "filter", "font", "height", "helvetica", "helveticaneue", "important", "left", "lspace", "margin", "moz", "neue", "overflow", "padding", "position", "float", "radius", "rspace", "sans", "serif", "serrif", "spacing", "size", "src", "text", "top", "vertical", "visibility", "webkit", "width")
    val regex = "(" + CSSCharacterEntities.map(x => "\\b" + x).mkString("\\b|") + ")"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll("")
}

// Optional
// Remove very short strings
val removeShortStrings = udf {
  (text: String) =>
    val regex = "\\b[\\w']{1,2}\\b"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
    
    matcher.replaceAll("")
}

// Optional
// Remove common known nouns specific to your data set
val removeKnownNouns = udf {
      (text: String) => 
    val NounEntities = List("john", "smith")
    val regex = "(" + NounEntities.map(x => "\\b" + x).mkString("\\b|") + ")"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll("")
}

// Optional
// Remove any other abnormalities you find when reviewing your results
val removeAbnomalities = udf {
        (text: String) => 
    val regex = "\\ba{3,}\\b"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(text)
  
    matcher.replaceAll("")
}

// COMMAND ----------

/*
 * Running our Normalization functions
 */

// We are going to run each row of the emailText column sequentially through each of our normalisation functions.
// This means that the first the text will be made lower case, then all scanned through for URLS, then all scanned 
// through for email addresses etc etc.
val dataColNormalizedDF = dataCollectionDF.withColumn("normalizedEmailText",
  removeAbnomalities(
  removeKnownNouns(
  removeShortStrings(
  removeHTMLElements(
  removeCSSElements(
  removePunctuationAndSpecialChar(
  normalizeNumber(
  removeHTMLCharacterEntities(
  normalizeCurrencySymbol(
  normalizeEmoticon(
  normalizeEmailAddress(
  normalizeURL(
  toLowerCase(dataCollectionDF.col("emailText")))))))))))))));

// Next we cache this new table for faster loading when using it again later!
dataColNormalizedDF.cache();
dataColNormalizedDF.createOrReplaceTempView("dataColNormalizedDF");

// COMMAND ----------

/*
 * Creating the MLlib Pipeline
 *
 * This is the processing and classification pipeline
 * 1) Indexer - converts category names into Indexes
 * 2) Tokenization - converts text into tokens (words)
 * 3) hashingTF - creates a term frequency matrix for every document. The role of the term frequency is to act as features of every email
 * 4) Logistic Regression Component - The actual classification algorithm we are planning on using to create our predictions. In our case, we are using Naive 
 * Bayes, but other Logisitic Regression options are available!
 */
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, StringIndexer, IndexToString}

// Splitting our dataCollection in to a random assortment of training and test data
val Array(trainingData, testData) = dataColNormalizedDF.randomSplit(Array(0.70, 0.30))

// Defining our Variables that we will use to tweak our machine learning algorithm
val NUM_FEATURES = 1500;
val SMOOTHING = 0.00001;
val MODEL_TYPE = "multinomial";

// Creating of Pipeline sections and defining their input and output columns
val indexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")
val tokenizer = new Tokenizer().setInputCol("normalizedEmailText").setOutputCol("tokens")
val hashingTF = new HashingTF().setInputCol("tokens").setOutputCol("features").setNumFeatures(NUM_FEATURES)

// Creating the Classifier section of our Pipeline
val naiveB = new NaiveBayes().setLabelCol("indexedLabel").setFeaturesCol("features").setSmoothing(SMOOTHING).setModelType(MODEL_TYPE)

// Actually BUILD the Pipeline
val pipeline = new Pipeline().setStages(Array(indexer, tokenizer, hashingTF, naiveB))

// Then create our model by fitting the training data to our pipeline
val model = pipeline.fit(trainingData)


// COMMAND ----------

// Create our Prediction from our model and testData
val prediction = model.transform(testData)

// Get totals of each email type to create accuracy percentages
val totalSpamEmails = prediction.select("*").where(prediction("indexedLabel")===1.0).count().toFloat;
val totalNonSpamEmails = prediction.select("*").where(prediction("indexedLabel")===0.0).count().toFloat;
val totalEmails = prediction.select("*").count().toFloat;

// Build variables for the Confusion Matrix
val truePositives = prediction.select("*").where(prediction("indexedLabel")===1.0).where(prediction("prediction")===1.0).count().toFloat;
val falsePositives = prediction.select("*").where(prediction("indexedLabel")===0.0).where(prediction("prediction")===1.0).count().toFloat;
val trueNegatives = prediction.select("*").where(prediction("indexedLabel")===1.0).where(prediction("prediction")===0.0).count().toFloat;
val falseNegatives = prediction.select("*").where(prediction("indexedLabel")===0.0).where(prediction("prediction")===0.0).count().toFloat;

// Calculate our Success at Predictions
val correctSpamPredictionRate = (truePositives / totalSpamEmails) * 100;
val correctNonSpamPredictionRate =  (falsePositives / totalNonSpamEmails) * 100;
val correctPredictionRate = ((truePositives + trueNegatives) / (totalEmails)) * 100;

// Print out our testing results and prediction percentages!
printf(s"""|========================== Confusion matrix ==================================
           |####################| %-15s                    %-15s
           |-------------+----------------------------------------------------------------
           |Predicted = spam    | %-15f                     %-15f
           |Predicted = nonspam | %-15f                     %-15f
           |==============================================================================
\n""".stripMargin, "Actual = spam", "Actual = nonspam", truePositives, falsePositives, trueNegatives, falseNegatives)
println("************************************************************************************************************");
println("PREDICTIONS:");
println("************************************************************************************************************");
println("Total Emails: " + totalEmails);
printf("Spam Prediction Accuracy: %.2f%%\n", correctSpamPredictionRate)
printf("Non-Spam Prediction Accuracy: %.2f%%\n", correctNonSpamPredictionRate)
printf("Overall Prediction Accuracy: %.2f%%\n", correctPredictionRate)
println("************************************************************************************************************\n");



// COMMAND ----------

// Saving out the PipelineModel for use in other notebooks!
val SAVE_PATH = "FileStore/fittedModel";
model.write.overwrite().save(SAVE_PATH);
