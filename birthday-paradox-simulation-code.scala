```scala
// Databricks notebook source

// DBTITLE 1,Libraries
// Importing the required libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.temporal.ChronoUnit
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Date
import java.sql.Date

// Initializing Spark session
val spark = SparkSession.builder.appName("BirthdayParadox").getOrCreate()

// Path to the json file
val filePath = "dbfs:/FileStore/BirthdayParadox.json"

// DBTITLE 1,Schema
// Defining the schema for the json file
val schema = StructType(Array(
  StructField("updated", StringType, true),
  StructField("count", IntegerType, true),
  StructField("recent", ArrayType(StructType(Array(
    StructField("day", IntegerType, true),
    StructField("timestamp", LongType, true),
    StructField("ago", StringType, true)
  ))), true),
  StructField("binnedDay", ArrayType(StructType(Array(
    StructField("key", IntegerType, true),
    StructField("value", IntegerType, true)
  ))), true),
  StructField("binnedGuess", ArrayType(StructType(Array(
    StructField("key", IntegerType, true),
    StructField("value", IntegerType, true)
  ))), true),
  StructField("tally", ArrayType(BooleanType, true), true),
  StructField("binnedTally", ArrayType(StructType(Array(
    StructField("key", BooleanType, true),
    StructField("value", IntegerType, true)
  ))), true)
))

// Loading the json file into a DF
val dfWithSchema = spark.read
  .option("multiline", "true") // Multiline json files
  .schema(schema)
  .json(filePath)

// DBTITLE 1,Flattening DFs
// Flattening the DFs
val recentDF = dfWithSchema.select(explode(col("recent")).as("recent"))
  .select(col("recent.day").as("recent_day"), col("recent.timestamp").as("recent_timestamp"), col("recent.ago").as("recent_ago"))
recentDF.show(4)

val binnedDayDF = dfWithSchema.select(explode(col("binnedDay")).as("binnedDay"))
  .select(col("binnedDay.key").as("binnedDay_key"), col("binnedDay.value").as("binnedDay_value"))
binnedDayDF.show(4)

val binnedGuessDF = dfWithSchema.select(explode(col("binnedGuess")).as("binnedGuess"))
  .select(col("binnedGuess.key").as("binnedGuess_key"), col("binnedGuess.value").as("binnedGuess_value"))
binnedGuessDF.show(4)

val tallyDF = dfWithSchema.select(explode(col("tally")).as("tally"))
tallyDF.show(4)

val binnedTallyDF = dfWithSchema.select(explode(col("binnedTally")).as("binnedTally"))
  .select(col("binnedTally.key").as("binnedTally_key"), col("binnedTally.value").as("binnedTally_value"))
binnedTallyDF.show(4)

// DBTITLE 1,Defining the dates
// Function to convert recent_day to actual date
def dayToDate(startDate: String, daysToAdd: Int): Date = {
  val startLocalDate = LocalDate.parse(startDate)
  val resultLocalDate = startLocalDate.plusDays(daysToAdd)
  Date.valueOf(resultLocalDate)
}

// Registering the function as a UDF
val startDate = "2023-01-01"
val dateUDF = udf((daysToAdd: Int) => dayToDate(startDate, daysToAdd))

// Applying the UDF to create the new columns
val recentWithDateDF = recentDF.withColumn("date", date_format(dateUDF(col("recent_day")), "ddMMyyyy"))
  .withColumn("day", dayofmonth(to_date(col("date"), "ddMMyyyy")))
  .withColumn("month", month(to_date(col("date"), "ddMMyyyy")))
  .withColumn("year", year(to_date(col("date"), "ddMMyyyy")))

recentWithDateDF.show(10, false)

// DBTITLE 1,File per day
// Creating directories per month and writing data per day
recentWithDateDF.groupBy("year", "month", "day").count().collect().foreach { row =>
  val year = row.getAs[Int]("year")
  val month = row.getAs[Int]("month")
  val day = row.getAs[Int]("day")
  val count = row.getAs[Long]("count")
  
  // Filtering the DataFrame for the specific day
  val dayDF = recentWithDateDF.filter(col("year") === year && col("month") === month && col("day") === day)
  
  dayDF.show(10)
  
  // Defining the path
  val path = s"dbfs:/FileStore/Birthdays/year=$year/month=$month/"
  
  // Writing the file with the count in the filename
  dayDF.write.mode(SaveMode.Overwrite).json(s"${path}day=$day-$count.json")
}

// DBTITLE 1,Even and odd days
// Grouping data by day and counting the number of people with birthdays on each day
val groupedDF = recentWithDateDF.groupBy("year", "month", "day").agg(count("recent_day").as("num_people"), when(count("recent_day") > 1, lit(true)).otherwise(lit(false)).as("shares_birthday"))

// Filtering even and odd days
val evenDaysDF = groupedDF.filter(col("day") % 2 === 0).orderBy("year", "month", "day")
val oddDaysDF = groupedDF.filter(col("day") % 2 =!= 0).orderBy("year", "month", "day")

evenDaysDF.show(10, false)
oddDaysDF.show(10, false)

// Defining the path for even and odd days
val evenDaysPath = "dbfs:/FileStore/Birthdays/evendays.json"
val oddDaysPath = "dbfs:/FileStore/Birthdays/odddays.json"

// Writing the even and odd days DF to their files
evenDaysDF.write.mode(SaveMode.Overwrite).json(evenDaysPath)
oddDaysDF.write.mode(SaveMode.Overwrite).json(oddDaysPath)

// DBTITLE 1,Shared Birthdays
// Initial grouping to identify shared birthdays
val sharedBirthdaysDF = recentWithDateDF.groupBy("year", "month", "day").agg(count("recent_day").as("num_people")).filter(col("num_people") > 1) // More than one birthday

// Summing the shared birthdays per month
val sharedBirthdaysPerMonthDF = sharedBirthdaysDF.groupBy("year", "month").agg(sum("num_people").as("shared_birthdays"))

// Grouping data by year and month to count the total number of people with birthdays
val monthSummaryDF = recentWithDateDF.groupBy("year", "month").agg(count("recent_day").as("total_people"))

// Joining the month summary with the shared birthdays summary
val finalMonthSummaryDF = monthSummaryDF.join(sharedBirthdaysPerMonthDF, Seq("year", "month"), "left").na.fill(0, Seq("shared_birthdays")) // Fill null values with 0

finalMonthSummaryDF.show(12, false)

// DBTITLE 1,Day Key Values
// Grouping by day within each month to create the key-value pairs
val numPeopleDF = recentWithDateDF.groupBy("date").agg(count("recent_day").as("num_people"))

// Creating key-value pairs based on the number of people sharing the birthday
val keyValueDF = numPeopleDF.withColumn("key_value", 
  when(col("num_people") === 1, concat(col("date"), lit(": only 1 person has this birthday")))
  .otherwise(concat(col("date"), lit(": "), col("num_people"), lit(" people share birthday")))) 

// Extracting year and month from the date column using to_date
val keyValueWithYearMonthDF = keyValueDF.withColumn("year", year(to_date(col("date"), "ddMMyyyy"))).withColumn("month", month(to_date(col("date"), "ddMMyyyy"))) 

// Grouping by year and month to collect the key-value pairs into a list
val dayKeyValueDF = keyValueWithYearMonthDF.groupBy("year", "month").agg(collect_list("key_value").as("day_key_values"))

dayKeyValueDF.show(4, 150, false)

// DBTITLE 1,Final DF
// Joining the two summaries
val monthBirthdaySummaryDF = finalMonthSummaryDF.join(dayKeyValueDF, Seq("year", "month"))

val finalDF = monthBirthdaySummaryDF.select(
  col("year"),
  col("month"),
  col("total_people"),
  col("shared_birthdays"),
  col("day_key_values")
).orderBy("month")

finalDF.show(12, 120, false)
