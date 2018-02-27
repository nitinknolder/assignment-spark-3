package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

//HomeTeam = Home Team
//AwayTeam = Away Team
//FTHG and HG = Full Time Home Team Goals
//FTAG and AG = Full Time Away Team Goals
//FTR and Res = Full Time Result (H=Home Win, D=Draw, A=Away Win)
object SoccerMatches {
  def main (args: Array[String]): Unit = {
    Logger.getLogger ("org").setLevel (Level.OFF)
    val log = Logger.getLogger (this.getClass)
    val sparkConf: SparkConf = new SparkConf ().setAppName ("sparkAssignment3").setMaster ("local[*]")
    val sparkSession: SparkSession = SparkSession.builder ().config (sparkConf).getOrCreate ()

    /** **********************************************(Q1.a (Creating DataFrame)) ******************************************************/
    log.info ("DataFrame Created...")
    val creatingDataFrame: DataFrame = sparkSession.read.option ("header", "true").option ("inferSchema", "true").csv ("src/main/resources/Football.csv")
    creatingDataFrame.select ("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").show ()

    /** *****************************************************(Q1.b(Number Of matches Played by Home team)) **************************************************/
    creatingDataFrame.createOrReplaceTempView ("MatchedPlayedAsHomeTeam")
    val playedAsHome = sparkSession.sql ("SELECT HomeTeam,count(1) As HomeCounts FROM MatchedPlayedAsHomeTeam GROUP  BY HomeTeam")
    playedAsHome.createOrReplaceTempView ("PlayedAsHomeTeam")
    playedAsHome.show ()

    /** *****************************************************(Q1.c(Top 10 Teams With Highest % of Wins)) *******************************/
    creatingDataFrame.createOrReplaceTempView ("MatchedPlayedAsAwayTeam")
    val playedAsAway = sparkSession.sql ("SELECT AwayTeam,count(1) As AwayCounts FROM MatchedPlayedAsAwayTeam GROUP BY AwayTeam")
    playedAsAway.createOrReplaceTempView ("PlayedAsAwayTeam")

    creatingDataFrame.createOrReplaceTempView ("HomeWins")
    val noOfHomeWins = sparkSession.sql ("SELECT HomeTeam,COUNT(1) As homeWins FROM HomeWins WHERE FTR = 'H' GROUP BY HomeTeam")
    noOfHomeWins.createOrReplaceTempView ("NoOfHomeWins")

    creatingDataFrame.createOrReplaceTempView ("AwayWins")
    val noOfAwayWins = sparkSession.sql ("SELECT AwayTeam,COUNT(1) As awayWins FROM AwayWins WHERE FTR = 'A' GROUP BY AwayTeam")
    noOfAwayWins.createOrReplaceTempView ("NoOfAwayWins")

    val totalNumberOfMatches = playedAsHome.join (playedAsAway, playedAsHome ("HomeTeam") === playedAsAway ("AwayTeam"))
    totalNumberOfMatches.createOrReplaceTempView ("numOfMatches")
    sparkSession.sql ("SELECT HomeTeam As matches,(HomeCounts + AwayCounts) As Total From numOfMatches").createOrReplaceTempView ("AllMatches")

    val totalNumberOfWins = noOfHomeWins.join (noOfAwayWins, noOfHomeWins ("HomeTeam") === noOfAwayWins ("AwayTeam"))
    totalNumberOfWins.createOrReplaceTempView ("numOfWins")
    sparkSession.sql ("SELECT HomeTeam As wining,(homeWins + awayWins) As Wins From numOfWins").createOrReplaceTempView ("AllWins")

    val result = sparkSession.sql (
      "SELECT matches As Team,(Wins/numOfMatches) * 100)" +
        "As highestPercentage FROM " +
        "AllWins" +
        "ORDER BY highestPercentage DESC LIMIT 10")
    result.show ()

    /** ************************************************************ DataSets *********************************************************/
    /* Ques4: Convert the DataFrame created in Q1 to DataSet by using
only following fields. */

    import sparkSession.implicits._
    val four = 4
    val five = 5
    val six = 6
    val convertToDataSet = creatingDataFrame.map (row => FootballDatabase (row.getString (2), row.getString (3), row.getInt (four),
      row.getInt (five), row.getString (six)))

    /* Ques5: Total number of match played by each team. */

    convertToDataSet.select ($"HomeTeam").union (convertToDataSet.select ($"AwayTeam")).groupBy ($"HomeTeam").count ().show ()

    /* Ques 6:  top ten Teams With Highest Number Of Wins */
    val homeTeam = convertToDataSet.select ("HomeTeam", "FTR").where ("FTR = 'H'").groupBy ("HomeTeam").count ().withColumnRenamed ("count", "HomeWins")
    val awayTeam = convertToDataSet.select ("AwayTeam", "FTR").where ("FTR = 'A'").groupBy ("AwayTeam").count ().withColumnRenamed ("count", "AwayWins")
    val teams = homeTeam.join (awayTeam, homeTeam.col ("HomeTeam") === awayTeam.col ("AwayTeam"))
    val add: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches
    val total = udf (add)
    val ten = 10
    teams.withColumn ("TotalWins", total (col ("HomeWins"), col ("AwayWins"))).select ("HomeTeam", "TotalWins")
      .withColumnRenamed ("HomeTeam", "Team").sort (desc ("TotalWins")).limit (ten).show ()

  }
}

