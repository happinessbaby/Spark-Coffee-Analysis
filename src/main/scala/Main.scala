import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.StdIn.readLine
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.io.Source
import org.apache.spark.ml.linalg.Vectors
// import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.PCA



object HiveTest5 {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark1 = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark1.sparkContext.setLogLevel("ERROR")
    //import spark1.implicits._
    println("created spark session")

    //create tables
    spark1.sql("CREATE TABLE IF NOT EXISTS Branches(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchA.txt' OVERWRITE INTO TABLE Branches")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchB.txt' INTO TABLE Branches")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchC.txt' INTO TABLE Branches")
    spark1.sql("CREATE TABLE IF NOT EXISTS BranchesA(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("CREATE TABLE IF NOT EXISTS BranchesB(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("CREATE TABLE IF NOT EXISTS BranchesC(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchA.txt' OVERWRITE INTO TABLE BranchesA")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchB.txt' OVERWRITE INTO TABLE BranchesB")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchC.txt' OVERWRITE INTO TABLE BranchesC")
    spark1.sql("CREATE TABLE IF NOT EXISTS BranchesAB(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchA.txt' OVERWRITE INTO TABLE BranchesAB")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchB.txt' INTO TABLE BranchesAB")
    spark1.sql("CREATE TABLE IF NOT EXISTS BranchesBC(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchB.txt' OVERWRITE INTO TABLE BranchesBC")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchC.txt' INTO TABLE BranchesBC")
    spark1.sql("CREATE TABLE IF NOT EXISTS BranchesAC(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchA.txt' OVERWRITE INTO TABLE BranchesAC")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/BranchC.txt' INTO TABLE BranchesAC")
    spark1.sql("CREATE TABLE IF NOT EXISTS Partitioned(beverages STRING) COMMENT 'A PARTITIONED BRANCH TABLE' PARTITIONED BY (branches STRING)")
    spark1.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark1.sql("INSERT OVERWRITE TABLE Partitioned PARTITION(branches) SELECT beverages,branches from Branches")
    spark1.sql("SELECT * FROM BranchesA")
    spark1.sql("CREATE TABLE IF NOT EXISTS CountA(beverages STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountA.txt' OVERWRITE INTO TABLE CountA" )
    spark1.sql("CREATE TABLE IF NOT EXISTS CountB(beverages STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountB.txt' OVERWRITE INTO TABLE CountB" )
    spark1.sql("CREATE TABLE IF NOT EXISTS CountC(beverages STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountC.txt' OVERWRITE INTO TABLE CountC" )
    spark1.sql("CREATE TABLE IF NOT EXISTS CountAC(beverages STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountA.txt' OVERWRITE INTO TABLE CountAC" )
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountC.txt' INTO TABLE CountAC" )
    spark1.sql("CREATE TABLE IF NOT EXISTS CountBC(beverages STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountB.txt' OVERWRITE INTO TABLE CountBC" )
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountC.txt' INTO TABLE CountBC" )
    spark1.sql("CREATE TABLE IF NOT EXISTS CountAB(beverages STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountB.txt' OVERWRITE INTO TABLE CountAB" )
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountA.txt' INTO TABLE CountAB" )
    spark1.sql("CREATE TABLE IF NOT EXISTS CountABC(beverages STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountB.txt' OVERWRITE INTO TABLE CountABC" )
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountA.txt' INTO TABLE CountABC" )
    spark1.sql("LOAD DATA LOCAL INPATH 'input/CountC.txt' INTO TABLE CountABC" )
    //finds the total number of beverages: 54
    //spark1.sql("SELECT COUNT(DISTINCT CountA.beverages) from CountA JOIN CountB On CountA.beverages=CountB.beverages JOIN CountC ON CountC.beverages=CountB.beverages")


    //creates a menu
    def menu(): Unit={
      println("Welcome, please choose from the menu to continue")
      var input = ""
      while (input!="E") {
        println("To view the total consumer counts, enter 1")
        println("To view the most consumed beverages, enter 2")
        println("To view the least consumed beverages, enter 3")
        println("To view the averaged consumed beverages, enter 4")
        println("To view all available beverages, enter 5")
        println("To view common available beverages, enter 6 ")
        println("To delete a row from record or add a note, enter 7")
        println("To see future trends, enter 8")
        println("To exit, enter E")
        input = readLine()
        if (input=="1") {
          scenario1()
        }
        else if (input=="2") {
          scenario2A()
        }
        else if (input=="3") {
          scenario2B()
        }
        else if (input=="4") {
          scenario2C()
        }
        else if (input=="5") {
          scenario3A()
        }
        else if (input=="6") {
          scenario3B()
        }
        else if (input=="7") {
          scenario5()
        }
        else if (input=="8") {
          scenarioFuture()
        }
        else if (input=="E"|input=="e") {
          System.exit(0)
        }
    }
    }
    menu()
  


    //finds total consumer counts for each branch
    def scenario1(): Unit={
      var input = readLine("To view the total number of consumers for each branch, please enter a branch number: ")
      System.out.flush
      if (input=="1") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')").show(60, false)
        spark1.sql("SELECT sum(CountA.count) as Total_Consumers FROM  BranchesA JOIN CountA ON BranchesA.beverages= CountA.beverages WHERE BranchesA.branches = 'Branch1'").show()
      }
      else if (input=="2") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesAC group by beverages)where array_contains(common_br, 'Branch2')").show(60, false)
        spark1.sql("SELECT sum(CountAC.count) as Total_Consumers FROM  BranchesAC JOIN CountAC ON BranchesAC.beverages= CountAC.beverages WHERE BranchesAC.branches = 'Branch2'").show()
    }
      else if (input=="3") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesB group by beverages)where array_contains(common_br, 'Branch3')").show(60, false)
        spark1.sql("SELECT sum(CountB.count) as Total_Consumers FROM  BranchesB JOIN CountB ON BranchesB.beverages= CountB.beverages WHERE BranchesB.branches = 'Branch3'").show()
      }
      else if (input=="4") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesC group by beverages)where array_contains(common_br, 'Branch4')").show(60, false)
        spark1.sql("SELECT sum(CountC.count) as Total_Consumers FROM  BranchesC JOIN CountC ON BranchesC.beverages= CountC.beverages WHERE BranchesC.branches = 'Branch4'").show()
      }
       else if (input=="5") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesAB group by beverages)where array_contains(common_br, 'Branch5')").show(60, false)
        spark1.sql("SELECT sum(CountAB.count) as Total_Consumers FROM  BranchesAB JOIN CountAB ON BranchesAB.beverages= CountAB.beverages WHERE BranchesAB.branches = 'Branch5'").show()
      }
       else if (input=="6") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM Branches group by beverages)where array_contains(common_br, 'Branch6')").show(60, false)
        spark1.sql("SELECT sum(CountABC.count) as Total_Consumers FROM  Branches JOIN CountABC ON Branches.beverages= CountABC.beverages WHERE Branches.branches = 'Branch6'").show()
      }
       else if (input=="7") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesBC group by beverages)where array_contains(common_br, 'Branch7')").show(60, false)
        spark1.sql("SELECT sum(CountBC.count) as Total_Consumers FROM  BranchesBC JOIN CountBC ON BranchesBC.beverages= CountBC.beverages WHERE BranchesBC.branches = 'Branch7'").show()
      }
      else if (input=="8") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesB group by beverages)where array_contains(common_br, 'Branch8')").show(60, false)
        spark1.sql("SELECT sum(CountB.count) as Total_Consumers FROM  BranchesB JOIN CountB ON BranchesB.beverages= CountB.beverages WHERE BranchesB.branches = 'Branch8'").show()
      }
      else if (input=="9") {
        var b = "Branch"+input
        spark1.sql("SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesAC group by beverages)where array_contains(common_br, 'Branch9')").show(60, false)
        spark1.sql("SELECT sum(CountAC.count) as Total_Consumers FROM  BranchesAC JOIN CountAC ON BranchesAC.beverages= CountAC.beverages WHERE BranchesAC.branches = 'Branch9'").show()
      }
    }



    //finds most consumed beverages
    def scenario2A(): Unit={
      var input = readLine("To view the most consumed beverage in each branch, please enter a branch number: ")
      spark1.sql("DROP VIEW BRANCH_BEVERAGES")
      if (input=="1") {
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountA.beverages, sum(CountA.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountA ON BRANCH_BEVERAGES.beverages= CountA.beverages group by CountA.beverages order by beverage_sum desc").show(3)
      }
      else if (input=="2"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesAC group by beverages)where array_contains(common_br, 'Branch2')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountAC.beverages, sum(CountAC.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountAC ON BRANCH_BEVERAGES.beverages= CountAC.beverages group by CountAC.beverages order by beverage_sum desc").show(3)
      }
      else if (input=="3"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesB group by beverages)where array_contains(common_br, 'Branch3')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountB.beverages, sum(CountB.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountB ON BRANCH_BEVERAGES.beverages= CountB.beverages group by CountB.beverages order by beverage_sum desc").show(3)
      }
      if (input=="4"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesC group by beverages)where array_contains(common_br, 'Branch4')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountC.beverages, sum(CountC.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountC ON BRANCH_BEVERAGES.beverages= CountC.beverages group by CountC.beverages order by beverage_sum desc").show(3)
      }
       if (input=="5"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesAB group by beverages)where array_contains(common_br, 'Branch5')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountAB.beverages, sum(CountAB.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountAB ON BRANCH_BEVERAGES.beverages= CountAB.beverages group by CountAB.beverages order by beverage_sum desc").show(3)
      }
    }

//finds least consumed beverages
def scenario2B(): Unit={
      var input = readLine("To view the least consumed beverage in each branch, please enter a branch number: ")
      spark1.sql("DROP VIEW BRANCH_BEVERAGES")
      if (input=="1") {
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountA.beverages, sum(CountA.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountA ON BRANCH_BEVERAGES.beverages= CountA.beverages group by CountA.beverages order by beverage_sum").show(3)
      }
      else if (2==2){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesAC group by beverages)where array_contains(common_br, 'Branch2')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountAC.beverages, sum(CountAC.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountAC ON BRANCH_BEVERAGES.beverages= CountAC.beverages group by CountAC.beverages order by beverage_sum").show(3)
      }
      else if (input=="3"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesB group by beverages)where array_contains(common_br, 'Branch3')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountB.beverages, sum(CountB.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountB ON BRANCH_BEVERAGES.beverages= CountB.beverages group by CountB.beverages order by beverage_sum").show(3)
      }
      if (input=="4"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesC group by beverages)where array_contains(common_br, 'Branch4')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("SELECT CountC.beverages, sum(CountC.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountB ON BRANCH_BEVERAGES.beverages= CountC.beverages group by CountC.beverages order by beverage_sum").show(3)
      }
    }


    //finds average consumed beverages
    def scenario2C(): Unit = {
      var input = readLine("To view the average consumed beverage in each branch, please enter a branch number: ")
      spark1.sql("DROP VIEW BRANCH_BEVERAGES")
      spark1.sql("DROP TABLE SUM_BEVERAGES")
      spark1.sql("Drop TABLE NEW")
      if (input=="1") {
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesA group by beverages)where array_contains(common_br, 'Branch1')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("CREATE TABLE SUM_BEVERAGES AS SELECT CountA.beverages, sum(CountA.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountA ON BRANCH_BEVERAGES.beverages= CountA.beverages group by CountA.beverages")
        spark1.sql("CREATE TABLE NEW SELECT *, ROW_NUMBER() OVER (ORDER BY beverage_sum) as row FROM SUM_BEVERAGES")
        spark1.sql("SELECT COUNT(*) FROM NEW").show()
        spark1.sql("SELECT beverages as average_consumed_beverage from NEW where row=10").show()
      }
      else if (input=="2"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesAC group by beverages)where array_contains(common_br, 'Branch2')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false) 
        spark1.sql("CREATE TABLE SUM_BEVERAGES AS SELECT CountAC.beverages, avg(CountAC.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountAC ON BRANCH_BEVERAGES.beverages= CountAC.beverages group by CountAC.beverages")
        spark1.sql("CREATE TABLE NEW SELECT *, ROW_NUMBER() OVER (ORDER BY beverage_sum) as row FROM SUM_BEVERAGES")
        spark1.sql("SELECT COUNT(*) FROM NEW").show()
        spark1.sql("SELECT beverages as average_consumed_beverage from NEW where row=25").show()
      }
      else if (input=="3"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesB group by beverages)where array_contains(common_br, 'Branch3')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false)       
        spark1.sql("CREATE TABLE SUM_BEVERAGES AS SELECT CountB.beverages, sum(CountB.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountB ON BRANCH_BEVERAGES.beverages= CountB.beverages group by CountB.beverages order by beverage_sum")
        spark1.sql("CREATE TABLE NEW SELECT *, ROW_NUMBER() OVER (ORDER BY beverage_sum) as row FROM SUM_BEVERAGES")
        spark1.sql("SELECT COUNT(*) FROM NEW").show()
        spark1.sql("SELECT beverages as average_consumed_beverage from NEW where row=17").show()
      }
      else if (input=="4"){
        var b = "Branch"+input
        spark1.sql("CREATE VIEW BRANCH_BEVERAGES AS SELECT beverages, common_br FROM (SELECT beverages, collect_set(branches) as common_br FROM BranchesC group by beverages)where array_contains(common_br, 'Branch4')")
        spark1.sql("SELECT * FROM BRANCH_BEVERAGES").show(60, false)        
        spark1.sql("CREATE TABLE SUM_BEVERAGES AS SELECT CountC.beverages, sum(CountC.count) as beverage_sum FROM BRANCH_BEVERAGES JOIN CountC ON BRANCH_BEVERAGES.beverages= CountC.beverages group by CountC.beverages order by beverage_sum")
        spark1.sql("CREATE TABLE NEW SELECT *, ROW_NUMBER() OVER (ORDER BY beverage_sum) as row FROM SUM_BEVERAGES")
        spark1.sql("SELECT COUNT(*) FROM NEW").show()
        spark1.sql("SELECT beverages as average_consumed_beverage from NEW where row=25").show()
      }
    }



    //finds average consumed beverages
    def scenario3A(): Unit={
      var correct = false
      while (correct == false) {
        var input = readLine("Please select branches to view their available beverages: ")
        var inputBranch = input.split(",")
        var exist = inputBranch.map(_.toInt).filter(_>9)
        if (exist.length!=0) {
          println("You've entered a branch that doesn't exist, try again")
        }
        else {
          correct = true
          spark1.sql("DROP VIEW ALL_AVAILABLE_BEVERAGES")
          var branchLength = inputBranch.length
          if (branchLength == 1) {
               var b1 = "Branch"+inputBranch(0)
              spark1.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages as available_beverages FROM Partitioned WHERE branches = '$b1'")
              spark1.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
          }
          else if (branchLength == 2) {
            var b1 = "Branch"+inputBranch(0)
            var b2 = "Branch"+inputBranch(1)
              spark1.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages as available_beverages FROM Partitioned WHERE branches = '$b1' UNION SELECT beverages FROM Partitioned WHERE branches = '$b2'")
              spark1.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
          }
          else if (branchLength == 3) {
            var b1 = "Branch"+inputBranch(0)
            var b2 = "Branch"+inputBranch(1)
            var b3 = "Branch"+inputBranch(2)
                spark1.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages FROM Partitioned WHERE branches = '$b1' UNION SELECT beverages FROM Partitioned WHERE branches ='$b2' UNION SELECT beverages FROM Partitioned WHERE branches ='$b2'")
                spark1.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
          }
          else if (branchLength == 4) {
            var b1 = "Branch"+inputBranch(0)
            var b2 = "Branch"+inputBranch(1)
            var b3 = "Branch"+inputBranch(2)
            var b4 = "Branch"+inputBranch(3)
                spark1.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages FROM Partitioned WHERE branches = '$b1' UNION SELECT beverages FROM Partitioned WHERE branches ='$b2' UNION SELECT beverages FROM Partitioned WHERE branches ='$b3' " +
                  s"UNION SELECT beverages FROM Partitioned WHERE branches ='$b4'")
                spark1.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
          }
          else if (branchLength == 5) {
            var b1 = "Branch"+inputBranch(0)
            var b2 = "Branch"+inputBranch(1)
            var b3 = "Branch"+inputBranch(2)
            var b4 = "Branch"+inputBranch(3)
            var b5 = "Branch"+inputBranch(4)
          spark1.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages FROM Partitioned WHERE branches = '$b1' UNION SELECT beverages FROM Partitioned WHERE branches ='$b2' UNION SELECT beverages FROM Partitioned WHERE branches ='$b3' " +
            s"UNION SELECT beverages FROM Partitioned WHERE branches ='$b4' UNION SELECT beverages FROM Partitioned WHERE branches ='$b5'")
            spark1.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
          }
            else if (branchLength == 6) {
            var b1 = "Branch"+inputBranch(0)
            var b2 = "Branch"+inputBranch(1)
            var b3 = "Branch"+inputBranch(2)
            var b4 = "Branch"+inputBranch(3)
            var b5 = "Branch"+inputBranch(4)
            var b6 = "Branch"+inputBranch(5)
          spark1.sql(s"CREATE VIEW ALL_AVAILABLE_BEVERAGES AS SELECT beverages FROM Partitioned WHERE branches = '$b1' UNION SELECT beverages FROM Partitioned WHERE branches ='$b2' UNION SELECT beverages FROM Partitioned WHERE branches ='$b3' " +
            s"UNION SELECT beverages FROM Partitioned WHERE branches ='$b4' UNION SELECT beverages FROM Partitioned WHERE branches ='$b5'UNION SELECT beverages FROM Partitioned WHERE branches ='$b6' ")
              spark1.sql("SELECT * FROM ALL_AVAILABLE_BEVERAGES").show(60)
          }
        
        }
      }
    }
  

    //finds common available beverages and creates partitions
    def scenario3B(): Unit={
     spark1.sql("DROP TABLE Pre_Partition")
     spark1.sql("DROP TABLE Post_Partition")
     spark1.sql("DROP TABLE Partitioned_Common_Beverages")
     var correct = false
      while (correct == false) {
        var input = readLine("Please select branches to view their common beverages: ")
        var branches = input.split(",")
        var exist = branches.map(_.toInt).filter(_>9)
        if (exist.length!=0) {
          println("You've entered a branch that doesn't exist, try again")
        }
        else {
          correct = true
          var branchLength = branches.length
          if (branchLength == 2) {
            var b1 = "Branch"+branches(0)
            var b2 = "Branch"+branches(1)
            spark1.sql(s"CREATE TABLE Pre_Partition AS SELECT beverages, common_branches FROM (SELECT beverages, collect_set(branches) as common_branches FROM Partitioned group by beverages) WHERE ARRAY_CONTAINS(common_branches, '$b1') AND ARRAY_CONTAINS(common_branches, '$b2')")
            spark1.sql("SELECT beverages FROM PRE_PARTITION").show(999)
            spark1.sql("CREATE TABLE Post_Partition AS SELECT beverages, partitioned_branches FROM Pre_Partition LATERAL VIEW explode(common_branches) Post_Partition AS partitioned_branches")
            spark1.sql("CREATE TABLE  Partitioned_Common_Beverages(beverages STRING) PARTITIONED BY (partitioned_branches STRING)")
            spark1.sql("set hive.exec.dynamic.partition.mode=nonstrict")
            spark1.sql("INSERT OVERWRITE TABLE Partitioned_Common_Beverages PARTITION(partitioned_branches) SELECT beverages, partitioned_branches from Post_Partition")
            spark1.sql(s"SELECT * FROM Partitioned_Common_Beverages where partitioned_branches='$b1' OR partitioned_branches='$b2'").show(999)
          }
          if (branchLength == 3) {
            var b1 = "Branch"+branches(0)
            var b2 = "Branch"+branches(1)
            var b3 = "Branch"+branches(2)
            spark1.sql(s"CREATE TABLE Pre_Partition AS SELECT beverages, common_branches FROM (SELECT beverages, collect_set(branches) as common_branches FROM Partitioned group by beverages) WHERE ARRAY_CONTAINS(common_branches, '$b1') AND ARRAY_CONTAINS(common_branches, '$b2') AND  ARRAY_CONTAINS(common_branches, '$b3')")
            spark1.sql("SELECT beverages FROM PRE_PARTITION").show(999)
            spark1.sql("CREATE TABLE  Post_Partition AS SELECT beverages, partitioned_branches FROM Pre_Partition LATERAL VIEW explode(common_branches) Post_Partition AS partitioned_branches")
            spark1.sql("CREATE TABLE Partitioned_Common_Beverages(beverages STRING) COMMENT 'A PARTITIONED COMMON BEVERAGES TABLE' PARTITIONED BY (partitioned_branches STRING)")
            spark1.sql("set hive.exec.dynamic.partition.mode=nonstrict")
            spark1.sql("INSERT OVERWRITE TABLE Partitioned_Common_Beverages PARTITION(partitioned_branches) SELECT beverages, partitioned_branches from Post_Partition")
            spark1.sql(s"SELECT * FROM Partitioned_Common_Beverages where partitioned_branches='$b1' OR partitioned_branches='$b2' OR partitioned_branches='$b3'").show(999)
           }
           if (branchLength == 4) {
            var b1 = "Branch"+branches(0)
            var b2 = "Branch"+branches(1)
            var b3 = "Branch"+branches(2)
            var b4 = "Branch"+branches(3)
            spark1.sql(s"CREATE TABLE Pre_Partition AS SELECT beverages, common_branches FROM (SELECT beverages, collect_set(branches) as common_branches FROM Partitioned group by beverages) WHERE ARRAY_CONTAINS(common_branches, '$b1') AND ARRAY_CONTAINS(common_branches, '$b2') AND  ARRAY_CONTAINS(common_branches, '$b3') AND " +
              s"ARRAY_CONTAINS(common_branches, '$b4')")
            spark1.sql("SELECT beverages FROM PRE_PARTITION").show(999)
            spark1.sql("CREATE TABLE Post_Partition AS SELECT beverages, partitioned_branches FROM Pre_Partition LATERAL VIEW explode(common_branches) Post_Partition AS partitioned_branches")
            spark1.sql("CREATE TABLE Partitioned_Common_Beverages(beverages STRING) COMMENT 'A PARTITIONED COMMON BEVERAGES TABLE' PARTITIONED BY (partitioned_branches STRING)")
            spark1.sql("set hive.exec.dynamic.partition.mode=nonstrict")
            spark1.sql("INSERT OVERWRITE TABLE Partitioned_Common_Beverages PARTITION(partitioned_branches) SELECT beverages, partitioned_branches from Post_Partition")
            spark1.sql(s"SELECT * FROM Partitioned_Common_Beverages where partitioned_branches='$b1' OR partitioned_branches='$b2' OR partitioned_branches='$b3' OR partitioned_branches = '$b4'").show(999)
           }
          }
      }
  }

  //deletes rows and adds comments
  def scenario5(): Unit={
    spark1.sql("DROP TABLE OLD")
    spark1.sql("DROP TABLE NEW")
    println("Here is a consumer count table: ")
    spark1.sql("CREATE TABLE OLD AS SELECT *, CAST(ROW_NUMBER() OVER (ORDER BY beverages) AS INT) AS row_num FROM CountA")
    spark1.sql("SELECT * FROM OLD").show(99)
    var input2 = readLine("Enter the row that you'd like to remove: ")
    var input1 = readLine("You're about to delete a row, please add a note: ")
    spark1.sql(s"CREATE TABLE  NEW AS SELECT * FROM OLD WHERE row_num != '$input2'")
     spark1.sql(s"ALTER TABLE NEW SET TBLPROPERTIES ('notes' = '$input1')")
    //spark1.sql("ALTER TABLE NEW REPLACE COLUMNS(row_num, INT)")
    //shows unix timestamp which notes is added: https://www.unixtimestamp.com/
    spark1.sql("SHOW TBLPROPERTIES NEW").show()
    spark1.sql("SELECT * FROM NEW").show(99)
  }



  def scenarioFuture(): Unit = {
  import spark1.implicits._

    val file = Source.fromFile("input/CountACut.txt")
    var coffee = ListBuffer[(String, Integer)]()
    for (line<-file.getLines) {
      val a = line.split(",")
      coffee.append(Tuple2(a(0), a(1).toInt))
    }
    file.close
    var oddDays = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
    var evenDays = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
    //var compare = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0))
    var daily = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0,0))
    var rdd = spark1.sparkContext.parallelize(coffee.toList).groupByKey()
    var a = rdd.collect().foreach(x=>{
      var x0 = x._1
      var x1 = x._2
      //these beverages are sold daily
      if (x0=="SMALL_cappuccino"|x0=="MED_cappuccino"|x0=="LARGE_cappuccino"|x0=="COLD_cappuccino"|x0=="ICY_cappuccino"|x0=="Triple_cappuccino"|x0=="Mild_cappuccino"|
      x0=="Special_cappuccino"|x0=="Double_cappuccino") {
        var v =  Vectors.dense(x1.slice(0, 30).map(x=>x.toDouble).toVector.toArray)
        daily = daily :+ v
      }
      //these beverages are sold every other day
       else if (x0 == "Triple_Lite"|x0=="Mild_Coffee"|x0=="LARGE_MOCHA"|x0=="Cold_Espresso"|x0=="SMALL_Lite"|x0=="ICY_Coffee"|x0=="Double_LATTE"|
       x0=="Cold_Lite"|x0=="Special_Espresso"|x0=="ICY_MOCHA"|x0=="Double_Coffee"|x0=="MED_Espresso"|x0=="Special_Lite"|x0=="Triple_LATTE"|x0=="LARGE_Espresso"|x0=="Double_MOCHA"|
       x0=="MED_Lite"|x0=="SMALL_Espresso"|x0=="Mild_MOCHA") {
        var v =  Vectors.dense(x1.slice(0, 15).toSeq.map(x=>x.toDouble).toVector.toArray)
        oddDays = oddDays :+ v
        //compare = compare :+ v
      }
      //these beverages are sold every other day
       else  {
        var v =  Vectors.dense(x1.slice(0, 15).toSeq.map(x=>x.toDouble).toVector.toArray)
        //soldEven.appended(x0,  x1.toSeq.map(_.toDouble))
        evenDays = evenDays :+ v
        //compare = compare :+ v
      }
      println(s"coffee is $x0 and day counts are $x1")
    })


    
    var dfD = spark1.createDataFrame(daily.drop(1).map(Tuple1.apply)).toDF("features")
    dfD.show(false)
    var dfO = spark1.createDataFrame(daily.drop(1).map(Tuple1.apply)).toDF("features")
    dfO.show(false)
    var dfE = spark1.createDataFrame(daily.drop(1).map(Tuple1.apply)).toDF("features")
    dfE.show(false)


    
    val Row(coeff1: Matrix) = Correlation.corr(dfD, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(dfD, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")

    val pca = new PCA()

  .setInputCol("features")
  .setOutputCol("pcaFeatures")
  .setK(9)
  .fit(dfD)

val result = pca.transform(dfD).select("pcaFeatures")
result.show(false)


    
    //generates number of beverages sold on each branch 
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesA where branches = 'Branch1'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesAC where branches = 'Branch2'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesB where branches = 'Branch3'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesC where branches = 'Branch4'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesA where branches = 'Branch5'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM Branches where branches = 'Branch6'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesBC where branches = 'Branch7'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesB where branches = 'Branch8'").show()
    // spark1.sql("SELECT COUNT(DISTINCT beverages) FROM BranchesAC where branches = 'Branch9'").show()
  }
}
   
}

    

    

   




