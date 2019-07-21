package com.frenhuit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Main {
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf().setAppName("HUBigData").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> initialRDD =
        sc.textFile(
            "D:\\Dropbox\\Dropbox\\IntelliJProjects\\Spark\\HUBigData\\resources\\winemag-data.csv");
    //    JavaRDD<String> initialRDD =
    //        sc.textFile(
    //            "s3://frenhuit-s3-1/winemag-data.csv");
    /*
    Title: id[0],country[1],description[2],designation[3],points[4],price[5],province[6],region_1[7],region_2[8],
    taster_name[9],taster_twitter_handle[10],title[11],variety[12],winery[13]
     */

    //    JavaPairRDD<String, Integer> testerRdd =
    //        initialRDD.mapToPair(
    //            value -> {
    //              String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    //              if (tokens.length == 14) {
    //                return new Tuple2<>(tokens[9], 1);
    //              }
    //              return new Tuple2<>("", 1);
    //            });
    //
    //    JavaPairRDD<String, Integer> testerCountRdd =
    //        testerRdd
    //            .filter(value -> value._1.length() > 0)
    //            .reduceByKey((value1, value2) -> value1 + value2)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1))
    //            .sortByKey(false)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1));

    //    testerCountRdd.collect().forEach(System.out::println);

    //    JavaPairRDD<String, Integer> wineryRdd =
    //        initialRDD.mapToPair(
    //            value -> {
    //              String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    //              if (tokens.length == 14) {
    //                return new Tuple2<>(tokens[13], 1);
    //              }
    //              return new Tuple2<>("", 1);
    //            });
    //
    //    JavaPairRDD<String, Integer> wineryCountRdd =
    //        wineryRdd
    //            .filter(value -> value._1.length() > 0)
    //            .reduceByKey((value1, value2) -> value1 + value2)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1))
    //            .sortByKey(false)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1));
    //
    //    wineryCountRdd.collect().forEach(System.out::println);

    //    JavaPairRDD<String, Float> wineryScoreRdd =
    //        initialRDD
    //            .mapToPair(
    //                value -> {
    //                  String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    //                  if (tokens.length == 14 && tokens[4].length() != 0) {
    //                    return new Tuple2<>(tokens[13], tokens[4]);
    //                  }
    //                  return new Tuple2<>("", "");
    //                })
    //            .filter(value -> value._1.length() > 0 && value._2 != null)
    //            .reduceByKey((score1, score2) -> score1 + "," + score2)
    //            .mapToPair(
    //                value -> {
    //                  String[] scoreStrings = value._2.split(",");
    //                  int totalScore = 0;
    //                  int totalCount = 0;
    //                  for (String scoreString : scoreStrings) {
    //                    if (scoreString.length() == 0) {
    //                      continue;
    //                    }
    //                    int score = Integer.parseInt(scoreString);
    //                    totalScore += score;
    //                    totalCount++;
    //                  }
    //                  float avgScore = (float) totalScore / (float) (totalCount);
    //                  if (totalCount > 20) {
    //                    return new Tuple2<>(value._1, avgScore);
    //                  }
    //                  return new Tuple2<>("", -1.0f);
    //                })
    //            .filter(value -> value._2 > 0f)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1))
    //            .sortByKey(false)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1));
    //
    //    wineryScoreRdd.collect().forEach(System.out::println);

    //    JavaPairRDD<String, Float> varietyScoreRdd =
    //        initialRDD
    //            .mapToPair(
    //                value -> {
    //                  String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    //                  if (tokens.length == 14 && tokens[4].length() != 0) {
    //                    return new Tuple2<>(tokens[12], tokens[4]);
    //                  }
    //                  return new Tuple2<>("", "");
    //                })
    //            .filter(value -> value._1.length() > 0 && value._2 != null)
    //            .reduceByKey((score1, score2) -> score1 + "," + score2)
    //            .mapToPair(
    //                value -> {
    //                  String[] scoreStrings = value._2.split(",");
    //                  int totalScore = 0;
    //                  int totalCount = 0;
    //                  for (String scoreString : scoreStrings) {
    //                    if (scoreString.length() == 0) {
    //                      continue;
    //                    }
    //                    int score = Integer.parseInt(scoreString);
    //                    totalScore += score;
    //                    totalCount++;
    //                  }
    //                  float avgScore = (float) totalScore / (float) (totalCount);
    //                  if (totalCount > 20) {
    //                    return new Tuple2<>(value._1, avgScore);
    //                  }
    //                  return new Tuple2<>("", -1.0f);
    //                })
    //            .filter(value -> value._2 > 0f)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1))
    //            .sortByKey(false)
    //            .mapToPair(value -> new Tuple2<>(value._2, value._1));
    //
    //    varietyScoreRdd.collect().forEach(System.out::println);

    /*
    Most 10 tested winery, their average price.
     */
    //    JavaPairRDD<String, Tuple2<Integer, Float>> wineryCountPriceRdd =
    //        initialRDD
    //            .mapToPair(
    //                value -> {
    //                  String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    //                  if (tokens.length == 14 && tokens[13].length() != 0 && tokens[5].length() !=
    // 0) {
    //                    return new Tuple2<>(tokens[13], new Tuple2<>(1, tokens[5]));
    //                  }
    //                  return new Tuple2<>("", new Tuple2<>(0, "-1"));
    //                })
    //            .filter(value -> value._1.length() > 0 && value._2._1 != 0)
    //            .mapToPair(
    //                value ->
    //                    new Tuple2<>(
    //                        value._1, new Tuple2<>(value._2._1, Float.parseFloat(value._2._2))))
    //            .reduceByKey(
    //                (tuple1, tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 +
    // tuple2._2))
    //            .mapToPair(
    //                value ->
    //                    new Tuple2<>(
    //                        value._1, new Tuple2<>(value._2._1, value._2._2 / (float)
    // value._2._1)))
    //            .mapToPair(value -> new Tuple2<>(value._2._1, new Tuple2<>(value._1,
    // value._2._2)))
    //            .sortByKey(false)
    //            .mapToPair(value -> new Tuple2<>(value._2._1, new Tuple2<>(value._1,
    // value._2._2)));
    //
    //    wineryCountPriceRdd.collect().forEach(System.out::println);

    /*
    Price distribution.
     */
    //    JavaRDD<Float> priceRdd =
    //        initialRDD
    //            .filter(
    //                value -> {
    //                  String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    //                  if (tokens.length == 14 && tokens[5].length() > 0) {
    //                    return true;
    //                  }
    //                  return false;
    //                })
    //            .map(value -> Float.parseFloat(value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",
    // -1)[5]))
    //            .sortBy(price -> price, true, 1);
    //    List<Float> prices = priceRdd.collect();
    //    int total = prices.size();
    //    for (int i = 0; i <= 100; i += 10) {
    //      System.out.print(i + "%: ");
    //      System.out.println(prices.get(total * i / 100 - (i == 0 ? 0 : 1)));
    //    }

    JavaPairRDD<String, Float> avgScoreForPrice =
        initialRDD
            .filter(
                value -> {
                  String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                  if (tokens.length == 14 && tokens[4].length() > 0 && tokens[5].length() > 0) {
                    return true;
                  }
                  return false;
                })
            .mapToPair(
                value -> {
                  String[] tokens = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                  float score = Float.parseFloat(tokens[4]);
                  float price = Float.parseFloat(tokens[5]);
                  String tag = priceTag(price);
                  return new Tuple2<>(tag, new Tuple2<>(1, score));
                })
            .reduceByKey(
                (value1, value2) -> new Tuple2<>(value1._1 + value2._1, value1._2 + value2._2))
            .mapToPair(value -> new Tuple2<>(value._1, value._2._2 / value._2._1))
            .sortByKey();

    avgScoreForPrice.collect().forEach(System.out::println);
  }

  static String priceTag(float price) {
    if (price >= 4.0 && price < 12.0) {
      return "04-12";
    } else if (price >= 12.0 && price < 15.0) {
      return "12-15";
    } else if (price >= 15.0 && price < 18.0) {
      return "15-18";
    } else if (price >= 18.0 && price < 22.0) {
      return "18-22";
    } else if (price >= 22.0 && price < 25.0) {
      return "25-25";
    } else if (price >= 25.0 && price < 30.0) {
      return "25-30";
    } else if (price >= 30.0 && price < 38.0) {
      return "30-38";
    } else if (price >= 38.0 && price < 48.0) {
      return "38-48";
    } else if (price >= 48.0 && price < 65.0) {
      return "48-65";
    } else {
      return "65+";
    }
  }
}
