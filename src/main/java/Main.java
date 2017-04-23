import static org.apache.spark.sql.functions.desc;

import java.util.Arrays;

import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

public class Main {

    public static void main(String[] args) throws Exception {

        String apiKey = "QFKYIB0rcjNOn0cO9bIfn5QRg";
        String apiSecret = "gAguz7Iw4J1uGtnyir6JLnRy0h7ftH2dm3BMtNTACgHRF3VOOl";
        String accessToken = "850685620796227584-ncWVgjdfO1bEQARuxcSFQybhSWTto9x";
        String accessTokenSecret = "HRYSqgwC8gCiPqKNYHz1PXkiOlWJXqP5BZ21QuCeAnDPi";
        TutorialHelper.configureTwitterCredentials(apiKey,apiSecret,accessToken,accessTokenSecret);


        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);
        SparkConf sparkConf = new SparkConf().setAppName("Tweets Android").setMaster("local[2]");


        JavaStreamingContext sc=new JavaStreamingContext(sparkConf,new Duration(5000));
        String[] filters={"#Android"};
                TwitterUtils.createStream(sc,twitterAuth,filters)
                        .window(new Duration(120000), new Duration(5000))
                        .map(Status::getText)
                        .flatMap(s -> Arrays.asList(StringUtils.split(s," .,\n!?")).iterator())
                        .map(String::toLowerCase)
                        .foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {

                            SparkSession sparkSession = SparkSession.builder()
                                    .config(stringJavaRDD.context().conf())
                                    .getOrCreate();

                            JavaRDD<JavaRow> rowRDD = stringJavaRDD.map((Function<String, JavaRow>) word -> {
                                JavaRow record = new JavaRow();
                                record.setWord(word);
                                return record;
                            });

                            Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRDD, JavaRow.class);

                            dataFrame.createOrReplaceTempView("words");

                            Dataset wordCountsDataFrame =
                                    sparkSession.sql("select word, count(*) as total from words group by word").sort(desc("total"));
                            wordCountsDataFrame.show(10);
                        });
        sc.start();
        sc.awaitTermination();

    }
}
