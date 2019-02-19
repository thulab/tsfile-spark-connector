package cn.edu.tsinghua.tsfile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class query {
    public static void main(String[] args) {

        String filePath = "src/test/resources/test.tsfile";

        String sql = "select * from tsfile where time < 100";

        if(args.length == 2) {
            filePath = args[0];
            sql = args[1];
        }

        SparkSession spark = SparkSession.builder().appName("tsfile-query").master("spark://fit-17:7077").getOrCreate();
        Dataset<Row> tsfileDataSet = spark.read().format("cn.edu.tsinghua.tsfile").load(filePath);

        tsfileDataSet.createOrReplaceTempView("tsfile");


        long timeConsumption;

        long startTime = System.currentTimeMillis();

        Dataset<Row> dataset = spark.sql(sql);

//        dataset.show(10000);

//        dataset.count();
        dataset.collect();

        timeConsumption = System.currentTimeMillis() - startTime;
        System.out.println(String.format("Time consumption: %dms", timeConsumption));
    }
}
