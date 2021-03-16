import net.minidev.json.JSONUtil;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestConsumer {
    public static void main(String args[]) throws IOException, StreamingQueryException, TimeoutException {
        TeriMaaKaBosda teriMaaKaBosda = new TeriMaaKaBosda();
        teriMaaKaBosda.pullTopics();
    }
}

class TeriMaaKaBosda {
    public void pullTopics() throws StreamingQueryException, TimeoutException {
        /**
         Properties props = new Properties();
         props.put("bootstrap.servers", "localhost:9092");
         props.put("group.id", "channel");
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         KafkaConsumer<String, String> consumer =
         new KafkaConsumer<>(props);
         consumer.subscribe(Collections.singletonList("channel_1"));
         **/

        // Use Spark session to get kafka format topic data into datasets

        SparkSession session = SparkSession.builder().master("local[*]").appName("testing").getOrCreate();

        Dataset<Row> df = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "channel_1")
                .load();
        df.createOrReplaceTempView("network");
        Dataset<Row> sqlTable =session.sql("SELECT cast(value as string) FROM network");

        StreamingQuery row  = sqlTable.writeStream().foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                rowDataset.write()
                        .format("jdbc")
                        .mode("overwrite")
                        .option("url","jdbc:postgresql://localhost:5432/postgres")
                        .option("dbtable","network")
                        .option("driver","org.postgresql.Driver")
                        .option("user","postgres")
                        .option("password","postgres")
                        .save();
            }

        }).start();

        row.awaitTermination();


       /**Dataset<Row> jdbcDf = session.read()
                .format("jdbc")
                .option("url","jdbc:postgresql://localhost:5432/postgres")
                .option("dbtable","network")
                .option("driver","org.postgresql.Driver")
                .option("user","postgres")
                .option("password","postgres").load();**/


        
        //row.awaitTermination();




        //For each data row call Jdbc and conn with postgress

        // call insert query and insert data[0]


        // execute

        //done


        // try {
        //   while (true) {
        //     consumer.seekToBeginning(consumer.assignment());
        //      ConsumerRecords<String, String> records = consumer.poll(100);
        //      for (ConsumerRecord<String, String> record : records) {
        //     System.out.println(
        //             record.key());

        //    }
        //   }
        //  } finally {
        //       consumer.close();
        //   }

    }
}
