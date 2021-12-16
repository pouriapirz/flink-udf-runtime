package functions;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;

import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public class FlinkUDFApp {
    public static void main(String[] args) throws Exception {
        final String udfJarFile = "/Users/pouria/Desktop/demo/udf_lib/flink-udf-1.0-SNAPSHOT.jar";
        final String udfClassName = "demo.DSUpperCase";
        final String udfName = "ds_uppercase";

        // Configure execution env
        final Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.JARS, Collections.singletonList("file://" + udfJarFile));
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        // Register UDF
        ClassLoader loader = URLClassLoader.newInstance(
            new URL[] { new File(udfJarFile).toURI().toURL() },
            ClassLoader.getSystemClassLoader());
        Class<? extends UserDefinedFunction> udfClass = (Class<? extends UserDefinedFunction>) Class.forName(udfClassName, true, loader);
        tableEnv.createTemporarySystemFunction(udfName, udfClass);

        // Sample Flink job:
        // 1. Setup Kafka consumer
        final String srcTopic = "pageviews";
        final String sinkTopic = "udfSink";
        final String kafkaUri = "localhost:9092";
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        final JsonRowDeserializationSchema.Builder srcBuilder = new JsonRowDeserializationSchema.Builder(
            Types.ROW_NAMED(new String[]{"viewtime", "userid", "pageid"}, Types.LONG, Types.STRING, Types.STRING));
        final FlinkKafkaConsumer<Row> kafkaConsumer = new FlinkKafkaConsumer<>(
            srcTopic,
            srcBuilder.ignoreParseErrors().build(),
            props);
        final DataStream<Row> ds = streamEnv.addSource(kafkaConsumer);

        // 2. Query using table api
        /*
        SELECT viewtime, userid, ds_uupercase(pageid) AS message
        FROM pageviews
        WHERE ds_uppercase(userid) = 'USER_5';
        */
        final Table tab = tableEnv.fromDataStream(ds)
            .filter(call(udfName, $("userid")).isEqual("USER_5"))
            .select($("viewtime"), $("userid"), call(udfName, $("pageid")).as("message"));

        // 3. Setup Kafka producer
        final JsonRowSerializationSchema.Builder sinkBuilder =  JsonRowSerializationSchema.builder();
        sinkBuilder.withTypeInfo(Types.ROW_NAMED(new String[]{"viewtime", "userid", "message"}, Types.LONG, Types.STRING, Types.STRING));
        final FlinkKafkaProducer<Row> kafkaProducer = new FlinkKafkaProducer<>(
            sinkTopic,
            sinkBuilder.build(),
            props);
        tableEnv.toDataStream(tab).addSink(kafkaProducer);

        // 4. Run Flink job
        streamEnv.execute();
    }
}
