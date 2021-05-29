package de.hpi.uni_potsdam.bpt;

import de.hpi.uni_potsdam.bpt.data.StockEvent;
import de.hpi.uni_potsdam.bpt.sources.StockStreamSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class Application {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tblEnv;
    private Table xetraTable;
    private Table nyseTable;

    public Application() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.tblEnv = StreamTableEnvironment.create(env);
    }

    private void registerSources() {
        DataStreamSource<StockEvent> xetraStream = env.addSource(new StockStreamSource("Google", "Microsoft", "Yahoo", "SAP", "Zoom"));
        DataStream<Tuple2<String, Integer>> xetraTuple = xetraStream.map(event -> new Tuple2<String, Integer>(event.getStockName(), event.getPrice())).returns(Types.TUPLE(Types.STRING, Types.INT));
        xetraTable = tblEnv.fromDataStream(xetraTuple, "stockName, price");
        DataStreamSource<StockEvent> nyseStream = env.addSource(new StockStreamSource("Google", "Microsoft", "Yahoo", "SAP", "Zoom"));
        DataStream<Tuple2<String, Integer>> nyseTuple = nyseStream.map(event -> new Tuple2<String, Integer>(event.getStockName(), event.getPrice())).returns(Types.TUPLE(Types.STRING, Types.INT));
        nyseTable = tblEnv.fromDataStream(nyseTuple, "stockName, price");
    }

    public static void main(String[] args) throws Exception {
        // Setup
        Application app = new Application();
        app.registerSources();
        app.runQueries();
    }

    // Passen Sie diese Funktion entsprechend der Aufgabenstellung an
    private void runQueries() throws Exception {
        Table result1 = tblEnv.sqlQuery("SELECT * FROM " + xetraTable);




       /* DataStream<Tuple2<Boolean, Tuple2<String, Integer>>> resultStream = tblEnv.toRetractStream(result, Types.TUPLE(Types.STRING, Types.INT));
        resultStream.addSink(new SinkFunction<Tuple2<Boolean, Tuple2<String, Integer>>>() {
            @Override
            public void invoke(Tuple2<Boolean, Tuple2<String, Integer>> value, Context context) throws Exception {
                System.out.println("New Result: " + (value.f0 ? "added " : "deleted ") + "tuple " + value.f1);
            }
        }); */
        Table result = tblEnv.sqlQuery("SELECT MIN(R1.price) FROM " + result1 + " AS R1 JOIN " + result1 + " AS R2 ON R1.stockName = R2.stockName" );
        DataStream<Tuple2<Boolean, Tuple2<String, Integer>>> resultStream = tblEnv.toRetractStream(result, Types.TUPLE(Types.INT));
        resultStream.addSink(new SinkFunction<Tuple2<Boolean, Tuple2<String, Integer>>>() {
            @Override
            public void invoke(Tuple2<Boolean, Tuple2<String, Integer>> value, Context context) throws Exception {
                System.out.println("New Result: " + (value.f0 ? "added " : "deleted ") + "tuple " + value.f1);
            }
        });


        // Start the execution of Flink
        env.execute();
    }
}
