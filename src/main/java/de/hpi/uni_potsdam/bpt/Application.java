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
        // find the min price xetraTable and join together
        Table xetraResult = tblEnv.sqlQuery("SELECT * FROM " + xetraTable);
        Table xetraMin = tblEnv.sqlQuery("SELECT MIN(X1.price) FROM " + xetraResult + " AS X1 JOIN " + xetraResult + " AS X2 ON X1.stockName = X2.stockName" );

        //find the min price of nyseTable  and join together
        Table nyseResult = tblEnv.sqlQuery("SELECT * FROM "+ nyseTable);
        Table nyseMin = tblEnv.sqlQuery("SELECT N1.stockName as Name,MIN(N1.price) as Price FROM " + nyseResult + " AS N1 JOIN " + nyseResult + " AS N2 ON N1.stockName = N2.stockName GROUP BY N1.stockName" );

        // join xetraMin and nyseMin
        Table Minresult = tblEnv.sqlQuery("SELECT * FROM "+ nyseMin +"," + xetraMin);
        DataStream<Tuple2<Boolean, Tuple2<String, Integer>>> resultStream = tblEnv.toRetractStream(Minresult, Types.TUPLE(Types.STRING ,Types.INT,Types.INT));
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
