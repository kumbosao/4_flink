package de.hpi.uni_potsdam.bpt.sources;

import de.hpi.uni_potsdam.bpt.data.StockEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class StockStreamSource implements SourceFunction<StockEvent> {
    boolean isRunning = false;
    private String[] stockNames;
    private Map<String,Integer> recentPrice;
    private static final int INTER_EVENT_TIME = 1000;

    public StockStreamSource(String... stockNames) {
        super();
        this.stockNames = stockNames;
        this.recentPrice = new HashMap<>();
        initializePrices();
    }

    private void initializePrices() {
        Random random = new Random();
        for (String stock : stockNames) {
            recentPrice.put(stock, 30 + random.nextInt(50));
        }
    }

    @Override
    public void run(SourceContext<StockEvent> sourceContext) throws Exception {
        isRunning = true;
        Random random = new Random();
        while (isRunning) {
            String stock = stockNames[random.nextInt(stockNames.length)];
            int price = recentPrice.get(stock) - 10 + random.nextInt(20);
            final StockEvent event = new StockEvent(
                    stock,
                    price
            );
            sourceContext.collectWithTimestamp(event, System.nanoTime());
            Thread.sleep(INTER_EVENT_TIME);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
