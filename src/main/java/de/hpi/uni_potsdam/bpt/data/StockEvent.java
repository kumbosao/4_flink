package de.hpi.uni_potsdam.bpt.data;

public class StockEvent {
    private String stockName;
    private int price;

    public StockEvent(String stockName, int price) {
        this.stockName = stockName;
        this.price = price;
    }

    public String getStockName() {
        return stockName;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "StockEvent{" +
                "StockName='" + stockName + '\'' +
                ", price=" + price +
                '}';
    }
}
