package P1;

public class CurrencyPair {

    private String symbol = "";
    private double bid = 0.0;
    private double ask = 0.0;
    private double price = 0.0;
    private int timestamp = 0;
    private String jsonString;

    public String getSymbol() {
        return symbol;
    }
    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getBid() {
        return bid;
    }
    public void setBid(double bid) {
        this.bid = bid;
    }

    public double getAsk() {
        return ask;
    }
    public void setAsk(double ask) {
        this.ask = ask;
    }

    public double getPrice() {
        return price;
    }
    public void setPrice(double price) {
        this.price = price;
    }

    public String getJsonString() {
        return jsonString;
    }
    public void setJsonString(String jsonString) {
        this.jsonString = jsonString;
    }

    public int getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
}
