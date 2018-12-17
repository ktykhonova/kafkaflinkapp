package P2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class FlatMapMinMax extends RichFlatMapFunction<ObjectNode, Tuple8<String, Double, Double, Double, Double, Double, Double, Integer>> {
    private int period;
    private transient int countMessage = 0;

    public FlatMapMinMax(int period) {
        this.period = period;
    }

    @Override
        public void flatMap(ObjectNode current,
                Collector<Tuple8<String, Double, Double, Double, Double, Double, Double, Integer>> out) throws Exception {

        JsonNode node = current.get("value");
        Tuple8<String, Double, Double, Double, Double, Double, Double, Integer> toInsert =
                new Tuple8<String, Double, Double, Double, Double, Double, Double, Integer>("",Double.MAX_VALUE,0.0,Double.MAX_VALUE,0.0,Double.MAX_VALUE,0.0,0);//tupleValues.value();
        countMessage++;

        toInsert.f0 = node.get("symbol").toString();

        double price = Double.parseDouble(node.get("price").toString().replace("\"", ""));
        System.out.println(price);
        if (price < toInsert.f1)
             toInsert.f1 = price;
        if (price > toInsert.f2)
            toInsert.f2 = price;

        double ask = Double.parseDouble(node.get("ask").toString().replace("\"", ""));
        if (ask < toInsert.f3)
            toInsert.f3 = ask;
        if (ask > toInsert.f4)
            toInsert.f4 = ask;

        double bid = Double.parseDouble(node.get("bid").toString().replace("\"", ""));
        if (bid < toInsert.f5)
            toInsert.f5 = bid;
        if (bid > toInsert.f6)
            toInsert.f6 = bid;

        toInsert.f7 = Integer.parseInt(node.get("bid").toString().replace("\"", ""));

        System.out.println("WinFun flatmap counted min max");
        if (countMessage % this.period == 0) {
            try {

            Class.forName("com.mysql.jdbc.Driver");

            // STEP 3: Open a connection
            System.out.print("\nConnecting to database...");
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/currency_pairs", "root", "Trfnthbyf1996");
            System.out.println(" SUCCESS!\n");

            System.out.print("\nInserting records into table...");
            Statement stmt = conn.createStatement();

            String sql = "INSERT INTO currency_pair (id, currencies_name, min_price, max_price, min_bid, max_bid, min_ask, max_ask, timestmp)" +
                    "VALUES ("+countMessage+", "
                    + toInsert.f0 +","
                    + toInsert.f1 +","
                    + toInsert.f2 +","
                    + toInsert.f3 +","
                    + toInsert.f4 +","
                    + toInsert.f5 +","
                    + toInsert.f6 +","
                    + toInsert.f7 +")";
                System.out.println(sql);
            stmt.executeUpdate(sql);

            System.out.println("SUCCESSFULLY inserted!\n");
        }catch (Exception e){
                System.out.println("Error: "+e.getMessage());
            }

    }

        out.collect(new Tuple8<>(toInsert.f0, toInsert.f1, toInsert.f2, toInsert.f3, toInsert.f4, toInsert.f5, toInsert.f6, toInsert.f7));


     }

    /*@Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple8<String, Double, Double, Double, Double, Double, Double, Integer>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple8<String, Double, Double, Double, Double, Double, Double, Integer>>() {
                        }), // type information
                        Tuple8.of("", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,0));
        tupleValues = getRuntimeContext().getState(descriptor);
        System.out.println("I'm in FlatMapMinMax - after tuple values");
    }*/
}
