package P2;

public class FlinkApp {
    public static void main(String[] args) throws Exception{
        int size_window = 1;
        int flink_period = size_window * 10;

        FlinkThread[] flinkThreads = new FlinkThread [3];
        Thread threads[] = new Thread[3];

        for(int i = 0; i<1; i++){
            flinkThreads[i] = new FlinkThread ("currency-pair-"+(i+1)+"",flink_period);
            threads[i] = new Thread(flinkThreads[i]);

        }
        for(int i = 0; i<1; i++){
            threads[i].start();
        }

    }
}
