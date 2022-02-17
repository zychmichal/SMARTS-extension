package processor.util;

class TimeCollector {
    private final int NUMBER_OF_SAMPLES;
    private final String name;
    private long[] timeSamples;
    private int currentIndex;
    private long aggTime;
    private long maxTime;
    private long totalAggTime;

    TimeCollector(String name) {
        this.name = name;
        this.totalAggTime = 0L;
        this.NUMBER_OF_SAMPLES = WorkerTimings.NUMBER_OF_SAMPLES;
        this.timeSamples = new long[NUMBER_OF_SAMPLES];
        reset();
    }

    void collectElapsedTime(long elapsedTime){
        currentIndex %= NUMBER_OF_SAMPLES;

        timeSamples[currentIndex] = elapsedTime;
        currentIndex++;

        aggregateTime(elapsedTime);

        maxTime(elapsedTime);
    }

    @Override
    public String toString() {
        return "; " + name + "={" +
                "AVG=" + ((aggTime/1.0E6)/NUMBER_OF_SAMPLES) +
                "| AGG=" + (aggTime/1.0E6) +
                "| MAX=" + (maxTime/1.0E6) +
//                "| L50=" + Arrays.toString(timeSamples) +
                "}";
    }

    void reset(){
        for (int i = 0; i < NUMBER_OF_SAMPLES; ++i)
            timeSamples[i] = -1L;
        this.currentIndex = 0;
        this.aggTime = 0L;
        this.maxTime = 0L;
    }


    private void aggregateTime(long elapsedTime){
        aggTime += elapsedTime;
    }

    private void maxTime(long elapsedTime){
        if (elapsedTime >= maxTime)
            maxTime = elapsedTime;
    }
}
