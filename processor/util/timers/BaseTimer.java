package processor.util.timers;

/**
 * BaseTimer starts to calculated elapsed time from specified point in code by start() method
 * up to the point when it is stopped
 * returns elapsed time in seconds between those points
 */
class BaseTimer{
    private final String name;
    protected long timeStamp;
    protected long elapsedTime;
    private int alreadyInvokedNumber = 1;

    BaseTimer(String name) {
        this.name = name;
        this.timeStamp = 0;
        this.elapsedTime = 0;
    }

    protected synchronized void start() {
        alreadyInvokedNumber = 0;
        timeStamp = System.nanoTime();
    }

    protected synchronized void stop(){
        alreadyInvokedNumber++;
        elapsedTime = getTime(timeStamp);
    }

//    protected synchronized double updateAndGetElapsedTime() {
//        updateElapsedTime();
//        return elapsedTime;
//    }

    public synchronized long getElapsedTime() {
        return alreadyInvokedNumber>1 ? -1 : elapsedTime;
    }

    public String getName(){
        return name;
    }

    synchronized long getTime(long timeStamp) {
        return (System.nanoTime() - timeStamp);
    }
}

