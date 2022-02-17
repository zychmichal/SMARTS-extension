package processor.util.timers;

public class UpdateTimer extends BaseTimer{

    public void resetCurrentTimeStamp() {
        super.start();
    }

    public synchronized void addTimeSinceLastReset() {
        elapsedTime += getTime(timeStamp);
    }

    public synchronized void resetElapsedTime(){
        elapsedTime = 0;
    }

    public UpdateTimer(String name) {
        super(name);
    }
}
