package processor.util;

import org.apache.logging.log4j.Logger;
import processor.util.timers.SimpleTimer;

import java.io.PrintStream;

public class ServerSendersTimings {
    private SimpleTimer sendSetupManagerTimer;
    private SimpleTimer sendSetupTimer;
    private SimpleTimer sendStartTimer;
    private SimpleTimer sendStopTimer;
    private SimpleTimer sendKillTimer;

    public ServerSendersTimings(int index) {
        this.sendSetupManagerTimer = new SimpleTimer("Sender [" + index + "] SWM_SETUP.");
        this.sendSetupTimer = new SimpleTimer("Sender [" + index + "] SW_SETUP.");
        this.sendStartTimer = new SimpleTimer("Sender [" + index + "] SW_START.");
        this.sendStopTimer = new SimpleTimer("Sender [" + index + "] SW_STOP.");
        this.sendKillTimer = new SimpleTimer("Sender [" + index + "] SW_KILL.");
    }

    public void printResults(PrintStream out) {
        printSingleTimerResult(out, sendSetupTimer);
    }

    public void logSendingSetupTimer(Logger logger) {
        logSingleTimerResult(logger, sendSetupTimer);
    }
    public void logSendingStartMsgTimer(Logger logger) {
        logSingleTimerResult(logger, sendStartTimer);
    }
    public void logSendingStopTimer(Logger logger) {
        logSingleTimerResult(logger, sendStopTimer);
    }
    public void logSendingKillTimer(Logger logger) {
        logSingleTimerResult(logger, sendKillTimer);
    }

    public void startSendingSetupConfirmation(){
        start(sendSetupTimer);
    }
    public void stopSendingSetupConfirmation(PrintStream out){
        stop(sendSetupTimer);
        printSingleTimerResult(out, sendSetupTimer);
    }

    public void startSendingSetupManagerConfirmation(){
        start(sendSetupManagerTimer);
    }

    public void stopSendingSetupManagerConfirmation(PrintStream out){
        stop(sendSetupManagerTimer);
        printSingleTimerResult(out, sendSetupManagerTimer);
    }


    public void startSendingStart(){
        start(sendStartTimer);
    }
    public void stopSendingStart(PrintStream out){
        stop(sendStartTimer);
        printSingleTimerResult(out, sendStartTimer);
    }

    public void startSendingStop(){
        start(sendStopTimer);
    }
    public void stopSendingStop(PrintStream out){
        stop(sendStopTimer);
        printSingleTimerResult(out, sendStopTimer);
    }

    public void startSendingKill(){
        start(sendKillTimer);
    }
    public void stopSendingKill(PrintStream out){
        stop(sendKillTimer);
        printSingleTimerResult(out, sendKillTimer);
    }

    private void start(SimpleTimer st){
        st.start();
    }
    private void stop(SimpleTimer st){
        st.stop();
    }

    private void printSingleTimerResult(PrintStream out, SimpleTimer bt){
        out.println("Server summary: " + bt.getName() + " time: " + bt.getElapsedTime()/1.0E9);
    }

    private void logSingleTimerResult(Logger logger, SimpleTimer bt) {
        logger.info("Server summary: " + bt.getName() + " time: " + bt.getElapsedTime()/1.0E9);
    }
}