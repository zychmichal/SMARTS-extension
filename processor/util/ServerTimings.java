package processor.util;

import org.apache.logging.log4j.Logger;
import processor.util.timers.SimpleTimer;
import processor.util.timers.UpdateTimer;

import java.io.PrintStream;


public class ServerTimings {
    private SimpleTimer initializationTimer;
    private SimpleTimer setupTimer;
    private SimpleTimer fileDumpingTimer;
    private SimpleTimer wholeProcessTimer;
    private SimpleTimer sendingSetupsTimer;
    private SimpleTimer sendingSetupsToAllTimer; // not used
    private SimpleTimer sendStopToAllWorkersTimer;
    private SimpleTimer sendStartToAllWorkersTimer;
    private SimpleTimer partitionGridCells;

    private UpdateTimer simulationStageTimer;

    public ServerTimings() {
        this.initializationTimer = new SimpleTimer("Initialization");
        this.setupTimer = new SimpleTimer("Setup");
        this.fileDumpingTimer = new SimpleTimer("File Dumping");
        this.wholeProcessTimer = new SimpleTimer("Whole Process");
        this.partitionGridCells = new SimpleTimer("Partition Grid Cells");
        this.simulationStageTimer = new UpdateTimer("Simulation");
        this.sendingSetupsTimer = new SimpleTimer("Sending Setups");
        this.sendStartToAllWorkersTimer = new SimpleTimer("Send start to workers");
        this.sendStopToAllWorkersTimer = new SimpleTimer("Send stop to workers");
        this.sendingSetupsToAllTimer = new SimpleTimer("Sending Confirmations for setups");
    }

    public void printResults(PrintStream out) {
        printSingleTimerResult(out, initializationTimer);
        printSingleTimerResult(out, setupTimer);
        printSingleTimerResult(out, simulationStageTimer);
        printSingleTimerResult(out, fileDumpingTimer);
        printSingleTimerResult(out, wholeProcessTimer);

        printSingleTimerResult(out, sendingSetupsTimer); // part of setup
        printSingleTimerResult(out, sendStartToAllWorkersTimer);
        printSingleTimerResult(out, sendStopToAllWorkersTimer);
        printSingleTimerResult(out, partitionGridCells); // part of initialization
//        printSingleTimerResult(out, sendingSetupsSentToAllTimer);
    }

    public void logResults(Logger logger) {
        logSingleTimerResult(logger, initializationTimer);
        logSingleTimerResult(logger, setupTimer);
        logSingleTimerResult(logger, simulationStageTimer);
        logSingleTimerResult(logger, fileDumpingTimer);
        logSingleTimerResult(logger, wholeProcessTimer);
        logSingleTimerResult(logger, sendingSetupsTimer);
        logSingleTimerResult(logger, sendStartToAllWorkersTimer);
        logSingleTimerResult(logger, sendStopToAllWorkersTimer);
        logSingleTimerResult(logger, partitionGridCells);
//        printSingleTimerResult(out, sendingSetupsSentToAllTimer);
    }


    public void startFirstStage(){          // 1
        start(initializationTimer);
        start(wholeProcessTimer);
    }
    public void stopFirstAndStartSecStage(){        // 2
        stop(initializationTimer);
        initSimulationElapsedTime();
        start(setupTimer);
    }
    public void stopSecondStage(){              // 3
        stop(setupTimer);
    }

    private void initSimulationElapsedTime(){
        simulationStageTimer.resetElapsedTime();
    }
    public void resetSimulationTimer(){                     // 4.1
        simulationStageTimer.resetCurrentTimeStamp();
    }
    public void updateSimulationTimer(){                    // 4.2
        simulationStageTimer.addTimeSinceLastReset();
    }

    public void startFileDumping(){             // 5
        start(fileDumpingTimer);
    }
    public void stopFileDumping(){             // 6
        stop(fileDumpingTimer);
    }
    public void stopWholeProcess(){             // 7,      goto ==> 1
        stop(wholeProcessTimer);
    }

    public void startSendingSetup(){
        start(sendingSetupsTimer);
    }
    public void stopSendingSetup(PrintStream out){
        stop(sendingSetupsTimer);
        printSingleTimerResult(out, sendingSetupsTimer);
    }

    public void startSendingSetupConfirmation(){
        start(sendingSetupsToAllTimer);
    }
    public void stopSendingSetupConfirmation(PrintStream out){
        stop(sendingSetupsToAllTimer);
        printSingleTimerResult(out, sendingSetupsToAllTimer);
    }

    public void startPartitionGridCells(){
        start(partitionGridCells);
    }
    public void stopPartitionGridCells(PrintStream out){
        stop(partitionGridCells);
        printSingleTimerResult(out, partitionGridCells);
    }

    public void startSendingStop(){
        start(sendStopToAllWorkersTimer);
    }
    public void stopSendingStop(PrintStream out){
        stop(sendStopToAllWorkersTimer);
        printSingleTimerResult(out, sendStopToAllWorkersTimer);
    }

    public void startSendingStart(){
        start(sendStartToAllWorkersTimer);
    }
    public void stopSendingStart(PrintStream out){
        stop(sendStartToAllWorkersTimer);
        printSingleTimerResult(out, sendStartToAllWorkersTimer);
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

    private void printSingleTimerResult(PrintStream out, UpdateTimer bt){
        out.println("Server summary: " + bt.getName() + " time: " + bt.getElapsedTime()/1.0E9);
    }

    private void logSingleTimerResult(Logger logger, SimpleTimer bt) {
        logger.info("Server summary: " + bt.getName() + " time: " + bt.getElapsedTime()/1.0E9);
    }

    private void logSingleTimerResult(Logger logger, UpdateTimer bt) {
        logger.info("Server summary: " + bt.getName() + " time: " + bt.getElapsedTime()/1.0E9);
    }

    public double getSimulationTime(){
        return simulationStageTimer.getElapsedTime()/1.0E9;
    }

}

