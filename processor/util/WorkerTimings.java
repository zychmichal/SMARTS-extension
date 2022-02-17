package processor.util;

import org.apache.logging.log4j.Logger;
import processor.util.timers.SimpleTimer;
import processor.util.timers.UpdateTimer;

import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;

public class WorkerTimings {
    public static final int NUMBER_OF_SAMPLES = 50;
    private SimpleTimer procesingWholeSetupTimer;
    private SimpleTimer sendingSetupDoneTimer;
    private SimpleTimer procesReceivedSimConfTimer;
    private SimpleTimer buildEnvTimer;
    private SimpleTimer resetTrafficTimer;
    private SimpleTimer internalVehicleTimer;
    private SimpleTimer externalVehicleTimer;
    private SimpleTimer dumpingTrajectoriesInWorkerTimer;

    private SimpleTimer simulationOfOneStepTimer;
    private SimpleTimer sendingAll_WW_msg_Timer;
    private UpdateTimer receive_WW_msgs_Timer;
    private SimpleTimer sendingAll_WS_report_Timer;
    private SimpleTimer wholeStepTimer;
    private UpdateTimer idleTimer;

    private TimeCollector simulationSampleCollector;
    private TimeCollector sendWWSampleCollector;      //part of simulation
    private TimeCollector receiveWWSampleCollector;
    private TimeCollector sendWSReportsSampleCollector;
    private TimeCollector wholeStepSampleCollector;
    private TimeCollector idleSampleCollector;

    Runtime runtime = Runtime.getRuntime();
    MBeanServer mBeanServer;
    java.lang.management.ThreadMXBean threadMXBean;
    MemoryUsage MU;

    public WorkerTimings() {

        MU = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        try {
            mBeanServer = ManagementFactory.getPlatformMBeanServer();
            threadMXBean = ManagementFactory.getThreadMXBean();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("WorkerTimings.WorkerTimings() - constructor error");
        }

        this.procesingWholeSetupTimer = new SimpleTimer("[Timer][Setup] Processing Whole setup");
        this.sendingSetupDoneTimer = new SimpleTimer("[Timer][Setup] Sending setup done");
        this.procesReceivedSimConfTimer = new SimpleTimer("[Timer][Setup] Process Received Conf");
        this.buildEnvTimer = new SimpleTimer("[Timer][Setup] Build Env");
        this.resetTrafficTimer = new SimpleTimer("[Timer][Setup] Reset Traffic");
        this.internalVehicleTimer = new SimpleTimer("[Timer][Setup] Create internal vehicles");
        this.externalVehicleTimer = new SimpleTimer("[Timer][Setup] Create external vehicles");
        this.dumpingTrajectoriesInWorkerTimer = new SimpleTimer("[Timer][Trajectoreis] Dumping trajectories into file.");

        this.simulationOfOneStepTimer = new SimpleTimer("simulation");       //  1 (synchronized)
        this.sendingAll_WW_msg_Timer = new SimpleTimer("sendWW");        //  1.1 part of 1
        this.receive_WW_msgs_Timer = new UpdateTimer("processRecWW");          //  2 (many parts)
        this.sendingAll_WS_report_Timer = new SimpleTimer("sendWSReport");  //  3
        this.wholeStepTimer = new SimpleTimer("wholeStep");                 //  5 contains: 1,2,3 and 1.1

        this.idleTimer = new UpdateTimer("idle");                           //additional
        this.idleSampleCollector = new TimeCollector(idleTimer.getName());          //additional

        this.simulationSampleCollector = new TimeCollector(simulationOfOneStepTimer.getName());
        this.sendWWSampleCollector = new TimeCollector(sendingAll_WW_msg_Timer.getName());      //part of simulation
        this.receiveWWSampleCollector = new TimeCollector(receive_WW_msgs_Timer.getName());
        this.sendWSReportsSampleCollector = new TimeCollector(sendingAll_WS_report_Timer.getName());
        this.wholeStepSampleCollector = new TimeCollector(wholeStepTimer.getName());
    }

    public void printResults(PrintStream out, String name, int step, int cars){

        Object attribute2 = null;
        Object attribute = null;
        try {
            attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
            attribute2 = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
        } catch (Exception e) {
            e.printStackTrace();
        }

        String strNonHeapMU = "{init=" + (MU.getInit() >> 10) + "K| "+
                "used=" + (MU.getUsed() >> 10) + "K| " +
                "committed=" + (MU.getCommitted() >> 10) + "K| " +
                "max=" + (MU.getMax() >> 10) + "K}";

        out.println("Worker=" + name + "; step=" + step +
                "; cpus=" + runtime.availableProcessors() +
                "; activeT: " + threadMXBean.getThreadCount() +
                "; peakT: " + threadMXBean.getPeakThreadCount() +
                "; totalStartedT: " + threadMXBean.getTotalStartedThreadCount() +
                "; Total_OS: "+ Long.parseLong(attribute.toString()) / 1024 /1024 +"MB" +
                "; Free_OS: "+ Long.parseLong(attribute2.toString()) / 1024 /1024 +"MB"+
                "; loadAvg=" + getOperatingSystemMXBean().getSystemLoadAverage() +
                "; P_CpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getProcessCpuLoad() +
                "; cpuLoad=" + ((com.sun.management.OperatingSystemMXBean)getOperatingSystemMXBean()).getCpuLoad() +
//                "; HeapBean=" + strHeapMU +
                "; NonHeapBean=" + strNonHeapMU +
                "; cpus2=" + getOperatingSystemMXBean().getAvailableProcessors() +
                "; MaxMem=" + ((double)runtime.maxMemory()/(1024*1024)) +
                "; TotalMem=" + ((double)runtime.totalMemory()/(1024*1024)) +
                "; FreeMem=" + ((double)runtime.freeMemory()/(1024*1024))+
                "; cars=" + cars +
                simulationSampleCollector.toString() +
                sendWWSampleCollector.toString() +
                receiveWWSampleCollector.toString() +
                sendWSReportsSampleCollector.toString() +
                wholeStepSampleCollector.toString() +
                idleSampleCollector.toString());
    }

    public void resetAll(){
        // reset every print (assumes NUMBER_OF_STEPS to collect)
        simulationSampleCollector.reset();
        sendWWSampleCollector.reset();
        receiveWWSampleCollector.reset();
        sendWSReportsSampleCollector.reset();
        wholeStepSampleCollector.reset();
        idleSampleCollector.reset();
    }

    public void startProcesReceivedSimConfTimer(){
        start(procesReceivedSimConfTimer);
    }
    public void stopProcesReceivedSimConfTimer(){
        stop(procesReceivedSimConfTimer);
    }

    public void startBuildEnvTimer(){
        start(buildEnvTimer);
    }
    public void stopBuildEnvTimer(){
        stop(buildEnvTimer);
    }

    public void startResetTrafficTimer(){
        start(resetTrafficTimer);
    }
    public void stopResetTrafficTimer(){
        stop(resetTrafficTimer);
    }

    public void startExternalVehicleTimer(){
        start(externalVehicleTimer);
    }
    public void stopExternalVehicleTimer(){
        stop(externalVehicleTimer);
    }

    public void startInternalVehicleTimer(){
        start(internalVehicleTimer);
    }
    public void stopInternalVehicleTimer(){
        stop(internalVehicleTimer);
    }

    public void startProcesingWholeSetupTimer(){
        start(procesingWholeSetupTimer);
    }
    public void stopProcesingWholeSetupTimer(){
        stop(procesingWholeSetupTimer);
    }

    public void startSendingSetupDoneTimer(){
        start(sendingSetupDoneTimer);
    }
    public void stopSendingSetupDoneTimer(){
        stop(sendingSetupDoneTimer);
    }

    public void logAllSetupTimers(Logger logger) {
        logSingleTimerResult(logger, procesReceivedSimConfTimer);
        logSingleTimerResult(logger, buildEnvTimer);
        logSingleTimerResult(logger, resetTrafficTimer);
        logSingleTimerResult(logger, externalVehicleTimer);
        logSingleTimerResult(logger, internalVehicleTimer);

        logSingleTimerResult(logger, procesingWholeSetupTimer);
        logSingleTimerResult(logger, sendingSetupDoneTimer);
    }

    public void printProcessingSetupElapsedTime(String workerName, PrintStream out){
        out.println("[WorkerTimer][Setup];"+workerName+";"+(procesingWholeSetupTimer.getElapsedTime()/1e9));
//        Worker summary: [Timer][Setup] Processing Whole setup time: 127.282345266
    }

    public void printDumpTrajFromWorkerTime(String workerName, PrintStream out){
        out.println("[WorkerTimer][Trajectories];"+workerName+";"+(dumpingTrajectoriesInWorkerTimer.getElapsedTime()/1e9));
//        Worker summary: [Timer][Setup] Processing Whole setup time: 127.282345266
    }

    public void startDumpingTrajecTimer(){
        start(dumpingTrajectoriesInWorkerTimer);
    }
    public void stopDumpingTrajecTimer(){
        stop(dumpingTrajectoriesInWorkerTimer);
    }

    public void logDumpingTrajectories(Logger logger){
        logSingleTimerResult(logger, dumpingTrajectoriesInWorkerTimer);
    }

//    public void printStdTrajectories(Logger logger){
//        printSingleTimerResult(System.out, dumpingTrajectoriesInWorkerTimer);
//    }

//    procesingWholeSetupTimer

    /*
     * Measures time for simulating one step in current worker
     * IT INCLUDES sending time of WW msgs with traffic
     */
    public void startSimulationOfOneStep(){
        start(simulationOfOneStepTimer);
    }
    public void stopSimulationOfOneStep(){
        stop(simulationOfOneStepTimer);
    }

    /*
     * Measures time for sending all Traffic msgs to other Workers in one step
     * IT IS A PART of simulation time of One step stage
     */
    public void startSendingWWmsgs(){
        start(sendingAll_WW_msg_Timer);
    }
    public void stopSendingWWmsgs(){
        stop(sendingAll_WW_msg_Timer);
    }

    /*
     * Measures time for sending single Traffic Report msg to Server in one step
     */
    public void startSendingWSreport(){
        start(sendingAll_WS_report_Timer);
    }
    public void stopSendingWSreport(){
        stop(sendingAll_WS_report_Timer);
    }

    /*
     * Measures time for whole one step in current worker
     */
    public void startWholeStep(){
        start(wholeStepTimer);
    }
    private void stopWholeStep(){
        stop(wholeStepTimer);
    }

    /*
     * Measures time for processing all received Traffic msgs from other Workers in one step
     * It is calculated as the sum of processing time for each msg - result for one step is stored in elapsedTime
     */
    private void initProcessReceivedWW(){
        receive_WW_msgs_Timer.resetElapsedTime();
    }
    public void resetProcessReceivedWW(){
        receive_WW_msgs_Timer.resetCurrentTimeStamp();
    }
    public void updateProcessReceivedWW(){
        receive_WW_msgs_Timer.addTimeSinceLastReset();
    }


    private void initIdle(){
        idleTimer.resetElapsedTime();
    }
    public void resetIdle(){
        idleTimer.resetCurrentTimeStamp();
    }
    public void updateIdle(){
        idleTimer.addTimeSinceLastReset();
    }

    public void processOneStepTimers(){
        stopWholeStep();
        // process stopWholeStep()
        storeTimeDataForStep(wholeStepSampleCollector, wholeStepTimer);
        storeTimeDataForStep(simulationSampleCollector, simulationOfOneStepTimer);
        storeTimeDataForStep(sendWWSampleCollector, sendingAll_WW_msg_Timer);
        storeTimeDataForStep(sendWSReportsSampleCollector, sendingAll_WS_report_Timer);
        storeTimeDataForStep(receiveWWSampleCollector, receive_WW_msgs_Timer);
        storeTimeDataForStep(idleSampleCollector, idleTimer);
        initProcessReceivedWW();
        initIdle();

        startWholeStep();
    }


    private void start(SimpleTimer st){
        st.start();
    }
    private void stop(SimpleTimer st){
        st.stop();
    }

    private void storeTimeDataForStep(TimeCollector c, SimpleTimer st){
        final long elapsedTime = st.getElapsedTime();
        c.collectElapsedTime(elapsedTime);
    }
    private void storeTimeDataForStep(TimeCollector c, UpdateTimer st){
        final long elapsedTime = st.getElapsedTime();
        c.collectElapsedTime(elapsedTime);
    }

    private void logSingleTimerResult(Logger logger, SimpleTimer bt) {
        logger.info("Worker summary: " + bt.getName() + " time: " + bt.getElapsedTime()/1.0E9);
    }

    private void printSingleTimerResult(PrintStream out, SimpleTimer bt){
        out.println("Worker summary: " + bt.getName() + " time: " + bt.getElapsedTime()/1.0E9);
    }
}

