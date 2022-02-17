package processor;

import java.io.IOException;
import java.net.Socket;

import common.Settings;
import processor.server.Server;
import processor.worker.Worker;

/**
 * Class for running a simulator with a single JVM.
 *
 */
public class Simulator {

    static boolean defaultSimulator = true;
    static int nrWorkers = 0;
    static String scriptPath = "";

    public static void createWorkers() {
        for (int i = 0; i < Settings.numWorkers; i++) {
            final Worker worker = new Worker();
            worker.run();
        }
    }

    static boolean isPortFree(final int port) {
        try (Socket ignored = new Socket("127.0.0.1", port)) {
            return false;
        } catch (final IOException ignored) {
            return true;
        }
    }

    static boolean proccessComandLineArgs(String[] args){
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-n":
                        nrWorkers = Integer.parseInt(args[i + 1]);
                        defaultSimulator = false;
                        break;
                    case "-script":
                        scriptPath = args[i+1];
                        break;
                }
            }
        }catch (final Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Starts the simulator.
     *
     * @param args
     */
    public static void main(final String[] args) {
        proccessComandLineArgs(args);
        Settings.isSharedJVM = true;
        Settings.isVisualize = true;
        if (isPortFree(Settings.serverListeningPortForWorkers)) {
            startSystem();
        } else {
            System.exit(0);
        }
    }

    static void startSystem() {
        Server server = new Server();
        if (!defaultSimulator){
            Settings.isVisualize = false;
            server.nrWorkers = nrWorkers;
            server.defaultSimulation = false;
            server.scriptPath = scriptPath;
        }
        server.run();
        createWorkers();

    }
}