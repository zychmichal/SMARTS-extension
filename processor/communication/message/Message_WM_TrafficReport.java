package processor.communication.message;

import java.util.ArrayList;

public class Message_WM_TrafficReport {

    public String name;
    public ArrayList<Message_WS_TrafficReport> trafficReports;

    public Message_WM_TrafficReport(){}

    public Message_WM_TrafficReport(String name, ArrayList<Message_WS_TrafficReport> trafficReports){
        this.name = name;
        this.trafficReports = trafficReports;
    }
}
