package processor.communication.message;

import java.util.ArrayList;

public class Message_WM_FinishSetupWorkers {

    public String name;
    public ArrayList<Message_WS_SetupDone> setupDones;

    public Message_WM_FinishSetupWorkers(String name, ArrayList<Message_WS_SetupDone> setupDones){
        this.name = name;
        this.setupDones = setupDones;
    }

    public Message_WM_FinishSetupWorkers(){}
}
