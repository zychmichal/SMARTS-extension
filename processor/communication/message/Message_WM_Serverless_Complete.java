package processor.communication.message;

import java.util.ArrayList;

public class Message_WM_Serverless_Complete {

    public String name;
    public ArrayList<Message_WS_Serverless_Complete> serverlessCompletes;

    public Message_WM_Serverless_Complete(){}

    public Message_WM_Serverless_Complete(String name, ArrayList<Message_WS_Serverless_Complete> serverlessCompletes){
        this.name = name;
        this.serverlessCompletes = serverlessCompletes;
    }
}
