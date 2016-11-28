import java.io.BufferedReader;
import java.io.IOException;
import java.util.Queue;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class ClientSender implements Runnable{

	private BufferedReader taskFile;
	AmazonSQS sqs;
	String sqsURL;
	private Queue<String> localQueue;
	
	public ClientSender(BufferedReader file, Queue<String> localQueue, AmazonSQS sqs, String sqsURL){
		this.taskFile=file;
		this.localQueue=localQueue;
		this.sqs=sqs;
		this.sqsURL=sqsURL;
	}
	
	@Override
	public void run() {
		String task=null;
		int id=0;
		//Check if it's local version
		boolean local=false;
		if(localQueue != null)
			local=true;
		
		System.out.println("Sending tasks...");
		
		do{
			try {
				//Read next task
				task=taskFile.readLine();
				if (task==null) break;
				
				if(local){
					//Add to queue
//					System.out.println("Local task sent");
					localQueue.offer(id+":"+task);
				}else{
					//Send to SQS Queue
//					System.out.println("Remote task sent");
					//Check if animoto or sleep task
					if (task.split(" ")[0].equals("sleep")){
						sqs.sendMessage(new SendMessageRequest(sqsURL, id+"-->"+task));
					}else if(task.split(" ")[0].equals("animoto")){
						//If animoto, continue until end of animoto task
						String animotoRequest="animoto\n";
						while(!(task=taskFile.readLine()).equals("end")){
							animotoRequest+=task+"\n";
						}
						sqs.sendMessage(new SendMessageRequest(sqsURL, id+"-->"+animotoRequest));
					}
				}
				id++;
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		
		}while(task != null);
		System.out.println("All tasks sent...");
	}

	
}
