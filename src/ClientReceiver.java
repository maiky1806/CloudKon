
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class ClientReceiver implements Runnable{

	AmazonSQS sqs;
	String sqsURL;
	Queue<String> localQueue;
	private int numTasks;
	private long startTime;
	
	public ClientReceiver(int count, Queue<String> localQResponse, AmazonSQS sqs, String sqsURLResponse, long startTime) {
		this.localQueue=localQResponse;
		this.sqs=sqs;
		this.sqsURL=sqsURLResponse;
		this.numTasks=count;
		this.startTime=startTime;
	}

	@Override
	public void run() {
		//Check if it's local version
		if (localQueue != null) {
			localReceiver();
		} else {
			remoteReceiver();
		}
		System.out.println("All tasks received");
		float elapsedTime = (float) ((float)(System.currentTimeMillis() - this.startTime)/1000.0);
		System.out.println("Time elapsed "+elapsedTime+" seconds");

	}

	private void localReceiver() {
		String task;
		int count=0;
		while(count<numTasks){
			task = this.localQueue.poll();
			
			//If queue is empty wait
			if(task == null){
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
				
			}else{//Else, 				
				//count increase
				count++;
			
			}
			
			
		}
		
	}
	
	private void remoteReceiver() {

		String task = null;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsURL);
		List<Message> messages;
		int count=0, tries;
		int last=5;
		while(count<numTasks){
			
			// Receive messages
			tries=0;//Set maximum of tries before waiting
			do{
				//Get message from queue
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				//If response non empty (one message) get this message
				if(!messages.isEmpty()){
					Iterator<Message> it = messages.iterator();
					Message m=null;
					while(it.hasNext()){
						m=it.next();
						task=m.getBody();
						//Check animoto URL video to print it
						if(task.contains("s3")){
							System.out.println(task);
						}
						//Increase count
						count++;
						//Remove message
						String messageReceiptHandle = m.getReceiptHandle();
			            sqs.deleteMessage(new DeleteMessageRequest(sqsURL, messageReceiptHandle));
					}
				}
			
			}while(tries<5 && task == null);
			
			//If queue is empty wait
			if(task == null){
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
				
			}
			
		}
	}
	
}
