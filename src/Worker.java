import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Queue;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Worker implements Runnable{

	
	private Queue<String> localQTasks;
	private Queue<String> localQResponse;
	private AmazonSQS sqs;
	private String sqsURL;
	private String sqsURLResponse;
	private AmazonDynamoDBClient dynamoDB;
	private String tableName;

	public Worker(Queue<String> localQTasks, AmazonSQS sqs, String sqsURL, Queue<String> localQResponse, String sqsURLResponse, String tableName, AmazonDynamoDBClient dynamoDB){
		this.localQTasks=localQTasks;
		this.localQResponse=localQResponse;
		this.sqs=sqs;
		this.sqsURL=sqsURL;
		this.sqsURLResponse=sqsURLResponse;
		this.dynamoDB=dynamoDB;
		this.tableName=tableName;
	}
	
	@Override
	public void run() {
		System.out.println("----Worker up----");
		if(localQTasks!=null){
			System.out.println("----Local worker----");
		
			localWorker();
		}else{
			remoteWorker();
		}
		
	}

	private void localWorker() {
		String task;
		while(true){
//			System.out.println("Queue size: "+localQTasks.size());
			task = this.localQTasks.poll();
//			System.out.println("Task: "+task);
			//If queue is empty wait
			if(task == null){
				try {
//					System.out.println("----Worker to sleep----");
					Thread.sleep(10);
//					System.out.println("----Worker got up----");
				} catch (InterruptedException e) {
				}
				
			}else{//Else, execute the task
				String id = task.split(":")[0];
//				System.out.println("Task ID: "+id);
				String[] args = task.split(":")[1].split(" ");
//				System.out.println("Task arguments: "+args[0]+"->"+args[1]);
				if(args[0].equals("sleep")){
					try {
						Thread.sleep(Integer.parseInt(args[1]));
						this.localQResponse.offer(id+":"+"OK");
//						System.out.println("----Task "+id+" done----");
					} catch (NumberFormatException | InterruptedException e) {
						e.printStackTrace();
						this.localQResponse.offer(id+":"+"ERROR");
					}
				}
				
			}
			
			
		}
		
	}
	
	private void remoteWorker() {

		String task;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsURL);
		//Set maximum of received messages to one
		receiveMessageRequest.setMaxNumberOfMessages(1);
		List<Message> messages;
		int count;
		while(true){
			task=null;
			// Receive messages
			count=0;//Set maximum of tries before waiting
			do{
				//Get message from queue
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				//If response non empty (one message) get this message
				if(!messages.isEmpty()){
					task=messages.get(0).getBody();
//					System.out.println("Message body:--"+task+"--");
				}
				count++;
			}while(count<5 && task == null);
			
			//If queue is empty wait
			if(task == null){
				try {
					System.out.println("--Worker to sleep--");
					Thread.sleep(300);
				} catch (InterruptedException e) {
				}
				
			}else{//Else, execute the task
				int id = Integer.parseInt(task.split("-->")[0]);
				
//				System.out.println("Message ID:--"+id+"--");

				
				//Check for duplicates
				DynamoDB dynamoDB = new DynamoDB(this.dynamoDB);

				Table table = dynamoDB.getTable(tableName);

				//Try to get it (shouldn't)
		        GetItemSpec spec = new GetItemSpec().withPrimaryKey("taskID", id);

		         
//				System.out.println("Attempting to read the item...");
				Item income = table.getItem(spec);
		        if(income==null){ 
				//If there is not previous attempt
		        	String args = task.split("-->")[1];
//					System.out.println("Message args:--"+args[0]+"--"+args[1]+"--");
					if(args.contains("sleep")){
						try {
							Thread.sleep(Integer.parseInt(args.split(" ")[1]));
							//Send to SQS Responses Queue
				            sqs.sendMessage(new SendMessageRequest(sqsURLResponse, id+":"+"OK"));
//				            System.out.println("Worker job done with ID "+id);
				            
//				            System.out.println("Deleting the task message.\n");
				            String messageReceiptHandle = messages.get(0).getReceiptHandle();
				            sqs.deleteMessage(new DeleteMessageRequest(sqsURL, messageReceiptHandle));
				            
				            table.putItem(new Item().withPrimaryKey("taskID", id).with("result", "OK"));
				            
						} catch (NumberFormatException | InterruptedException e ) {
							e.printStackTrace();
							sqs.sendMessage(new SendMessageRequest(sqsURLResponse,id+":"+"ERROR"));
						} catch (ProvisionedThroughputExceededException e){
							
						}
					}else if(args.contains("animoto")){//If animoto
						//Download the image files
						String[] imgURLs = args.split("\n");
						String nameBase="img";
						int numImg;
						System.out.println("Downloading images...");
						//Dowload the images
						for (numImg=1;numImg<imgURLs.length; numImg++){
							try {
								String cmd="wget -O "+nameBase+numImg+".jpg "+imgURLs[numImg];
								Process p = Runtime.getRuntime().exec(cmd);
								p.waitFor();
							} catch (IOException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						System.out.println("Making video...");
						//Make the video
						try {
							String cmd = "ffmpeg -framerate 1 -i img%d.jpg -c:v libx264 -r 30  animoto_video_"+id+".mp4";
							Process p =Runtime.getRuntime().exec(cmd);
							p.waitFor();
							
						} catch (IOException | InterruptedException e) {
							e.printStackTrace();
						}
						
						System.out.println("Video done! Uploading...");
						//Upload to s3
						AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
//							try {
//								String cmd = "aws s3 cp animoto_video_"+id+".mp4 s3://animotopa3/";
//								Process p =Runtime.getRuntime().exec(cmd);
//								p.waitFor();
//								
//							} catch (IOException | InterruptedException e) {
//								e.printStackTrace();
//							}
						
						File file = new File("animoto_video_"+id+".mp4");
			            s3client.putObject(new PutObjectRequest(
			            		                 "animotopa3", "animoto_video_"+id+".mp4", file));
			           
			            
						String s3URL="s3://animotopa3/animoto_video_"+id+".mp4";
						//Send task done
						sqs.sendMessage(new SendMessageRequest(sqsURLResponse, id+":"+"OK->"+s3URL));
						//Delete from queue
						String messageReceiptHandle = messages.get(0).getReceiptHandle();
			            sqs.deleteMessage(new DeleteMessageRequest(sqsURL, messageReceiptHandle));
			            
			            //Delete tmp files
			            new File("animoto_video_"+id+".mp4").delete();
			            for (numImg=1;numImg<imgURLs.length; numImg++)
			            	new File("img"+numImg+".jpg").delete();
			            
					}
		        }else{
		        	System.out.println("Task already done: "+income);
		        }
				
			}
			
		}
	}
}
