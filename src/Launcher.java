import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
/**
 * Main class containing the launch process for the different parts
 * @author Miguel Menendez
 *
 */
public class Launcher {

	private static final String RESPONSE_QUEUE = "taskExecuted";
	private static final String DYNAMO_TABLE = "taskTracker";
	
	public static void main(String[] args) {
		//Read arguments
		Options options = new Options();

		options.addOption("client", false, "Client or local version");
		options.addOption("worker", false, "Remote worker version");
		options.addOption("s", true, "Queue name");
		options.addOption("t", true, "number of threads");
		options.addOption("w", true, "Workload file");
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		
		//Params
		String filename=null;
		String queuename=null;
		int numThreads=0;
		boolean client=false;
		
		//Objects for either local or remote
		Queue<String> localQTask=null;
		Queue<String> localQResponse=null;
		AWSCredentials credentials = null;
		AmazonSQS sqs=null;
		AmazonDynamoDBClient clientDB = null;
		String sqsURLTask=null;
		String sqsURLResponse=null;
		
		
		
		//Get queue name/local
		if(cmd.hasOption("s")) {
			queuename = cmd.getOptionValue("s");
		}
		else {
			System.out.println("Error: Queue name not provided");
		    System.exit(0);
		}
		//Get queue name/local
		if (cmd.hasOption("t")) {
			numThreads = Integer.parseInt(cmd.getOptionValue("t"));
		}
		
		
		if (args[0].equals("client")) {
			System.out.println("Client:");
			client=true;
			//Get task's file name
			if(cmd.hasOption("w")) {
				filename = cmd.getOptionValue("w");
			}
			else {
				System.out.println("Error: Workload file not provided");
			    System.exit(0);
			}
		}else if (args[0].equals("worker")) {
			System.out.println("Worker:");
		}else{
			System.out.println("Error: Type of instance not provided");
		    System.exit(0);
		}
		
		//If local
		if(queuename.equals("LOCAL")){
			//Initiate local queue
			localQTask = new ConcurrentLinkedQueue<String>();
			localQResponse = new ConcurrentLinkedQueue<String>();
			
		}else{
			//If remote, initiate SQS and DynamoDB
	        try {
	            credentials = new ProfileCredentialsProvider().getCredentials();
	        } catch (Exception e) {
	            throw new AmazonClientException(
	                    "Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your credentials file is at the correct " +
	                    "location (~/.aws/credentials), and is in valid format.",
	                    e);
	        }

	        sqs = new AmazonSQSClient(credentials);
	        Region eucentral = Region.getRegion(Regions.EU_CENTRAL_1);
	        sqs.setRegion(eucentral);
			
	        
			// Get queue URL
	        System.out.println("Queue name: "+queuename);
			sqsURLTask = sqs.getQueueUrl(queuename).getQueueUrl();
			sqsURLResponse = sqs.getQueueUrl(RESPONSE_QUEUE).getQueueUrl();
			
			//Connection with DynamoDB
			clientDB = new AmazonDynamoDBClient(credentials);
	        clientDB.setRegion(eucentral);
			
		}
		
		if(client){
			if (!queuename.equals("LOCAL")){
				//Purgue queues
				sqs.purgeQueue(new PurgeQueueRequest(sqsURLTask));
				sqs.purgeQueue(new PurgeQueueRequest(sqsURLResponse));
				System.out.println("Purging queues...");
				
				
				//Delete/create table 
				DynamoDB dynamoDB = new DynamoDB(clientDB);
				//Delete
		        try {
		        	Table table = dynamoDB.getTable(DYNAMO_TABLE);
		            System.out.print("Attempting to delete table; please wait...\t");
		            table.delete();
		            table.waitForDelete();
		            System.out.println("Success.");
	
		        } catch (Exception e) {
		            System.err.println("Unable to delete table: ");
		            System.err.println(e.getMessage());
		        }
		        //Create
		        try {
		            System.out.print("Attempting to create table; please wait...\t");
		            Table table = dynamoDB.createTable(DYNAMO_TABLE,
		                Arrays.asList(
		                    new KeySchemaElement("taskID", KeyType.HASH)), //Partition key
		                    Arrays.asList(
		                        new AttributeDefinition("taskID", ScalarAttributeType.N)), 
		                    new ProvisionedThroughput(50L, 50L));
		            table.waitForActive();
		            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
	
		        } catch (Exception e) {
		            System.err.println("Unable to create table: ");
		            System.err.println(e.getMessage());
		        }
		        
				
		        
			}
			int count=0;
			try {
				LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(filename)));
				
				lnr.skip(Long.MAX_VALUE);
				count= lnr.getLineNumber();
				
				lnr.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			System.out.println("----Task count: "+count+"----");
			//Open file with tasks
			BufferedReader taskfile=null;
			try {
				taskfile = new BufferedReader(new FileReader(new File(filename)));
			} catch (FileNotFoundException e) {
				System.out.println("Error: Not able to open the workload file");
				System.exit(0);
			}
		
		
			//Launch client sender (either local or remote)
			Thread cSender = new Thread(new ClientSender(taskfile, localQTask, sqs, sqsURLTask));
			long startTime = System.currentTimeMillis();
			cSender.start();
			
	
			//Launch client receiver
			Thread cRec = new Thread(new ClientReceiver(count, localQResponse, sqs, sqsURLResponse, startTime));
			cRec.start();
		
		}
					
		if((queuename.equals("LOCAL")&& client) || !client){	
			//Launch workers
			ExecutorService service = Executors.newFixedThreadPool(numThreads);
			for (int i = 0; i < numThreads; i++) {
				service.execute(new Worker(localQTask, sqs, sqsURLTask, localQResponse, sqsURLResponse, DYNAMO_TABLE, clientDB));
			}
			service.shutdown();
			try {
				service.awaitTermination(2, TimeUnit.HOURS);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	

	}

}
