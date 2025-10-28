/*
 * Replace the following string of 0s with your student number
 * c403141619
 */
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.File;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.SocketException;



public class Protocol {

	static final String  NORMAL_MODE="nm"   ;         // normal transfer mode: (for Part 1 and 2)
	static final String	 TIMEOUT_MODE ="wt"  ;        // timeout transfer mode: (for Part 3)
	static final String	 LOST_MODE ="wl"  ;           // lost Ack transfer mode: (for Part 4)
	static final int DEFAULT_TIMEOUT =1000  ;         // default timeout in milliseconds (for Part 3)
	static final int DEFAULT_RETRIES =4  ;            // default number of consecutive retries (for Part 3)
	public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

	/*
	 * The following attributes control the execution of the transfer protocol and provide access to the 
	 * resources needed for the transfer 
	 * 
	 */ 

	private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
	private int portNumber; 		    // the  port the server is listening on
	private DatagramSocket socket;      // the socket that the client binds to

	private File inputFile;            // the client-side CSV file that has the readings to transfer  
	private String outputFileName ;    // the name of the output file to create on the server to store the readings
	private int maxPatchSize;		   // the patch size - no of readings to be sent in the payload of a single Data segment

	private Segment dataSeg   ;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server 
	private Segment ackSeg  ;          // the protocol Ack segment for receiving ACK segments from the server

	private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
	private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
	private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

	private int fileTotalReadings;    // number of all readings in the csv file
	private int sentReadings;         // number of readings successfully sent and acknowledged
	private int totalSegments;        // total segments that the client sent to the server

	// Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
	public static Protocol instance = new Protocol();

	/**************************************************************************************************************************************
	 **************************************************************************************************************************************
	 * For this assignment, you have to implement the following methods:
	 *		sendMetadata()
	 *      readandSend()
	 *      receiveAck()
	 *      startTimeoutWithRetransmission()
	 *		receiveWithAckLoss()
	 * Do not change any method signatures, and do not change any other methods or code provided.
	 ***************************************************************************************************************************************
	 **************************************************************************************************************************************/
	/*
	 * This method sends protocol metadata to the server.
	 * Metadata includes total readings, output file name, and patch size.
	 * It's always sent first before any data segments.
	 */
	public void sendMetadata() {
		String payload = Protocol.instance.getFileTotalReadings() + "," +
				Protocol.instance.getOutputFileName() + "," +
				Protocol.instance.getMaxPatchSize();

		Segment metaSeg = new Segment();
		metaSeg.setType(SegmentType.Meta);   // Metadata segment
		metaSeg.setSeqNum(0);                // Sequence number 0 for metadata
		metaSeg.setPayLoad(payload);         // Add payload string

		try {
			// Convert segment to bytes to send via UDP
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
			objectStream.writeObject(metaSeg);
			objectStream.flush();
			byte[] payloadByte = byteStream.toByteArray();

			// Create UDP packet and send to server
			DatagramPacket packet = new DatagramPacket(payloadByte, payloadByte.length, ipAddress, portNumber);
			socket.send(packet);

			System.out.println("CLIENT: META [SEQ#0] sent (TotalReadings: "
					+ Protocol.instance.getFileTotalReadings()
					+ ", Output: " + Protocol.instance.getOutputFileName()
					+ ", PatchSize: " + Protocol.instance.getMaxPatchSize() + ")");

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	/* 
	 * This method read and send the next data segment (dataSeg) to the server. 
	 * See coursework specification for full details.
	 */
	// Read a patch of readings from CSV and send as a Data segment
	public void readAndSend() {
		try {
			// Open the CSV file
			Scanner csvScanner = new Scanner(instance.inputFile);

			// Skip readings that were already sent and acknowledged
			int skipped = 0;
			while (skipped < instance.sentReadings && csvScanner.hasNextLine()) {
				csvScanner.nextLine();
				skipped++;
			}

			// Read next batch of readings (up to maxPatchSize)
			StringBuilder payloadBuilder = new StringBuilder();
			int readingsInSegment = 0;
			while (csvScanner.hasNextLine() && readingsInSegment < instance.maxPatchSize) {
				String line = csvScanner.nextLine().trim();
				if (!line.isEmpty()) {
					if (payloadBuilder.length() > 0) {
						payloadBuilder.append(";"); // separate readings with semicolon
					}
					payloadBuilder.append(line);
					readingsInSegment++;
				}
			}

			// If there are no readings left, exit the program
			if (readingsInSegment == 0) {
				csvScanner.close();
				System.exit(0);
			}

			// Create a Data segment to send
			// seqNum alternates between 1 and 0 for Stop-and-Wait
			instance.dataSeg = new Segment(
					(instance.totalSegments % 2) + 1,
					SegmentType.Data,
					payloadBuilder.toString(),
					payloadBuilder.toString().length()
			);

			// Send the Data segment over UDP
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
			objStream.writeObject(instance.dataSeg);
			objStream.flush();
			byte[] packetData = byteStream.toByteArray();
			DatagramPacket packet = new DatagramPacket(packetData, packetData.length, instance.ipAddress, instance.portNumber);
			instance.socket.send(packet);

			// Update counters to track progress
			instance.totalSegments++;  // total segments sent
			instance.sentReadings += readingsInSegment;  // total readings sent

			// Print a simple message for user to follow progress
			System.out.println("CLIENT: Sent DATA [SEQ#" + instance.dataSeg.getSeqNum() + "] with "
					+ readingsInSegment + " readings, total segments sent: " + instance.totalSegments);

			csvScanner.close();

		} catch (Exception e) {
			System.out.println("CLIENT: Error sending data segment");
			e.printStackTrace();
			System.exit(0);
		}
	}

	// Receive an ACK from the server
	public boolean receiveAck() {
		try {
			// Prepare buffer to receive ACK
			byte[] buf = new byte[MAX_Segment_SIZE];
			DatagramPacket incomingPacket = new DatagramPacket(buf, buf.length);
			instance.socket.receive(incomingPacket);

			// Deserialize received object into Segment
			ByteArrayInputStream byteIn = new ByteArrayInputStream(incomingPacket.getData());
			ObjectInputStream objIn = new ObjectInputStream(byteIn);
			Segment ackSeg = (Segment) objIn.readObject();

			// Check if received segment is an ACK and matches our last sent seqNum
			if (ackSeg.getType() == SegmentType.Ack && ackSeg.getSeqNum() == instance.dataSeg.getSeqNum()) {
				System.out.println("CLIENT: Received ACK [SEQ#" + ackSeg.getSeqNum() + "]");
				return true;  // ACK is correct
			} else {
				System.out.println("CLIENT: Received wrong ACK or duplicate, expected SEQ#" + instance.dataSeg.getSeqNum());
				return false; // ACK does not match, ignore
			}

		} catch (Exception e) {
			System.out.println("CLIENT: Error receiving ACK");
			e.printStackTrace();
			System.exit(0);
			return false;
		}
	}




	/* 
	 * This method starts a timer and does re-transmission of the Data segment 
	 * See coursework specification for full details.
	 */
	public void startTimeoutWithRetransmission()   {  
		System.exit(0);
	}


	/* 
	 * This method is used by the server to receive the Data segment in Lost Ack mode
	 * See coursework specification for full details.
	 */
	public void receiveWithAckLoss(DatagramSocket serverSocket, float loss)  {
		System.exit(0);
	}


	/*************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	These methods are implemented for you .. Do NOT Change them 
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************/	 
	/* 
	 * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
	 */
	public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
		instance.ipAddress = InetAddress.getByName(hostName);
		instance.portNumber = Integer.parseInt(portNumber);
		instance.socket = new DatagramSocket();

		instance.inputFile = checkFile(fileName); //check if the CSV file does exist
		instance.outputFileName =  outputFileName;
		instance.maxPatchSize= Integer.parseInt(batchSize);

		instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
		instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

		instance.fileTotalReadings = 0; 
		instance.sentReadings=0;
		instance.totalSegments =0;

		instance.timeout = DEFAULT_TIMEOUT;
		instance.maxRetries = DEFAULT_RETRIES;
		instance.currRetry = 0;		 
	}


	/* 
	 * check if the csv file does exist before sending it 
	 */
	private static File checkFile(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists()) {
			System.out.println("CLIENT: File does not exists"); 
			System.out.println("CLIENT: Exit .."); 
			System.exit(0);
		}
		return file;
	}

	/* 
	 * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
	 */
	private static Boolean isLost(float prob) 
	{ 
		double randomValue = Math.random();  //0.0 to 99.9
		return randomValue <= prob;
	}

	/* 
	 * getter and setter methods	 *
	 */
	public String getOutputFileName() {
		return outputFileName;
	} 

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	} 

	public int getMaxPatchSize() {
		return maxPatchSize;
	} 

	public void setMaxPatchSize(int maxPatchSize) {
		this.maxPatchSize = maxPatchSize;
	} 

	public int getFileTotalReadings() {
		return fileTotalReadings;
	} 

	public void setFileTotalReadings(int fileTotalReadings) {
		this.fileTotalReadings = fileTotalReadings;
	}

	public void setDataSeg(Segment dataSeg) {
		this.dataSeg = dataSeg;
	}

	public void setAckSeg(Segment ackSeg) {
		this.ackSeg = ackSeg;
	}

	public void setCurrRetry(int currRetry) {
		this.currRetry = currRetry;
	}

}
