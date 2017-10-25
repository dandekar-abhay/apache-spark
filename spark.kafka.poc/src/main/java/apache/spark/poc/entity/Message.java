package apache.spark.poc.entity;

import java.io.IOException;
import java.io.Serializable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.Assert;

/** This class will contain the message format from 
 * kafka
 * 
 * @author abhay
 *
 */

/*
Task ID	Auto Generated
File Name	FOPS_<DateTime>
Skip Header	True/False
Physical Location in HDFS	/user/fusionops/ispring/import/<Customer Name>/
*/

public class Message implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String taskId;
	
	private String fileName;
	
	private boolean skipHeader;
	
	private String hdfsLocation;
	
	private boolean skipProcessing;

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public boolean isSkipHeader() {
		return skipHeader;
	}

	public void setSkipHeader(boolean skipHeader) {
		this.skipHeader = skipHeader;
	}

	public String getHdfsLocation() {
		return hdfsLocation;
	}

	public void setHdfsLocation(String hdfsLocation) {
		this.hdfsLocation = hdfsLocation;
	}
	
	public boolean isSkipProcessing() {
    return skipProcessing;
  }

  public void setSkipProcessing(boolean skipProcessing) {
    this.skipProcessing = skipProcessing;
  }

  @Override
	public String toString() {
		return taskId+":"+fileName+":"+skipHeader+":"+hdfsLocation+":"+skipProcessing;
	}
	
	public Message(String taskId, String fileName, boolean skipHeader, String hdfsLocation, boolean skipProcessing  ) {
		this.taskId = taskId;
		this.fileName = fileName;
		this.skipHeader = skipHeader;
		this.hdfsLocation = hdfsLocation;
		this.skipProcessing = skipProcessing;
	}
	
	public Message() {
		
	}
	
	public static void main(String[] args) {
		
		myUnitTest();
	}
	
	private static void myUnitTest(){
		
		Message testMessage = new Message("0", "Input-File_10", true, "HDFS-File-Location_10", false);

		ObjectMapper mapper = new ObjectMapper();
		try {
			String newJson = mapper.writeValueAsString(testMessage);
			System.out.println("To String:" + newJson);
			
			Message retrievedMsg = mapper.readValue(newJson.getBytes(), Message.class);
			System.out.println("From String: " + retrievedMsg.toString());
			System.out.println("Expected   : " + "0:Input-File_10:true:HDFS-File-Location_10:false");
			Assert.assertEquals("0:Input-File_10:true:HDFS-File-Location_10:false", retrievedMsg.toString());
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
