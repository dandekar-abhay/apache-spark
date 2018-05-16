//package apache.spark.poc.controller;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Timer;
//import java.util.TimerTask;
//
//import org.apache.spark.launcher.SparkAppHandle;
//import org.apache.spark.launcher.SparkLauncher;
//
//public class Controller {
//
//	class StatusDisplayer extends TimerTask {
//
//		SparkAppHandle handle;
//
//		@Override
//		public void run() {
//			System.out.println("Spark Status: " + handle.getState());
//		}
//
//		public StatusDisplayer(SparkAppHandle handle) {
//			this.handle = handle;
//		}
//
//	}
//
//	class SparkStopper extends TimerTask {
//
//		SparkAppHandle handle;
//
//		boolean alreadyExecuted = false;
//
//		@Override
//		public void run() {
//
//			if ( !alreadyExecuted) {
//				File spinFile = new File("/tmp/stop.spark");
//				if (spinFile.exists()) {
//
//					alreadyExecuted = true;
//					System.out.println("Found spinfile ... Stopping spark");
//					handle.stop();
//					// handle.kill();
//					System.out.println("Handle stop executed...");
//				}
//			}
//
//		}
//
//		public SparkStopper(SparkAppHandle handle) {
//			this.handle = handle;
//		}
//
//	}
//
//	private void execute() {
//		SparkLauncher launcher = new SparkLauncher().setAppResource(
//				"/home/abhay/MyHome/WorkHome/CodeHome/Apache/Spark/MyExamples/Java/apache-spark/spark.kafka.poc/target/kafka.poc-0.0.3-SNAPSHOT.jar")
//				.setMainClass("apache.spark.poc.SparkStructuredStreamProcessor").setMaster("local[2]")
//				.setConf(SparkLauncher.DRIVER_MEMORY, "1g").setConf(SparkLauncher.EXECUTOR_MEMORY, "1g")
//				.setConf(SparkLauncher.EXECUTOR_CORES, "1");
//
//		try {
//			SparkAppHandle handle = launcher.startApplication();
//			Timer timer = new Timer();
//			TimerTask statusTask = this.new StatusDisplayer(handle);
//			TimerTask stopperTask = this.new SparkStopper(handle);
//			timer.schedule(statusTask, 1000, 3 * 1000);
//			timer.schedule(stopperTask, 1000, 5 * 1000);
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//	}
//
//	public static void main(String[] args) {
//
//		new Controller().execute();
//
//	}
//
//}
