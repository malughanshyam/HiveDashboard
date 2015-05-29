/**
 * @author gmalu
 *
 */

import java.sql.SQLException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.*;

public class HiveExecutor {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private String jobID;
	private String hiveHost;
	private String dbName;
	private String hiveUser;
	private String queryFilePath;
	private Connection con;
	private Statement stmt;
	private String statusFilePath;
	private String outputDir ;
	private String resultFilePath;
	private ResultSet res;
	private Exception occurredException;
	
	HiveExecutor (String [] args){	
		this.jobID = args[0];
		this.hiveUser = args[1];
		this.hiveHost = args[2];
		this.dbName = args[3];
		this.queryFilePath = args[4];
		this.outputDir = "./data/"+this.jobID;
		this.resultFilePath = this.outputDir +"/result.txt";
		this.statusFilePath = this.outputDir +"/status.txt";
	}
	
	public static void main(String[] args)  throws SQLException, IOException {
		
		if (args.length != 5) {
			usage();
		}

		HiveExecutor hiveExecObj = new HiveExecutor(args);
		hiveExecObj.establishConnection();
		String sql = hiveExecObj.readFile(hiveExecObj.queryFilePath);
		
		hiveExecObj.createOutputDirectory();
		
		boolean jobSuccessFlag = hiveExecObj.executeQuery(sql);
		
		if (jobSuccessFlag == true){			
			hiveExecObj.exportResult();
			hiveExecObj.copyQueryFileToOuputDir();
		}

		hiveExecObj.updateStatusFile(jobSuccessFlag);
	
	}


	private static void usage() {
		System.err.println("Usage : java " + HiveExecutor.class.getName()
				+ " jobID hiveUserName hiveHost dbName hiveQueryFile");
		System.exit(1);
	}

	private void establishConnection()  throws SQLException{
		
		try {
			Class.forName(driverName);
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		String connectionURL = "jdbc:hive2://" + hiveHost + "/" + dbName;
		this.con = DriverManager.getConnection(connectionURL, hiveUser, "");
		// Connection con = DriverManager.getConnection("jdbc:hive2://172.16.226.129:10000/default", "hive", "");
		this.stmt = con.createStatement();
	}

	private String readFile(String path) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		String query = new String(encoded, Charset.defaultCharset());
		query = query.replaceAll("\r", "").replaceAll("\n", " ").replaceAll(";", "");
		return query;
	}
	
	private boolean executeQuery(String sql)   throws IOException, SQLException {
		boolean jobSuccessFlag = false;
		
		try {
			System.out.println("JobID : " + this.jobID);
			System.out.println("Running : " + sql);
			
			this.res = stmt.executeQuery(sql);
			jobSuccessFlag = true;
		
		} catch (Exception e){
			System.out.println("Job Failed");
			this.occurredException = e;
			jobSuccessFlag = false;
		} finally {
			
		}
		return jobSuccessFlag;
		
	}
	
	private void createOutputDirectory(){
		String dirname = this.outputDir;
	      File d = new File(dirname);
	      // Create directory now.
	      d.mkdirs();
	}

	private void exportResult() throws SQLException, FileNotFoundException, UnsupportedEncodingException{
		
		PrintWriter writer = new PrintWriter(this.resultFilePath, "UTF-8");
		ResultSetMetaData rsmd;
		
		while (this.res.next()) {
			rsmd = this.res.getMetaData();
			int numOfCols = rsmd.getColumnCount();
			for (int i = 1; i <= numOfCols; i++) {	
				writer.print(this.res.getString(i) + "\t");
			}
			writer.println();
		}
		writer.close();
		System.out.println("Output written :"+this.resultFilePath);
	}

	private void copyQueryFileToOuputDir() throws IOException {
		
		File sourceFile = new File(this.queryFilePath);
		File destFile = new File(this.outputDir + "/sql.txt");
		Files.copy(sourceFile.toPath(), destFile.toPath());
		
	}


	private void updateStatusFile(boolean jobSuccessFlag) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(this.statusFilePath, "UTF-8");
		if (jobSuccessFlag == true){
			writer.println("SUCCESS");
		}
		else{
			writer.println("FAILED");
			this.occurredException.printStackTrace(writer);
		}
		writer.close();
	}
	
	public static void printColHeaders(ResultSet res) throws SQLException {
		ResultSetMetaData rsmd;
		while (res.next()) {
			rsmd = res.getMetaData();
			int numOfCols = rsmd.getColumnCount();
			for (int i = 1; i <= numOfCols; i++) {
				System.out.print(rsmd.getColumnName(i) + "\t");
			}
			System.out.println();
			// System.out.println(String.valueOf(res));
		}
	}
}
