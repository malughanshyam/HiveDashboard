/**
 * @author gmalu
 *
 */

import java.sql.SQLException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

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
	private String resultFilePath;
	
	HiveExecutor (String [] args){	
		this.jobID = args[0];
		this.hiveUser = args[1];
		this.hiveHost = args[2];
		this.dbName = args[3];
		this.queryFilePath = args[4];
	}
	
	public static void main(String[] args)  throws SQLException, IOException {
		
		if (args.length != 5) {
			usage();
		}

		HiveExecutor hiveExecObj = new HiveExecutor(args);
		hiveExecObj.establishConnection();
		String sql = hiveExecObj.readFile(hiveExecObj.queryFilePath);
		hiveExecObj.executeQuery(sql);
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
		query = query.replaceAll("\r", "").replaceAll("\n", "").replaceAll(";", "");
		return query;
	}
	
	private void executeQuery(String sql)   throws SQLException, IOException {
		System.out.println("JobID : " + this.jobID);
		System.out.println("Running : " + sql);
		ResultSet res = stmt.executeQuery(sql);
		ResultSetMetaData rsmd;
		while (res.next()) {
			rsmd = res.getMetaData();
			int numOfCols = rsmd.getColumnCount();
			for (int i = 1; i <= numOfCols; i++) {
				System.out.print(res.getString(i) + "\t");
			}
			System.out.println();
		}

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
