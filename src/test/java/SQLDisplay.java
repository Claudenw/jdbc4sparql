import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import net.sf.jsqlparser.JSQLParserException;



public class SQLDisplay {
	
	private static final String DRIVER = "com.mysql.jdbc.Driver";

	  private static final String URL = "jdbc:mysql://127.0.0.1/test";

	  private static final String USERNAME = "claude";

	  private static final String PASSWORD = "";

	public SQLDisplay() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws JSQLParserException 
	 */
	public static void main(String[] args) throws Exception {
//		String sqlQuery = "Select MAX(foo) as junk from tbl";
//		CCJSqlParserManager parserManager = new CCJSqlParserManager();
//		final Statement stmt = parserManager.parse(new StringReader(
//				sqlQuery));
//		System.out.println( stmt.toString() );
		listSystemFunctions();
		listStringFunctions();
		listNumericFunctions();
		listSQLKeywords();
		listFunctions();
	}
	
	 public static void listSystemFunctions() throws ClassNotFoundException, SQLException {
		 
		    Class.forName(DRIVER);
		    Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		    DatabaseMetaData metadata = connection.getMetaData();

		    String[] functions = metadata.getSystemFunctions().split(",\\s*");

		    for (int i = 0; i < functions.length; i++) {
		      String function = functions[i];
		      System.out.println("System Function = " + function);
		    }
	 }
	 
	 public static void listStringFunctions() throws ClassNotFoundException, SQLException {
		 
		    Class.forName(DRIVER);
		    Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		    DatabaseMetaData metadata = connection.getMetaData();

		    String[] functions = metadata.getStringFunctions().split(",\\s*");

		    for (int i = 0; i < functions.length; i++) {
		      String function = functions[i];
		      System.out.println("String Function = " + function);
		    }
	 }
	 
	 public static void listNumericFunctions() throws ClassNotFoundException, SQLException {
		 
		    Class.forName(DRIVER);
		    Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		    DatabaseMetaData metadata = connection.getMetaData();

		    String[] functions = metadata.getNumericFunctions().split(",\\s*");

		    for (int i = 0; i < functions.length; i++) {
		      String function = functions[i];
		      System.out.println("Numeric Function = " + function);
		    }
	 }
	
	 public static void listSQLKeywords() throws ClassNotFoundException, SQLException {
		 
		    Class.forName(DRIVER);
		    Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		    DatabaseMetaData metadata = connection.getMetaData();

		    String[] functions = metadata.getSQLKeywords().split(",\\s*");

		    for (int i = 0; i < functions.length; i++) {
		      String function = functions[i];
		      System.out.println("SQL keyword = " + function);
		    }
	 }

	 public static void listFunctions() throws ClassNotFoundException, SQLException {
		 
		    Class.forName(DRIVER);
		    Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		    DatabaseMetaData metadata = connection.getMetaData();

		    ResultSet rs = metadata.getFunctions(null,null,null);
		    printRS( "Functions", rs );
	 }
	 
	 private static void printRS( String name, ResultSet rs) throws SQLException
	 {
		 ResultSetMetaData meta = rs.getMetaData();
		 System.out.println( String.format( "%s", name)  );
		 while (rs.next())
		 {
			 for (int i=1;i<=meta.getColumnCount();i++)
			 {
				 System.out.println( String.format( "%s=%s", meta.getColumnName(i), rs.getObject(i)));
			 }
		 }
	 }


}
