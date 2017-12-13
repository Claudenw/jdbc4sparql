import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import net.sf.jsqlparser.JSQLParserException;

public class SQLDisplay {

	public static void listFunctions() throws ClassNotFoundException,
	SQLException {

		Class.forName(SQLDisplay.DRIVER);
		final Connection connection = DriverManager.getConnection(
				SQLDisplay.URL, SQLDisplay.USERNAME, SQLDisplay.PASSWORD);
		final DatabaseMetaData metadata = connection.getMetaData();

		final ResultSet rs = metadata.getFunctions(null, null, null);
		SQLDisplay.printRS("Functions", rs);
	}

	public static void listNumericFunctions() throws ClassNotFoundException,
	SQLException {

		Class.forName(SQLDisplay.DRIVER);
		final Connection connection = DriverManager.getConnection(
				SQLDisplay.URL, SQLDisplay.USERNAME, SQLDisplay.PASSWORD);
		final DatabaseMetaData metadata = connection.getMetaData();

		final String[] functions = metadata.getNumericFunctions()
				.split(",\\s*");

		for (final String function : functions) {
			System.out.println("Numeric Function = " + function);
		}
	}

	public static void listSQLKeywords() throws ClassNotFoundException,
	SQLException {

		Class.forName(SQLDisplay.DRIVER);
		final Connection connection = DriverManager.getConnection(
				SQLDisplay.URL, SQLDisplay.USERNAME, SQLDisplay.PASSWORD);
		final DatabaseMetaData metadata = connection.getMetaData();

		final String[] functions = metadata.getSQLKeywords().split(",\\s*");

		for (final String function : functions) {
			System.out.println("SQL keyword = " + function);
		}
	}

	public static void listStringFunctions() throws ClassNotFoundException,
	SQLException {

		Class.forName(SQLDisplay.DRIVER);
		final Connection connection = DriverManager.getConnection(
				SQLDisplay.URL, SQLDisplay.USERNAME, SQLDisplay.PASSWORD);
		final DatabaseMetaData metadata = connection.getMetaData();

		final String[] functions = metadata.getStringFunctions().split(",\\s*");

		for (final String function : functions) {
			System.out.println("String Function = " + function);
		}
	}

	public static void listSystemFunctions() throws ClassNotFoundException,
	SQLException {

		Class.forName(SQLDisplay.DRIVER);
		final Connection connection = DriverManager.getConnection(
				SQLDisplay.URL, SQLDisplay.USERNAME, SQLDisplay.PASSWORD);
		final DatabaseMetaData metadata = connection.getMetaData();

		final String[] functions = metadata.getSystemFunctions().split(",\\s*");

		for (final String function : functions) {
			System.out.println("System Function = " + function);
		}
	}

	/**
	 * @param args
	 * @throws JSQLParserException
	 */
	public static void main(final String[] args) throws Exception {
	    DRIVER = args[0];
	    URL = args[1];
	    if (args.length>2)
	    {
	        USERNAME=args[2];
	        if (args.length>3)
	        {
	            PASSWORD = args[3];
	        }
	    }

		SQLDisplay.listSystemFunctions();
		SQLDisplay.listStringFunctions();
		SQLDisplay.listNumericFunctions();
		SQLDisplay.listSQLKeywords();
		SQLDisplay.listFunctions();
	}

	private static void printRS(final String name, final ResultSet rs)
			throws SQLException {
		final ResultSetMetaData meta = rs.getMetaData();
		System.out.println(String.format("%s", name));
		while (rs.next()) {
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				System.out.println(String.format("%s=%s",
						meta.getColumnName(i), rs.getObject(i)));
			}
		}
	}

	private static String DRIVER;

	private static String URL;

	private static String USERNAME = "";

	private static String PASSWORD = "";

	public SQLDisplay() {
	}

}
