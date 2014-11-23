import java.sql.SQLException;
import java.util.Iterator;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;

import net.sf.jsqlparser.JSQLParserException;

public class TypeDisplay {

	public static void listSPARQLTypes() throws ClassNotFoundException,
			SQLException {
		Iterator<RDFDatatype> iter = TypeMapper.getInstance().listTypes();
		while (iter.hasNext()) {
			RDFDatatype dt = iter.next();
			Class<?> cls = dt.getJavaClass();
			System.out.println(String.format(
					"new SPARQLToJava(\"%s\", %s.class),", dt.getURI(),
					cls == null ? "String" : cls.getSimpleName()));
		}

	}

	// public static void listNumericFunctions() throws ClassNotFoundException,
	// SQLException
	// {
	//
	// Class.forName(TypeDisplay.DRIVER);
	// final Connection connection = DriverManager.getConnection(
	// TypeDisplay.URL, TypeDisplay.USERNAME, TypeDisplay.PASSWORD);
	// final DatabaseMetaData metadata = connection.getMetaData();
	//
	// final String[] functions = metadata.getNumericFunctions()
	// .split(",\\s*");
	//
	// for (final String function : functions)
	// {
	// System.out.println("Numeric Function = " + function);
	// }
	// }
	//
	// public static void listSQLKeywords() throws ClassNotFoundException,
	// SQLException
	// {
	//
	// Class.forName(TypeDisplay.DRIVER);
	// final Connection connection = DriverManager.getConnection(
	// TypeDisplay.URL, TypeDisplay.USERNAME, TypeDisplay.PASSWORD);
	// final DatabaseMetaData metadata = connection.getMetaData();
	//
	// final String[] functions = metadata.getSQLKeywords().split(",\\s*");
	//
	// for (final String function : functions)
	// {
	// System.out.println("SQL keyword = " + function);
	// }
	// }
	//
	// public static void listStringFunctions() throws ClassNotFoundException,
	// SQLException
	// {
	//
	// Class.forName(TypeDisplay.DRIVER);
	// final Connection connection = DriverManager.getConnection(
	// TypeDisplay.URL, TypeDisplay.USERNAME, TypeDisplay.PASSWORD);
	// final DatabaseMetaData metadata = connection.getMetaData();
	//
	// final String[] functions = metadata.getStringFunctions().split(",\\s*");
	//
	// for (final String function : functions)
	// {
	// System.out.println("String Function = " + function);
	// }
	// }
	//
	// public static void listSystemFunctions() throws ClassNotFoundException,
	// SQLException
	// {
	//
	// Class.forName(TypeDisplay.DRIVER);
	// final Connection connection = DriverManager.getConnection(
	// TypeDisplay.URL, TypeDisplay.USERNAME, TypeDisplay.PASSWORD);
	// final DatabaseMetaData metadata = connection.getMetaData();
	//
	// final String[] functions = metadata.getSystemFunctions().split(",\\s*");
	//
	// for (final String function : functions)
	// {
	// System.out.println("System Function = " + function);
	// }
	// }

	/**
	 * @param args
	 * @throws JSQLParserException
	 */
	public static void main(final String[] args) throws Exception {
		// String sqlQuery = "Select MAX(foo) as junk from tbl";
		// CCJSqlParserManager parserManager = new CCJSqlParserManager();
		// final Statement stmt = parserManager.parse(new StringReader(
		// sqlQuery));
		// System.out.println( stmt.toString() );
		TypeDisplay.listSPARQLTypes();
		// TypeDisplay.listStringFunctions();
		// TypeDisplay.listNumericFunctions();
		// TypeDisplay.listSQLKeywords();
		// TypeDisplay.listFunctions();
	}

	// private static void printRS( final String name, final ResultSet rs )
	// throws SQLException
	// {
	// final ResultSetMetaData meta = rs.getMetaData();
	// System.out.println(String.format("%s", name));
	// while (rs.next())
	// {
	// for (int i = 1; i <= meta.getColumnCount(); i++)
	// {
	// System.out.println(String.format("%s=%s",
	// meta.getColumnName(i), rs.getObject(i)));
	// }
	// }
	// }
	//
	// private static final String DRIVER = "com.mysql.jdbc.Driver";
	//
	// private static final String URL = "jdbc:mysql://127.0.0.1/test";
	//
	// private static final String USERNAME = "claude";
	//
	// private static final String PASSWORD = "";

	public TypeDisplay() {
		// TODO Auto-generated constructor stub
	}

}
