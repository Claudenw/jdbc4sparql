package org.xenei.jdbc4sparql.example;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jena.entities.MissingAnnotation;

public class SQLClient
{

	private static void doIO( final Connection connection, final String queryStr )
			throws SQLException
	{
		final DatabaseMetaData md = connection.getMetaData();
		ResultSet rs = md.getTables(null, null, null, null);
		while (rs.next())
		{
			System.out.println(String.format("%s.%s.%s",
					rs.getString("TABLE_CAT"), rs.getString("TABLE_SCHEM"),
					rs.getString("TABLE_NAME")));
		}
		rs.close();
		final Statement stmt = connection.createStatement();
		stmt.execute("SELECT * FROM Agent_data");
		rs = stmt.getResultSet();
		final ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next())
		{
			for (int i = 0; i < rsmd.getColumnCount(); i++)
			{
				System.out.print(String.format("[%s]%s ",
						rsmd.getColumnName(i + 1), rs.getString(i + 1)));
			}
			System.out.println();
		}
		rs.close();
		stmt.close();
	}

	public static void main( final String[] args ) throws URISyntaxException,
			IOException, SQLException, ClassNotFoundException,
			InstantiationException, IllegalAccessException, MissingAnnotation
	{
		// final File outFile = ConfigBuilder.gatherSchema(Arrays.asList(args)
		// .subList(1, args.length));

		final J4SDriver driver = new J4SDriver();

		final String urlStr = String.format("jdbc:j4s:file:%s",
				"/tmp/configBuilder.zip");
		final J4SUrl url = new J4SUrl(urlStr);
		System.out.println("Opening " + url);
		final Properties properties = new Properties();

		final J4SConnection connection = new J4SConnection(driver, url,
				properties);
		SQLClient.doIO(connection, "");

	}
}
