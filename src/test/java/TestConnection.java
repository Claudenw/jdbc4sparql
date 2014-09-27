import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;

import java.net.URL;
import java.sql.ResultSet;
import java.util.Properties;

import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SConnectionTest;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SUrl;

public class TestConnection
{

	/**
	 * arg[0] is the j4s URL. If not provided defaults to
	 * jdbc:j4s?builder=org.xenei.jdbc4sparql.sparql.builders.
	 * SimpleNullableBuilder&type=turtle:file:./J4SStatementTest.ttl
	 * 
	 * arg[1] is the output file name. If not provided defaults to
	 * config.zip in the system temp directory.
	 * 
	 * @param args
	 * @throws ParseException
	 */
	public static void main( final String[] args ) throws Exception
	{
		final J4SDriver driver = new J4SDriver();
		String urlStr = null;
		if (args.length > 0)
		{
			urlStr = args[0];
		}
		else
		{
			final URL fUrl = J4SConnectionTest.class
					.getResource("./J4SStatementTest.ttl");
			urlStr = "jdbc:j4s:file:/tmp/configBuilder.zip" ;
		}
		final J4SUrl url = new J4SUrl(urlStr);
		final Properties properties = new Properties();
		final J4SConnection connection = new J4SConnection(driver, url,
				properties);

		ResultSet rs = connection.getMetaData().getCatalogs();
		while (rs.next())
		{
			System.out.println( rs.getString(1));
		}
		
		connection.close();
	}

}
