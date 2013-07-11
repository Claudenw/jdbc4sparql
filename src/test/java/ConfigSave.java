import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.Properties;

import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SConnectionTest;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SUrl;

public class ConfigSave
{

	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main( final String[] args ) throws Exception
	{
		final J4SDriver driver = new J4SDriver();
		final URL fUrl = J4SConnectionTest.class
				.getResource("./J4SStatementTest.ttl");
		final J4SUrl url = new J4SUrl("jdbc:j4s?builder=org.xenei.jdbc4sparql.sparql.builders.SimpleNullableBuilder&type=turtle:"
				+ fUrl.toExternalForm());
		final Properties properties = new Properties();
		final J4SConnection connection = new J4SConnection(driver, url,
				properties);

		final File f = new File("/tmp/config.zip");
		final FileOutputStream fos = new FileOutputStream(f);
		connection.saveConfig(fos);
		fos.close();
		connection.close();
	}

}
