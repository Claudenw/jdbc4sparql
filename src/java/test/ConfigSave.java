import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.lang.sparql_11.SPARQLParser11;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Reader;
import java.io.StringReader;
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
	public static void main( String[] args ) throws Exception
	{
		J4SDriver driver = new J4SDriver();
		URL fUrl = J4SConnectionTest.class.getResource("./J4SDriverTest.ttl");
		J4SUrl url = new J4SUrl("jdbc:j4s?type=turtle:"+fUrl.toExternalForm());
		Properties properties = new Properties();
		J4SConnection connection = new J4SConnection(driver, url, properties );
		
		File f = new File( "/tmp/config.zip");
		FileOutputStream fos = new FileOutputStream( f );
		connection.saveConfig( fos );
		fos.close();
	}

}
