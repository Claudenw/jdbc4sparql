package org.xenei.jdbc4sparql.mock;

import java.net.MalformedURLException;
import java.util.Properties;

import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SURL;

public class MockConnection extends J4SConnection
{

	public MockConnection( final J4SDriver driver, J4SURL url,
			final Properties properties  ) throws MalformedURLException
	{
		super(driver, url, properties);

	}

}
