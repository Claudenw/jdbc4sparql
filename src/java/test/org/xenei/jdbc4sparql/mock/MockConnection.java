package org.xenei.jdbc4sparql.mock;

import java.util.Properties;

import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SDriver;

public class MockConnection extends J4SConnection
{

	public MockConnection( J4SDriver driver, String url, Properties props )
	{
		super(driver, url, props);

	}

}
