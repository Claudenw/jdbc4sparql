package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;

public class MockJ4SUrl extends J4SUrl
{

	public MockJ4SUrl()
	{
		super("jdbc:j4s?type=sparql:http://example.com");
	}

	@Override
	public SchemaBuilder getBuilder()
	{
		return new MockSchemaBuilder();
	}

}
