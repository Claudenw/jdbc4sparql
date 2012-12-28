package org.xenei.jdbc4sparql.meta;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;

public class MetaCatalog extends MetaNamespace implements Catalog
{

	private MetaSchema schema;
	
	public MetaCatalog()
	{
		this.schema = new MetaSchema( this );
	}
	

	@Override
	public String getLocalName()
	{
		return "Jdbc4Sparql_METADATA";
	}

	@Override
	public Set<Schema> getSchemas()
	{
		return new HashSet<Schema>( Arrays.asList( new Schema[] {schema}));
	}
	
	public MetaSchema getSchema()
	{
		return schema;
	}

	@Override
	public Schema getSchema( String schema )
	{
		if (this.schema.getLocalName().equals(schema))
		{
			return this.schema;
		}
		return null;
	}

}
