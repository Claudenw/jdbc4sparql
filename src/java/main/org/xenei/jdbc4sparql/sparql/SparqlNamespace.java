package org.xenei.jdbc4sparql.sparql;

import org.xenei.jdbc4sparql.iface.NamespacedObject;

public class SparqlNamespace implements NamespacedObject
{
	private String namespace;
	private String localName;

	protected SparqlNamespace( String namespace, String localName)
	{
		this.namespace = namespace;
		this.localName=localName;
		if (! (namespace.endsWith("#") || namespace.endsWith( "/" )  ))
		{
			namespace+=namespace.contains("#")?"/":"#";
		}
	}
	
	@Override
	public String getNamespace()
	{
		return namespace;
	}

	@Override
	public String getLocalName()
	{
		return localName;
	}

	@Override
	public String getFQName()
	{
		return namespace+localName;
	}

}
