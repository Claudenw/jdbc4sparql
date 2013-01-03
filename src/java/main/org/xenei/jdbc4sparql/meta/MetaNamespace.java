package org.xenei.jdbc4sparql.meta;

import org.xenei.jdbc4sparql.iface.NamespacedObject;

public abstract class MetaNamespace implements NamespacedObject
{
	public static final String NS = "http://org.xenei.jdbc4sparql/meta#";

	@Override
	public String getFQName()
	{
		return getNamespace() + getLocalName();
	}

	@Override
	public String getNamespace()
	{
		return MetaNamespace.NS;
	}

}
