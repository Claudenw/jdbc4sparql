package org.xenei.jdbc4sparql.meta;

import org.xenei.jdbc4sparql.iface.NamespacedObject;

public abstract class MetaNamespace implements NamespacedObject
{

	@Override
	public String getNamespace()
	{
		return "http://org.xenei.jdbc4sparql/meta#";
	}

}
