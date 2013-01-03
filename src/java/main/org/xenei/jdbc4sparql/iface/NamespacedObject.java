package org.xenei.jdbc4sparql.iface;

public interface NamespacedObject
{
	String getFQName();

	String getLocalName();

	String getNamespace();
}
