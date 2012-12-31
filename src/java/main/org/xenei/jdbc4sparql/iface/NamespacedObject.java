package org.xenei.jdbc4sparql.iface;

public interface NamespacedObject
{
	String getNamespace();
	String getLocalName();
	String getFQName();
}
