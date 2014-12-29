package org.xenei.jdbc4sparql.impl.rdf;

import org.xenei.jdbc4sparql.iface.NamespacedObject;
import org.xenei.jena.entities.ResourceWrapper;

public abstract class RdfNamespacedObject implements NamespacedObject,
		ResourceWrapper {

	@Override
	public final String getFQName() {
		return this.getResource().getURI();
	}

	@Override
	public final String getLocalName() {
		return this.getResource().asNode().getLocalName();
	}

	@Override
	public final String getNamespace() {
		return this.getResource().asNode().getNameSpace();
	}
}
