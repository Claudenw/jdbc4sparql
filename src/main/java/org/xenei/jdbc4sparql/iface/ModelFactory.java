package org.xenei.jdbc4sparql.iface;

import com.hp.hpl.jena.rdf.model.Model;

import java.util.Properties;

public interface ModelFactory {
	Model createModel(final Properties properties);

}
