package org.xenei.jdbc4sparql.iface;

import java.util.Properties;

import com.hp.hpl.jena.rdf.model.Model;

public interface ModelFactory {
	Model createModel(final Properties properties);

}
