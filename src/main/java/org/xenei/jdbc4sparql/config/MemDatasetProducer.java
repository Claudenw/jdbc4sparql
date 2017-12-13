package org.xenei.jdbc4sparql.config;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.zip.ZipInputStream;

import org.xenei.jdbc4sparql.impl.AbstractDatasetProducer;

import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.LabelExistsException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.util.Context;

public class MemDatasetProducer extends AbstractDatasetProducer {

	public MemDatasetProducer() {
		this(new Properties());
	}

	public MemDatasetProducer(final Properties props) {
		super(props, DatasetFactory.create(), DatasetFactory
					.create());
	}

	public MemDatasetProducer(final Properties props, final ZipInputStream zis)
			throws IOException {
		this(props);
		load(zis);
	}
}