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
//	private static class MetaDS implements Dataset {
//		Dataset ds;
//		MultiUnion g;
//		Model m;
//
//		public MetaDS(final Dataset ds) {
//			this.ds = ds;
//			rescan();
//		}
//
//		@Override
//		public void abort() {
//			ds.abort();
//		}
//
//		@Override
//		public void addNamedModel(final String uri, final Model model)
//				throws LabelExistsException {
//			ds.addNamedModel(uri, model);
//			g.addGraph(ds.getNamedModel(uri).getGraph());
//		}
//
//		@Override
//		public DatasetGraph asDatasetGraph() {
//			return ds.asDatasetGraph();
//		}
//
//		@Override
//		public void begin(final ReadWrite readWrite) {
//			ds.begin(readWrite);
//		}
//
//		@Override
//		public void close() {
//			ds.close();
//		}
//
//		@Override
//		public void commit() {
//			ds.commit();
//		}
//
//		@Override
//		public boolean containsNamedModel(final String uri) {
//			return "urn:x-arq:UnionGraph".equals(uri) ? true : ds
//					.containsNamedModel(uri);
//		}
//
//		@Override
//		public void end() {
//			ds.end();
//		}
//
//		@Override
//		public Context getContext() {
//			return ds.getContext();
//		}
//
//		@Override
//		public Model getDefaultModel() {
//			return ds.getDefaultModel();
//		}
//
//		@Override
//		public Lock getLock() {
//			return ds.getLock();
//		}
//
//		@Override
//		public Model getNamedModel(final String uri) {
//			if ("urn:x-arq:UnionGraph".equals(uri)) {
//				return m;
//			}
//			final boolean hasModel = ds.containsNamedModel(uri);
//
//			final Model model = ds.getNamedModel(uri);
//			if (!hasModel) {
//				g.addGraph(model.getGraph());
//			}
//			return model;
//		}
//
//		@Override
//		public boolean isInTransaction() {
//			return ds.isInTransaction();
//		}
//
//		@Override
//		public Iterator<String> listNames() {
//			return ds.listNames();
//		}
//
//		@Override
//		public void removeNamedModel(final String uri) {
//			g.removeGraph(ds.getNamedModel(uri).getGraph());
//			ds.removeNamedModel(uri);
//		}
//
//		@Override
//		public void replaceNamedModel(final String uri, final Model model) {
//			g.removeGraph(ds.getNamedModel(uri).getGraph());
//			ds.replaceNamedModel(uri, model);
//			g.addGraph(ds.getNamedModel(uri).getGraph());
//		}
//
//		public void rescan() {
//			g = new MultiUnion();
//			final Iterator<String> iter = ds.listNames();
//			while (iter.hasNext()) {
//				g.addGraph(ds.getNamedModel(iter.next()).getGraph());
//			}
//			m = ModelFactory.createModelForGraph(g);
//		}
//
//		@Override
//		public void setDefaultModel(final Model model) {
//			ds.setDefaultModel(model);
//		}
//
//		@Override
//		public boolean supportsTransactions() {
//			return ds.supportsTransactions();
//		}
//
//		@Override
//		public Model getUnionModel() {
//			return m;
//		}
//
//		@Override
//		public boolean supportsTransactionAbort() {
//			return false;
//		}
//
//		@Override
//		public boolean isEmpty() {
//			return m.isEmpty();
//		}
//
//	}

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