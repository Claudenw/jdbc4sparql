package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.iterator.WrappedIterator;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/Catalog#")
public class RdfCatalog implements Catalog, ResourceWrapper {
	public static class Builder implements Catalog {
		public static String getFQName(final String shortName) {
			return String.format("%s/instance/N%s",
					ResourceBuilder.getFQName(RdfCatalog.class), shortName);
		}

		private Model localModel;
		private URL sparqlEndpoint;
		private String name;

		private final Set<Schema> schemas = new HashSet<Schema>();

		public Builder() {
		}

		public Builder(final RdfCatalog catalog) throws MalformedURLException {
			this();
			setName(catalog.getShortName());
			if (catalog.getSparqlEndpoint() != null) {
				setSparqlEndpoint(new URL(catalog.getSparqlEndpoint()));
			}
			if (catalog.localModel != null) {
				setLocalModel(catalog.localModel);
			}
		}

		public RdfCatalog build(final Model model) {

			if (model == null) {
				throw new IllegalArgumentException("Model may not be null");
			}
			checkBuildState();
			final Class<?> typeClass = RdfCatalog.class;
			final String fqName = getFQName();
			final ResourceBuilder builder = new ResourceBuilder(model);

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();

			// create catalog graph resource
			Resource catalog = null;
			if (builder.hasResource(fqName)) {
				catalog = builder.getResource(fqName, typeClass);
			} else {
				catalog = builder.getResource(fqName, typeClass);
				catalog.addLiteral(RDFS.label, name);
				if (sparqlEndpoint != null) {
					catalog.addProperty(
							builder.getProperty(typeClass, "sparqlEndpoint"),
							sparqlEndpoint.toExternalForm());
				}

				for (final Schema scm : schemas) {
					if (scm instanceof ResourceWrapper) {
						catalog.addProperty(
								builder.getProperty(typeClass, "schema"),
								((ResourceWrapper) scm).getResource());
					}
				}
			}

			// create RdfCatalog object from graph object
			try {
				final RdfCatalog retval = entityManager.read(catalog,
						RdfCatalog.class);

				model.register(retval.new ChangeListener());
				retval.localModel = localModel != null ? localModel
						: ModelFactory.createMemModelMaker().createFreshModel();
				return retval;
			} catch (final MissingAnnotation e) {
				RdfCatalog.LOG.error(
						String.format("Error building %s: %s", name,
								e.getMessage()), e);
				throw new RuntimeException(e);
			}

		}

		protected void checkBuildState() {
			if (name == null) {
				throw new IllegalStateException("Name must be set");
			}
			if ((localModel == null) && (sparqlEndpoint == null)) {
				localModel = ModelFactory.createDefaultModel();
			}

		}

		@Override
		public void close() {
			throw new UnsupportedOperationException();
		}

		@Override
		public List<QuerySolution> executeLocalQuery(final Query query) {
			throw new UnsupportedOperationException();
		}

		@Override
		public NameFilter<Schema> findSchemas(final String schemaNamePattern) {
			return new NameFilter<Schema>(schemaNamePattern, schemas);
		}

		private String getFQName() {
			return Builder.getFQName(name);
		}

		public Model getLocalModel() {
			return localModel;
		}

		@Override
		public CatalogName getName() {
			return new CatalogName(name);
		}

		@Override
		public Schema getSchema(final String schemaName) {
			final NameFilter<Schema> nf = findSchemas(schemaName);
			return nf.hasNext() ? nf.next() : null;
		}

		@Override
		public Set<Schema> getSchemas() {
			return schemas;
		}

		public Builder setLocalModel(final Model localModel) {
			this.localModel = localModel;
			return this;
		}

		public Builder setName(final String name) {
			this.name = StringUtils.defaultString(name);
			return this;
		}

		public Builder setSparqlEndpoint(final URL sparqlEndpoint) {
			this.sparqlEndpoint = sparqlEndpoint;
			return this;
		}

	}

	public class ChangeListener extends
			AbstractChangeListener<Catalog, RdfSchema> {

		public ChangeListener() {
			super(RdfCatalog.this.getResource(), RdfCatalog.class, "schemas",
					RdfSchema.class);
		}

		@Override
		protected void addObject(final RdfSchema t) {
			schemaList.add(t);
		}

		@Override
		protected void clearObjects() {
			schemaList = null;
		}

		@Override
		protected boolean isListening() {
			return schemaList != null;
		}

		@Override
		protected void removeObject(final RdfSchema t) {
			schemaList.remove(t);
		}

	}

	// the model that contains the sparql data.
	private Model localModel;

	private Set<Schema> schemaList;

	private static Logger LOG = LoggerFactory.getLogger(RdfCatalog.class);

	@Override
	public void close() {
		localModel = null;
		schemaList = null;
	}

	/**
	 * Execute the query against the local Model.
	 *
	 * This is used to execute queries built by the query builder.
	 *
	 * @param query
	 * @return The list of QuerySolutions.
	 */
	@Override
	public List<QuerySolution> executeLocalQuery(final Query query) {

		final QueryExecution qexec = QueryExecutionFactory.create(query,
				localModel);
		try {
			ResultSet rs = qexec.execSelect();
			final List<QuerySolution> retval = WrappedIterator.create(rs)
					.toList();
			return retval;
		} catch (final Exception e) {
			RdfCatalog.LOG.error(
					"Error executing local query: " + e.getMessage(), e);
			throw e;
		} finally {
			qexec.close();
		}
	}

	/**
	 * Execute a jena query against the data.
	 *
	 * @param query
	 *            The query to execute.
	 * @return The list of QuerySolutions.
	 */
	public List<QuerySolution> executeQuery(final Query query) {
		QueryExecution qexec = null;
		if (isService()) {
			qexec = QueryExecutionFactory.sparqlService(getSparqlEndpoint(),
					query);
		} else {
			qexec = QueryExecutionFactory.create(query, localModel);
		}
		try {
			return WrappedIterator.create(qexec.execSelect()).toList();
		} finally {
			qexec.close();
		}
	}

	/**
	 * Execute a query against the data.
	 *
	 * @param queryStr
	 *            The query as a string.
	 * @return The list of QuerySolutions.
	 */
	public List<QuerySolution> executeQuery(final String queryStr) {
		return executeQuery(QueryFactory.create(queryStr));
	}

	@Override
	public NameFilter<Schema> findSchemas(final String schemaNamePattern) {
		return new NameFilter<Schema>(schemaNamePattern, readSchemas());
	}

	public Set<Schema> fixupSchemas(final Set<Schema> schemas) {
		final Set<Schema> schemaList = new HashSet<Schema>();
		for (final Schema schema : schemas) {
			schemaList.add(RdfSchema.Builder.fixupCatalog(this,
					(RdfSchema) schema));
		}
		this.schemaList = schemaList;
		return schemaList;
	}

	public Model getLocalModel() {
		return localModel;
	}

	@Override
	public CatalogName getName() {
		return new CatalogName(getShortName());
	}

	@Override
	@Predicate(impl = true)
	public Resource getResource() {
		throw new EntityManagerRequiredException();
	}

	@Override
	public Schema getSchema(final String schemaName) {
		final NameFilter<Schema> nf = findSchemas(StringUtils
				.defaultString(schemaName));
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	@Predicate(impl = true, type = RdfSchema.class, postExec = "fixupSchemas")
	public Set<Schema> getSchemas() {
		throw new EntityManagerRequiredException();
	}

	public Node getServiceNode() {
		return isService() ? NodeFactory.createURI(getSparqlEndpoint()) : null;
	}

	@Predicate(impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label")
	public String getShortName() {
		throw new EntityManagerRequiredException();
	}

	@Predicate(impl = true, emptyIsNull = true)
	public String getSparqlEndpoint() {
		throw new EntityManagerRequiredException();
	}

	// /**
	// * Create a sparql schema that has an empty namespace.
	// *
	// * @return The Schema.
	// */
	// public Schema getViewSchema()
	// {
	// Schema retval = getSchema("");
	// if (retval == null)
	// {
	// retval = new RdfSchema.Builder().setName("").setCatalog(this)
	// .build(this.getResource().getModel());
	// }
	// return retval;
	// }

	public boolean isService() {
		return getSparqlEndpoint() != null;
	}

	private Set<Schema> readSchemas() {
		if (schemaList == null) {
			schemaList = getSchemas();
		}
		return schemaList;
	}

}
