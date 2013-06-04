package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.util.iterator.WrappedIterator;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.config.ModelReader;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Catalog#" )
public class RdfCatalog implements Catalog
{
	public static class Builder implements Catalog
	{
		private Model localModel;
		private URL sparqlEndpoint;
		private String name;
		private final Set<RdfSchema> schemas = new HashSet<RdfSchema>();

		public RdfCatalog build( final Model model )
		{
			checkBuildState();
			final Class<?> typeClass = RdfCatalog.class;
			final String fqName = getFQName();
			final ResourceBuilder builder = new ResourceBuilder(model);

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();

			Resource catalog = null;
			if (builder.hasResource(fqName))
			{
				catalog = builder.getResource(fqName, typeClass);
			}
			else
			{
				catalog = builder.getResource(fqName, typeClass);
				catalog.addLiteral(RDFS.label, name);

				for (final Schema scm : schemas)
				{
					catalog.addProperty(
							builder.getProperty(typeClass, "schema"),
							scm.getResource());
				}
			}
			try
			{
				final RdfCatalog retval = entityManager.read(catalog,
						RdfCatalog.class);
				model.register(retval.new ChangeListener());
				retval.localModel = localModel != null ? localModel
						: ModelFactory.createMemModelMaker().createFreshModel();
				;
				retval.sparqlEndpoint = sparqlEndpoint;
				new RdfSchema.Builder().setName(Catalog.DEFAULT_SCHEMA)
						.setCatalog(retval).build(model);
				return retval;
			}
			catch (final MissingAnnotation e)
			{
				throw new RuntimeException(e);
			}

		}

		protected void checkBuildState()
		{
			if (StringUtils.isBlank(name))
			{
				throw new IllegalStateException("Name must be set");
			}
			if ((localModel == null) && (sparqlEndpoint == null))
			{
				throw new IllegalStateException(
						"Either LocalModel or SPARQL endpoint must be set");
			}

		}

		@Override
		public void close()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public NameFilter<RdfSchema> findSchemas( final String schemaNamePattern )
		{
			return new NameFilter<RdfSchema>(schemaNamePattern, schemas);
		}

		private String getFQName()
		{
			return String.format("%s/instance/N%s",
					ResourceBuilder.getFQName(RdfCatalog.class), name);
		}

		@Override
		public String getName()
		{
			return name;
		}

		@Override
		@Predicate
		public Resource getResource()
		{
			return ResourceFactory.createResource(getFQName());
		}

		@Override
		public RdfSchema getSchema( final String schemaName )
		{
			final NameFilter<RdfSchema> nf = findSchemas(schemaName);
			return nf.hasNext() ? nf.next() : null;
		}

		@Override
		public Set<RdfSchema> getSchemas()
		{
			return schemas;
		}

		public Builder setLocalModel( final Model localModel )
		{
			this.localModel = localModel;
			return this;
		}

		public Builder setName( final String name )
		{
			this.name = name;
			return this;
		}

		public Builder setSparqlEndpoint( final URL sparqlEndpoint )
		{
			this.sparqlEndpoint = sparqlEndpoint;
			return this;
		}

	}

	public class ChangeListener extends
			AbstractChangeListener<Catalog, RdfSchema>
	{

		public ChangeListener()
		{
			super(RdfCatalog.this.getResource(), RdfCatalog.class, "schemas",
					RdfSchema.class);
		}

		@Override
		protected void addObject( final RdfSchema t )
		{
			schemaList.add(t);
		}

		@Override
		protected void clearObjects()
		{
			schemaList = null;
		}

		@Override
		protected boolean isListening()
		{
			return schemaList != null;
		}

		@Override
		protected void removeObject( final RdfSchema t )
		{
			schemaList.remove(t);
		}

	}

	// The URL for the sparql endpoint
	private URL sparqlEndpoint;
	// the model that contains the sparql data.
	private Model localModel;

	private Set<RdfSchema> schemaList;

	@Override
	public void close()
	{
		localModel.close();
	}

	/**
	 * Execute the query against the local Model.
	 * 
	 * This is used to execute queries built by the query builder.
	 * 
	 * @param query
	 * @return The list of QuerySolutions.
	 */
	public List<QuerySolution> executeLocalQuery( final Query query )
	{
		final QueryExecution qexec = QueryExecutionFactory.create(query,
				localModel);

		try
		{
			return WrappedIterator.create(qexec.execSelect()).toList();
		}
		finally
		{
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
	public List<QuerySolution> executeQuery( final Query query )
	{
		QueryExecution qexec = null;
		if (isService())
		{
			qexec = QueryExecutionFactory.sparqlService(
					sparqlEndpoint.toString(), query);
		}
		else
		{
			qexec = QueryExecutionFactory.create(query, localModel);
		}
		try
		{
			return WrappedIterator.create(qexec.execSelect()).toList();
		}
		finally
		{
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
	public List<QuerySolution> executeQuery( final String queryStr )
	{
		return executeQuery(QueryFactory.create(queryStr));
	}

	@Override
	public NameFilter<RdfSchema> findSchemas( final String schemaNamePattern )
	{
		return new NameFilter<RdfSchema>(schemaNamePattern, readSchemas());
	}

	public ModelReader getModelReader()
	{
		if (isService())
		{
			throw new IllegalStateException(
					"getModelReader() may not be called on a service catalog");
		}
		return new ModelReader() {
			@Override
			public Model getModel()
			{
				return localModel;
			}

			@Override
			public void read( final Model model )
			{
				if (localModel != model)
				{
					localModel.close();
					localModel = model;
				}
			}
		};
	}

	@Override
	@Predicate( impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label" )
	public String getName()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate( impl = true )
	public Resource getResource()
	{
		throw new EntityManagerRequiredException();
	}

	@Override
	public RdfSchema getSchema( final String schemaName )
	{
		final NameFilter<RdfSchema> nf = findSchemas(schemaName);
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	@Predicate( impl = true, type = RdfSchema.class )
	public Set<RdfSchema> getSchemas()
	{
		throw new EntityManagerRequiredException();
	}

	public Node getServiceNode()
	{
		return isService() ? Node.createURI(sparqlEndpoint.toExternalForm())
				: null;
	}

	/**
	 * Create a sparql schema that has an empty namespace.
	 * 
	 * @return The Schema.
	 */
	public RdfSchema getViewSchema()
	{
		final RdfSchema.Builder builder = new RdfSchema.Builder();
		builder.setCatalog(this).setName("");
		return builder.build(this.getResource().getModel());

	}

	public boolean isService()
	{
		return sparqlEndpoint != null;
	}

	private Set<RdfSchema> readSchemas()
	{
		if (schemaList == null)
		{
			schemaList = getSchemas();
		}
		return schemaList;
	}

}
