package org.xenei.jdbc4sparql.impl.rdf;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.arq.querybuilder.AskBuilder;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.PlanOp;
import org.apache.jena.sparql.engine.QueryExecutionBase;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.QExecutor;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.name.CatalogName;
import org.xenei.jdbc4sparql.impl.AbstractDatasetProducer;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.SubjectInfo;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;
import org.xenei.jena.entities.impl.EntityManagerImpl;



@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/Catalog#")
public class RdfCatalog implements Catalog, ResourceWrapper {
	
	public static class Builder implements Catalog {
		
		/**
		 * Create the fully qualified name for the catalog.
		 * @param entityManager The entity manager to resolve the catalog name against.
		 * @param shortName the short name for the catalog.
		 * @return the fully qualified catalog name.
		 */
		public static String getFQName(EntityManager entityManager, final String shortName) {
			return String.format("%s/instance/N%s",
					ResourceBuilder.getFQName(entityManager, RdfCatalog.class), shortName);
		}

		private RDFConnection connection;
		private URL sparqlEndpoint;
		private String shortName;
		private String writeGraph;
		private String readGraph;
		
		public Builder() {
		}

		public Builder(final RdfCatalog catalog) throws MalformedURLException {
			this();
			setName(catalog.getShortName());
			setWriteGraph( catalog.getWriteGraph().getURI() );
			if (catalog.getSparqlEndpoint() != null) {
				setSparqlEndpoint(new URL(catalog.getSparqlEndpoint()));
			}
			if (catalog.connection != null) {
				setLocalConnection(catalog.connection);
			}
		}

		public RdfCatalog build(EntityManager entityManager) {		           
            
			if (entityManager == null) {
				throw new IllegalArgumentException("EntityManager may not be null");
			}
			
			checkBuildState();
			writeGraph = writeGraph==null?Quad.defaultGraphIRI.getURI():writeGraph;
			readGraph = readGraph==null?writeGraph:readGraph;
			
			final Class<?> typeClass = RdfCatalog.class;
						
			final String fqName = getFQName(entityManager);
			
			final ResourceBuilder builder = new ResourceBuilder( entityManager);
			
			// create catalog graph resource
			Resource catalog = null;
			if (builder.hasResource(fqName)) {
				catalog = builder.getResource(fqName, typeClass);
			}
			else {
				catalog = builder.getResource(fqName, typeClass);

				
				if (sparqlEndpoint != null) {
					catalog.addProperty(
							builder.getProperty(typeClass, "sparqlEndpoint"),
							sparqlEndpoint.toExternalForm());
				}

			}

			// create RdfCatalog object from graph object
			try {
				final RdfCatalog retval = entityManager.read(catalog, RdfCatalog.class);
				
	            catalog.removeAll( RDFS.label);
				catalog.addLiteral(RDFS.label, shortName);
				catalog.removeAll( VOID.rootResource);
				String graphName = AbstractDatasetProducer.getModelURI(entityManager, writeGraph);
				catalog.addProperty( VOID.rootResource, ResourceFactory.createResource(graphName));
				
				Property p = entityManager.getSubjectInfo(RdfCatalog.class).getPredicateProperty("getReadGraph");
				catalog.removeAll( p );
				graphName = AbstractDatasetProducer.getModelURI(entityManager, readGraph);
				catalog.addProperty( p, ResourceFactory.createResource( graphName ) );
				
				ChangeListener cl = retval.new ChangeListener();	
				retval.getResource().getModel().register( cl );
				
				retval.connection = connection;
				if (connection== null) {
					if (sparqlEndpoint != null)
					{
						retval.connection = RDFConnectionFactory.connect(sparqlEndpoint.toExternalForm());
					} else {
						retval.connection = RDFConnectionFactory.connect(DatasetFactory.create());
					}
				}
				
				return retval;
			} catch (final MissingAnnotation e) {
				RdfCatalog.LOG.error(
						String.format("Error building %s: %s", shortName,
								e.getMessage()), e);
				throw new RuntimeException(e);
			}						

		}

		protected void checkBuildState() {
			if (shortName == null) {
				throw new IllegalStateException("Name must be set");
			}
			if ((connection == null) && (sparqlEndpoint == null)) {
				connection = RDFConnectionFactory.connect(DatasetFactory.create());
			}

		}

		@Override
		public void close() {
			throw new UnsupportedOperationException();
		}

		@Override
		public QExecutor getLocalExecutor() {
			throw new UnsupportedOperationException();
		}

		@Override
		public NameFilter<Schema> findSchemas(final String schemaNamePattern) {
			return new NameFilter<Schema>(schemaNamePattern, getSchemas());
		}

		private String getFQName(EntityManager mgr) {
			return Builder.getFQName(mgr, shortName);
		}

		public RDFConnection getConnection() {
			return connection;
		}

		@Override
		public CatalogName getName() {
			return new CatalogName(shortName);
		}

		@Override
		public Schema getSchema(final String schemaName) {
			final NameFilter<Schema> nf = findSchemas(schemaName);
			return nf.hasNext() ? nf.next() : null;
		}

		@Override
		public Set<Schema> getSchemas() {
			return Collections.emptySet();
		}

		public Builder setLocalConnection(final RDFConnection connection) {
			this.connection = connection;
			return this;
		}
				
		public Builder setWriteGraph(final String graphName) {
			this.writeGraph = graphName;
			return this;
		}

		public Builder setReadGraph(final String graphName) {
		    this.readGraph = graphName;
			return this;
		}
		public Builder setName(final String name) {
			this.shortName = StringUtils.defaultString(name);
			return this;
		}

		public Builder setSparqlEndpoint(final URL sparqlEndpoint) {
			this.sparqlEndpoint = sparqlEndpoint;
			return this;
		}

		@Override
		public String getShortName() {
			return getName().getCatalog();
		}

	}

	public class ChangeListener extends
	AbstractChangeListener<Catalog, RdfSchema> {

		public ChangeListener() {
			super(RdfCatalog.this, RdfCatalog.class, "schemas",
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
	private RDFConnection connection;
	//private Node graphName;
	private Set<Schema> schemaList;

	private static Logger LOG = LoggerFactory.getLogger(RdfCatalog.class);

	@Override
	public void close() {
		if (connection != null)
		{
			connection.close();
		}
		connection = null;
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
	public QExecutor getLocalExecutor() {
		
	    return new QExecutor() {

            @Override
            public QueryExecution execute(Query query) {
                Resource graphName = getReadGraph();
                if ( ! graphName.asNode().equals( Quad.defaultGraphIRI))
                {
                    ElementNamedGraph ge = new ElementNamedGraph( graphName.asNode(), query.getQueryPattern() );
                    query.setQueryPattern(ge);
                }
                    
                return connection.query(query);
            }
	        
	    };
		
		
	}

	/**
	 * @return The QExecutor against the data.
	 */
	public QExecutor getExecutor() {
	    return new QExecutor() {

            @Override
            public QueryExecution execute(Query query) {
                return connection.query(query);
            }};

	}

	@Override
	public NameFilter<Schema> findSchemas(final String schemaNamePattern) {
		return new NameFilter<Schema>(schemaNamePattern, readSchemas());
	}

	public Set<Schema> fixupSchemas(final Set<Schema> schemas) {
		final Set<Schema> schemaList = new HashSet<Schema>();
		for (final Schema schema : schemas) {
			schemaList.add(RdfSchema.Builder.fixupCatalog(getEntityManager(), this,
					(RdfSchema) schema));
		}
		this.schemaList = schemaList;
		return schemaList;
	}

	public RDFConnection getLocalConnection() {
		return connection;
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
	@Predicate(impl = true)
	public EntityManager getEntityManager() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public SubjectInfo getSubjectInfo() {
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

	@Override
	@Predicate(impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label")
	public String getShortName() {
		throw new EntityManagerRequiredException();
	}

	@Predicate(impl = true, emptyIsNull = true)
	public String getSparqlEndpoint() {
		throw new EntityManagerRequiredException();
	}


	private Set<Schema> readSchemas() {
		if (schemaList == null) {
			schemaList = getSchemas();
		}
		return schemaList;
	}

	@Override
	public String toString() {
		return getName().toString();
	}

	@Predicate( impl=true, namespace = VOID.NS, name = "rootResource" )
	public Resource getWriteGraph() {
		throw new EntityManagerRequiredException();
	}
	
	@Predicate( impl=true )
	public Resource getReadGraph() {
		throw new EntityManagerRequiredException();
	}
	
	public Resource getResource( String uri )
	{		
		Resource graphName = getWriteGraph();
		Node n = NodeFactory.createURI(uri);
		ConstructBuilder sb = new ConstructBuilder().addConstruct( n, "?p", "?o");
		if (graphName != null) {
				sb.addGraph( graphName, new ConstructBuilder().addWhere( n, "?p", "?o") );
		} else {
			sb.addWhere( n, "?p", "?o");
		}
		Model m = getLocalConnection().queryConstruct(sb.build());
		return m.createResource(uri);
	}
}
