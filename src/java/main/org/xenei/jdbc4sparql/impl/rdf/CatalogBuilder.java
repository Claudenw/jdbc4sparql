package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;

public class CatalogBuilder implements Catalog
{

	private String name;
	private final Set<Schema> schemas = new HashSet<Schema>();

	public Catalog build( final Model model )
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
				catalog.addProperty(builder.getProperty(typeClass, "schema"),
						scm.getResource());
			}
		}
		try
		{
			final RdfCatalog retval = entityManager.read(catalog,
					RdfCatalog.class);
			model.register(retval.new ChangeListener());
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

	}

	@Override
	public void close()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public NameFilter<Schema> findSchemas( final String schemaNamePattern )
	{
		return new NameFilter<Schema>(schemaNamePattern, schemas);
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
	public Schema getSchema( final String schemaName )
	{
		final NameFilter<Schema> nf = findSchemas(schemaName);
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	public Set<Schema> getSchemas()
	{
		return schemas;
	}

	public CatalogBuilder setLocalName( final String name )
	{
		this.name = name;
		return this;
	}

	public CatalogBuilder setName( final String name )
	{
		this.name = name;
		return this;
	}

}
