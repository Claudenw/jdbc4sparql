package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;

public class TableDefBuilder implements TableDef
{

	private final List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
	private Key primaryKey;
	private Key sortKey;
	private TableDef superTable;
	private Class<? extends RdfTableDef> typeClass = RdfTableDef.class;
	private Class<? extends RdfColumnDef> colDefClass = RdfColumnDef.class;
	
	public TableDefBuilder()
	{
		
	}

	protected TableDefBuilder( Class<? extends RdfTableDef> typeClass, Class<? extends RdfColumnDef> colDefClass)
	{
		this.typeClass = typeClass;
		this.colDefClass = colDefClass;
	}
	
	public TableDefBuilder addColumnDef( final ColumnDef column )
	{
		columnDefs.add(column);
		return this;
	}

	public TableDef build( final Model model )
	{
		checkBuildState();
		
		final String fqName = getFQName();
		final ResourceBuilder builder = new ResourceBuilder(model);

		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();

		Resource tableDef = null;
		if (builder.hasResource(fqName))
		{
			tableDef = builder.getResource(fqName, typeClass);
		}
		else
		{

			tableDef = builder.getResource(fqName, typeClass);

			if (primaryKey != null)
			{

				tableDef.addProperty(
						builder.getProperty(typeClass, "primaryKey"),
						primaryKey.getResource());
			}

			if (sortKey != null)
			{
				tableDef.addProperty(builder.getProperty(typeClass, "sortKey"),
						sortKey.getResource());

			}

			if (superTable != null)
			{
				tableDef.addProperty(
						builder.getProperty(typeClass, "superTableDef"),
						superTable.getResource());

			}

			RDFList lst = null;

			for (final ColumnDef seg : columnDefs)
			{
				final Resource s = seg.getResource();
				if (lst == null)
				{
					lst = model.createList().with(s);
				}
				else
				{
					lst.add(s);
				}
			}
			final Property p = model.createProperty(ResourceBuilder
					.getFQName(colDefClass));
			tableDef.addProperty(p, lst);

		}
		try
		{
			RdfTableDef rdfTableDef = entityManager.read(tableDef, typeClass);
			rdfTableDef.setColDefClass(colDefClass);
			return rdfTableDef;
		}
		catch (final MissingAnnotation e)
		{
			throw new RuntimeException(e);
		}
	}

	protected void checkBuildState()
	{
		if (columnDefs.size() == 0)
		{
			throw new IllegalStateException(
					"There must be at least one column defined");
		}
	}

	@Override
	public int getColumnCount()
	{
		return columnDefs.size();
	}

	@Override
	public ColumnDef getColumnDef( final int idx )
	{
		return columnDefs.get(idx);
	}

	@Override
	public List<ColumnDef> getColumnDefs()
	{
		return columnDefs;
	}

	@Override
	public int getColumnIndex( final ColumnDef column )
	{
		return columnDefs.indexOf(column);
	}

	public String getFQName()
	{
		final StringBuilder sb = new StringBuilder();
		for (final ColumnDef cd : columnDefs)
		{
			sb.append(cd.getResource().getURI()).append(" ");
		}
		if (primaryKey != null)
		{
			sb.append(primaryKey.getId()).append(" ");
		}
		if (sortKey != null)
		{
			sb.append(sortKey.getId()).append(" ");
		}
		if (superTable != null)
		{
			sb.append(superTable.getResource().getURI());
		}

		return String.format("%s/instance/UUID-%s",
				ResourceBuilder.getFQName(RdfTableDef.class),
				UUID.nameUUIDFromBytes(sb.toString().getBytes()));

	}

	@Override
	public Key getPrimaryKey()
	{
		return primaryKey;
	}

	@Override
	public Resource getResource()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Key getSortKey()
	{
		return sortKey;
	}

	@Override
	public TableDef getSuperTableDef()
	{
		return superTable;
	}

	public TableDefBuilder setPrimaryKey( final Key key )
	{
		if (!key.isUnique())
		{
			throw new IllegalArgumentException(
					"primary key must be a unique key");
		}
		this.primaryKey = key;
		return this;
	}

	public TableDefBuilder setSortKey( final Key key )
	{
		this.sortKey = key;
		return this;
	}

	public TableDefBuilder setSuperTableDef( final TableDef tableDef )
	{
		this.superTable = tableDef;
		return this;
	}

}
