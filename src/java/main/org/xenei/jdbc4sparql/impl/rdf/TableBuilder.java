package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;

public class TableBuilder implements Table
{
	private TableDef tableDef;
	private String name;
	private Schema schema;
	private ColumnBuilder[] columns;
	private String type;
	private Class<? extends RdfTable> typeClass = RdfTable.class;
	private Class<? extends RdfColumn> colTypeClass;
	
	public TableBuilder()
	{
	}
	
	protected TableBuilder( Class<? extends RdfTable> typeClass, Class<? extends RdfColumn> colTypeClass)
	{
		this.typeClass = typeClass;
		this.colTypeClass = colTypeClass;
	}

	public Table build( final Model model )
	{
		checkBuildState();
		final String fqName = getFQName();
		final ResourceBuilder builder = new ResourceBuilder(model);

		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();

		Resource table = null;
		if (builder.hasResource(fqName))
		{
			table = builder.getResource(fqName, typeClass);
		}
		else
		{

			table = builder.getResource(fqName, typeClass);

			table.addLiteral(RDFS.label, name);

			table.addProperty(builder.getProperty(typeClass, "tableDef"),
					tableDef.getResource());

			table.addProperty(builder.getProperty(typeClass, "schema"),
					schema.getResource());

			table.addLiteral(builder.getProperty(typeClass, "type"), type);

			for (final ColumnBuilder cBldr : columns)
			{
				cBldr.setTable(this);
				final Column col = cBldr.build(model);
				table.addProperty(builder.getProperty(typeClass, "column"),
						col.getResource());
			}

			schema.getResource().addProperty(
					builder.getProperty(RdfSchema.class, "tables"), table);
		}

		try
		{
			return entityManager.read(table, typeClass);
		}
		catch (final MissingAnnotation e)
		{
			throw new RuntimeException(e);
		}
	}

	private void checkBuildState()
	{
		if (StringUtils.isBlank(name))
		{
			throw new IllegalStateException("Name must be set");
		}

		if (StringUtils.isBlank(type))
		{
			throw new IllegalStateException("Type must be set");
		}

		if (schema == null)
		{
			throw new IllegalStateException("schema must be set");
		}

		for (int i = 0; i < columns.length; i++)
		{
			if (columns[i] == null)
			{
				throw new IllegalStateException(String.format(
						"column %s must be set", i));
			}
		}
	}

	@Override
	public void delete()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public NameFilter<Column> findColumns( final String columnNamePattern )
	{
		return new NameFilter<Column>(columnNamePattern, getColumns());
	}

	@Override
	public Catalog getCatalog()
	{
		return getSchema().getCatalog();
	}

	@Override
	public Column getColumn( final int idx )
	{
		return columns[idx];
	}

	@Override
	public Column getColumn( final String name )
	{
		for (final Column c : columns)
		{
			if (c.getName().equals(name))
			{
				return c;
			}
		}
		return null;
	}

	@Override
	public int getColumnCount()
	{
		return tableDef.getColumnCount();
	}

	@Override
	public int getColumnIndex( final Column column )
	{
		for (int i = 0; i < columns.length; i++)
		{
			if (columns[i].getResource().equals(column.getResource()))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public int getColumnIndex( final String name )
	{
		for (int i = 0; i < columns.length; i++)
		{
			if (name.equals(columns[i].getName()))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public Iterator<? extends Column> getColumns()
	{
		return Arrays.asList(columns).iterator();
	}

	private String getFQName()
	{
		final StringBuilder sb = new StringBuilder()
				.append(schema.getResource().getURI()).append(" ").append(name);

		return String.format("%s/instance/N%s", ResourceBuilder
				.getFQName(RdfTable.class),
				UUID.nameUUIDFromBytes(sb.toString().getBytes()).toString());
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
	public Schema getSchema()
	{
		return schema;
	}

	@Override
	public String getSPARQLName()
	{
		return NameUtils.getSPARQLName(this);
	}

	@Override
	public String getSQLName()
	{
		return NameUtils.getDBName(this);
	}

	@Override
	public Table getSuperTable()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public TableDef getTableDef()
	{
		return tableDef;
	}

	@Override
	public String getType()
	{
		return type;
	}

	public TableBuilder setColumn( final int idx, final String name )
	{
		if (tableDef == null)
		{
			throw new IllegalStateException(
					"TableDef must be specified before defining columns");
		}

		if ((idx < 0) || (idx >= columns.length))
		{
			throw new IllegalArgumentException(String.format(
					"index '%s' must be between 0 and %s inclusive", idx,
					columns.length - 1));
		}
		final ColumnBuilder builder = new ColumnBuilder()
				.setColumnDef(tableDef.getColumnDef(idx)).setName(name)
				.setTable(this);

		columns[idx] = builder;
		return this;
	}

	public TableBuilder setName( final String name )
	{
		this.name = name;
		return this;
	}

	public TableBuilder setSchema( final Schema schema )
	{
		this.schema = schema;
		return this;
	}

	public TableBuilder setTableDef( final TableDef tableDef )
	{
		this.tableDef = tableDef;
		this.columns = new ColumnBuilder[tableDef.getColumnCount()];
		return this;
	}

	public TableBuilder setType( final String type )
	{
		this.type = type;
		return this;
	}

}
