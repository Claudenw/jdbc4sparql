package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;

public class QueryInfoSet
{

	// the list of tables in the query indexed by SQL name.
	private final Map<QueryItemName, QueryTableInfo> tablesInQuery;

	// the list of columns in the query indexed by SQL name.
	private final Map<QueryItemName, QueryColumnInfo> columnsInQuery;
	
	private static final Logger LOG = LoggerFactory.getLogger(QueryInfoSet.class);

	public QueryInfoSet()
	{
		this.tablesInQuery = new HashMap<QueryItemName, QueryTableInfo>();
		this.columnsInQuery = new HashMap<QueryItemName, QueryColumnInfo>();
	}

	public void addColumn( final QueryColumnInfo info )
	{
		LOG.debug( "adding column: {}", info);
		columnsInQuery.put(info.getName(), info);
	}

	public void addTable( final QueryTableInfo tbl )
	{
		LOG.debug( "adding table: {}", tbl);
		tablesInQuery.put(tbl.getName(), tbl);
	}

	public boolean containsColumn( final String columnName )
	{
		final QueryItemName name = QueryColumnInfo.getNameInstance(columnName);
		return name.findMatch(columnsInQuery) != null;
	}

	public QueryColumnInfo findColumn( final String schemaName,
			final String tableName, final String columnName )
			throws SQLException
	{
		final QueryItemName name = QueryColumnInfo.getNameInstance(schemaName,
				tableName, columnName);
		QueryColumnInfo retval = name.findMatch(columnsInQuery);
		if (retval == null)
		{
			final QueryItemName tblName = QueryTableInfo.getNameInstance(
					schemaName, tableName);

			final QueryTableInfo tableInfo = tblName.findMatch(tablesInQuery);
			if (tableInfo == null)
			{
				throw new SQLException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_QUERY, tblName));
			}
			retval = tableInfo.getColumn(columnName);
			if (retval == null)
			{
				throw new SQLException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_ANY_, columnName,
						"table"));
			}
		}
		return retval;
	}

	public QueryColumnInfo getColumnByName( final String name )
	{
		final QueryItemName itemName = QueryColumnInfo.getNameInstance(name);
		final QueryColumnInfo retval = itemName.findMatch(columnsInQuery);
		if (retval == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, name));
		}
		return retval;
	}

	public QueryColumnInfo getColumnByNode( final Node n )
	{
		return getItemByNode(columnsInQuery, n);
	}

	public Collection<RdfColumn> getColumns()
	{
		return WrappedIterator.create(columnsInQuery.values().iterator())
				.mapWith(new Map1<QueryColumnInfo, RdfColumn>() {

					@Override
					public RdfColumn map1( final QueryColumnInfo o )
					{
						return o.getColumn();
					}
				}).toList();
	}

	private <T extends QueryItemInfo> T getItemByNode(
			final Map<QueryItemName, T> map, final Node n )
	{
		for (final T entry : map.values())
		{
			if (entry.getVar().equals(n))
			{
				return entry;
			}
		}
		return null;
	}

	public QueryTableInfo getTable( final String alias )
	{
		return getTable( QueryTableInfo.getNameInstance(alias) );
	}
	
	public QueryTableInfo getTable( QueryTableInfo.Name name )
	{
		return name.findMatch( tablesInQuery );
	}

	public Set<String> getTableAliases()
	{
		return WrappedIterator.create(tablesInQuery.keySet().iterator())
				.mapWith(new Map1<QueryItemName, String>() {

					@Override
					public String map1( final QueryItemName o )
					{
						return o.toString();
					}
				}).toSet();
	}

	public QueryTableInfo getTableByNode( final Node n )
	{
		return getItemByNode(tablesInQuery, n);
	}

	public Set<String> getTableNames()
	{
		return WrappedIterator.create(tablesInQuery.values().iterator())
				.mapWith(new Map1<QueryTableInfo, String>() {

					@Override
					public String map1( final QueryTableInfo o )
					{
						return o.getSQLName();
					}
				}).toSet();
	}

	public Collection<QueryTableInfo> getTables()
	{
		return tablesInQuery.values();
	}

	public Collection<QueryTableInfo> listTables()
	{
		return tablesInQuery.values();
	}
}
