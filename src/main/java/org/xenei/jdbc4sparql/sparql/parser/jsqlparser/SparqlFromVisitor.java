/*
 * This file is part of jdbc4sparql jsqlparser implementation.
 * 
 * jdbc4sparql jsqlparser implementation is free software: you can redistribute
 * it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * jdbc4sparql jsqlparser implementation is distributed in the hope that it will
 * be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with jdbc4sparql jsqlparser implementation. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.sql.SQLException;
import java.util.Map;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.sparql.QueryItemName;
import org.xenei.jdbc4sparql.sparql.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

class SparqlFromVisitor implements FromItemVisitor
{
	public static final boolean OPTIONAL = true;
	public static final boolean REQUIRED = false;

	private final SparqlQueryBuilder builder;
	private final boolean optional;
	private QueryItemName name;
	private static Logger LOG = LoggerFactory
			.getLogger(SparqlFromVisitor.class);

	SparqlFromVisitor( final SparqlQueryBuilder builder )
	{
		this(builder, SparqlFromVisitor.REQUIRED);
	}

	SparqlFromVisitor( final SparqlQueryBuilder builder, final boolean optional )
	{
		this.builder = builder;
		this.optional = optional;
	}

	@Override
	public void visit( final SubJoin subjoin )
	{
		throw new UnsupportedOperationException("FROM subjoin is not supported");
	}

	@Override
	public void visit( final SubSelect subSelect )
	{
		throw new UnsupportedOperationException(
				"FROM subselect is not supported");
	}

	@Override
	public void visit( final Table tableName )
	{
		SparqlFromVisitor.LOG.debug("visit table: {}", tableName);
		if (tableName.getAlias() != null )
		{
			name = QueryTableInfo.getNameInstance( tableName.getAlias() );
		}
		else
		{
			name = QueryTableInfo.getNameInstance( tableName.getSchemaName(), tableName.getName() );
		}
		try
		{
			// FIXME handle aliases here
			builder.addTable(tableName.getSchemaName(), tableName.getName(),
					tableName.getAlias(), optional);
		}
		catch (final SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public QueryItemName getName()
	{
		return name;
	}
}