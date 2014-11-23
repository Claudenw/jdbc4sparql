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

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

class SparqlFromVisitor implements FromItemVisitor {

	private final SparqlQueryBuilder builder;
	private final boolean optional;
	private TableName name;
	private static Logger LOG = LoggerFactory
			.getLogger(SparqlFromVisitor.class);

	SparqlFromVisitor(final SparqlQueryBuilder builder) {
		this(builder, SparqlQueryBuilder.REQUIRED);
	}

	SparqlFromVisitor(final SparqlQueryBuilder builder, final boolean optional) {
		this.builder = builder;
		this.optional = optional;
	}

	public TableName getName() {
		return name;
	}

	@Override
	public void visit(final SubJoin subjoin) {
		throw new UnsupportedOperationException("FROM subjoin is not supported");
	}

	@Override
	public void visit(final SubSelect subSelect) {
		throw new UnsupportedOperationException(
				"FROM subselect is not supported");
	}

	@Override
	public void visit(final Table table) {
		SparqlFromVisitor.LOG.debug("visit table: {}", table);
		try {
			String defaultCatalog = builder.getCatalogName();
			String defaultSchema = builder.getDefaultSchemaName();
			final TableName tName = new TableName(defaultCatalog,
					StringUtils.defaultString(table.getSchemaName(),
							defaultSchema), table.getName());
			name = (table.getAlias() != null) ? TableName.getNameInstance(
					defaultCatalog, defaultSchema, table.getAlias()) : tName;
			builder.addTable(tName, name, optional);

		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}
}