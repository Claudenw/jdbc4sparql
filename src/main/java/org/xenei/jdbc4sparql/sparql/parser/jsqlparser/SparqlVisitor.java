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

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public class SparqlVisitor implements StatementVisitor
{
	private final SparqlQueryBuilder sparqlQueryBuilder;
	private static Logger LOG = LoggerFactory.getLogger(SparqlVisitor.class);

	public SparqlVisitor( final RdfCatalog catalog, final RdfSchema schema )
	{
		sparqlQueryBuilder = new SparqlQueryBuilder(catalog, schema);
	}

	public SparqlQueryBuilder getBuilder()
	{
		return sparqlQueryBuilder;
	}

	@Override
	public void visit( final CreateTable createTable )
	{
		throw new UnsupportedOperationException("CREATE TABLE");
	}

	@Override
	public void visit( final Delete delete )
	{
		throw new UnsupportedOperationException("DELETE");
	}

	@Override
	public void visit( final Drop drop )
	{
		throw new UnsupportedOperationException("DROP");
	}

	@Override
	public void visit( final Insert insert )
	{
		throw new UnsupportedOperationException("INSERT");
	}

	@Override
	public void visit( final Replace replace )
	{
		throw new UnsupportedOperationException("REPLACE");
	}

	@Override
	public void visit( final Select select )
	{
		SparqlVisitor.LOG.debug("visit: {}", select);
		final SparqlSelectVisitor v = new SparqlSelectVisitor(
				sparqlQueryBuilder);
		select.getSelectBody().accept(v);
	}

	@Override
	public void visit( final Truncate truncate )
	{
		throw new UnsupportedOperationException("TRUNCATE");
	}

	@Override
	public void visit( final Update update )
	{
		throw new UnsupportedOperationException("UPDATE");
	}
}
