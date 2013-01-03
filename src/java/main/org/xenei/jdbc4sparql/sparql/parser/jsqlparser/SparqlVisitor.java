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

import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public class SparqlVisitor implements StatementVisitor
{
	private final SparqlQueryBuilder sparqlQueryBuilder;

	public SparqlVisitor( final SparqlCatalog catalog )
	{
		sparqlQueryBuilder = new SparqlQueryBuilder(catalog);
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
