package org.xenei.jdbc4sparql.iface;

import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface QExecutor {
    public static Logger LOG = LoggerFactory.getLogger( QExecutor.class );
    /**
     * Prepare a query execution.
     * @param query the query to prepare
     * @return The query execution against the associated entity manager.
     */
    public QueryExecution execute( Query query );

    public static List<QuerySolution> asList(QueryExecution qexec) {
        try {
            final ResultSet rs = qexec.execSelect();
            final List<QuerySolution> retval = WrappedIterator.create( rs ).toList();
            return retval;
        } catch (final Exception e) {
            LOG.error( "Error executing local query: " + e.getMessage(), e );
            throw e;
        } finally {
            qexec.close();
        }
    }
    
    public static QueryExecution execute( QExecutor qExec, String queryStr)
    {
        return qExec.execute(  QueryFactory.create(queryStr) );
    }
}
