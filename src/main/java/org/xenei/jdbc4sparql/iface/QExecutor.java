package org.xenei.jdbc4sparql.iface;

import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface QExecutor extends Transactional{
    public static Logger LOG = LoggerFactory.getLogger( QExecutor.class );
    /**
     * Prepare a query execution.
     * @param query the query to prepare
     * @return The query execution against the associated entity manager.
     */
    public QueryExecution execute( Query query );

    public static List<QuerySolution> asList(QExecutor qExec, QueryExecution qe) {
        try {
            qExec.begin( ReadWrite.READ );
            final ResultSet rs = qe.execSelect();
            final List<QuerySolution> retval = WrappedIterator.create( rs ).toList();            
            return retval;
        } catch (final Exception e) {
            LOG.error( "Error executing local query: " + e.getMessage(), e );
            throw e;
        } finally {
            qe.close();
            qExec.end();
        }
    }
    
    public static QueryExecution execute( QExecutor qExec, String queryStr)
    {
        return qExec.execute(  QueryFactory.create(queryStr) );
    }
}
