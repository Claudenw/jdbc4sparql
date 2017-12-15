package org.xenei.jdbc4sparql.impl;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdfconnection.RDFConnection;
import org.xenei.jdbc4sparql.iface.QExecutor;

/**
 * A default implementation of a QExecutor that wraps an RDFConnection.
 *
 */
public class ConnectionQExecutor implements QExecutor {

    private final RDFConnection connection;

    /**
     * Constructor.
     * @param connection the connection to wrap.
     */
    public ConnectionQExecutor(RDFConnection connection) {
        this.connection = connection;
    }

    @Override
    public QueryExecution execute(Query query) {
        return connection.query( query );
    }

    @Override
    public void begin(ReadWrite readWrite) {
        connection.begin( readWrite );
    }

    @Override
    public void commit() {
        connection.commit();
    }

    @Override
    public void end() {
        connection.end();
    }

    @Override
    public void abort() {
        connection.abort();
    }

    @Override
    public boolean isInTransaction() {
        return connection.isInTransaction();
    }

}
