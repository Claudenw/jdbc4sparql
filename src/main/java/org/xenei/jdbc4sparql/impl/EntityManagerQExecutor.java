package org.xenei.jdbc4sparql.impl;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.xenei.jdbc4sparql.iface.QExecutor;
import org.xenei.jena.entities.EntityManager;

public class EntityManagerQExecutor extends ConnectionQExecutor {

    private final EntityManager entityManager;
    
    public EntityManagerQExecutor( EntityManager entityManager )
    {
        super( entityManager.getConnection() );
        this.entityManager = entityManager;
    }
    
    @Override
    public QueryExecution execute( Query query ) {
        return entityManager.execute( query );
    }
}
