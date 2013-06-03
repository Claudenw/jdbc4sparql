/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.iface;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.xenei.jdbc4sparql.impl.rdf.ResourceBuilder;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/Key#" )
public abstract class Key implements Comparator<Object[]>, ResourceWrapper
{

	private List<KeySegment> segments;

	@Override
	public final int compare( final Object[] data1, final Object[] data2 )
	{
		for (final KeySegment segment : getSegments())
		{
			final int retval = segment.compare(data1, data2);
			if (retval != 0)
			{
				return retval;
			}
		}
		return 0;
	}

	public final String getId()
	{
		final StringBuilder sb = new StringBuilder().append(isUnique());
		for (final KeySegment ks : getSegments())
		{
			sb.append(ks.getId());
		}
		return UUID.nameUUIDFromBytes(sb.toString().getBytes()).toString();
	}

	/**
	 * Get the key name (may be null)
	 * 
	 * @return the key name.
	 */
	@Predicate
	abstract public String getKeyName();

	public List<KeySegment> getSegments()
	{
		if (segments == null)
		{
			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			segments = new ArrayList<KeySegment>();
			final Resource resource = getResource();
			final Property p = resource.getModel().createProperty(
					ResourceBuilder.getFQName(KeySegment.class));
			final List<RDFNode> resLst = resource.getRequiredProperty(p)
					.getResource().as(RDFList.class).asJavaList();
			for (final RDFNode n : resLst)
			{
				try
				{
					segments.add(entityManager.read(n.asResource(),
							KeySegment.class));
				}
				catch (MissingAnnotation e)
				{
					throw new RuntimeException( e );
				}
			}
		}
		return segments;
	}

	@Predicate
	abstract public boolean isUnique();
}
