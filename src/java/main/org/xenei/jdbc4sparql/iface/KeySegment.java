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

import java.util.Comparator;

import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject( namespace = "http://org.xenei.jdbc4sparql/entity/KeySegment#" )
public abstract class KeySegment implements Comparator<Object[]>,
		ResourceWrapper
{

	@Override
	public final int compare( final Object[] data1, final Object[] data2 )
	{
		final int idx = getIdx();
		final Object o1 = data1[idx];
		final Object o2 = data2[idx];
		int retval;
		if (o1 == null)
		{
			retval = o2 == null ? 0 : -1;
		}
		else if (o2 == null)
		{
			retval = 1;
		}
		else
		{
			retval = Comparable.class.cast(data1[idx]).compareTo(data2[idx]);
		}
		return isAscending() ? retval : -1 * retval;
	}

	public final String getId()
	{
		return (isAscending() ? "A" : "D") + getIdx();
	}

	@Predicate
	abstract public short getIdx();

	@Predicate
	abstract public boolean isAscending();
}
