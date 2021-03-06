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

public interface NamespacedObject {
	public static class Utils {

		public static boolean equals(final NamespacedObject o1, final Object o2) {
			return (o2 instanceof NamespacedObject) ? o1.getFQName().equals(
					((NamespacedObject) o2).getFQName()) : false;
		}

		public static String getFQName(final NamespacedObject o) {
			return o.getNamespace() + o.getLocalName();
		}

		public static int hashCode(final NamespacedObject o) {
			return o.getFQName().hashCode();
		}

	}

	String getFQName();

	String getLocalName();

	String getNamespace();
}
