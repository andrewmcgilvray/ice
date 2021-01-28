/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice

import grails.converters.JSON
import com.netflix.ice.common.TagConfig
import com.netflix.ice.common.TagMappingTerm
import com.netflix.ice.common.TagMappings
import com.netflix.ice.tag.Account
import com.netflix.ice.tag.Product
import com.netflix.ice.tag.Tag
import com.netflix.ice.tag.UserTagKey

class JSONConverter {

    static void register() {
        JSON.registerObjectMarshaller(Tag) { Tag it ->
            return [name: it.name]
        }
		
		JSON.registerObjectMarshaller(Account) { Account it ->
			return [name: it.getIceName(), awsName: it.getAwsName(), id: it.getId(), email: it.getEmail(), parents: it.getParents(), status: it.getStatus(),
				joinedMethod: it.getJoinedMethod(), joinedDate: it.getJoinedDate(), unlinkedDate: it.getUnlinkedDate(), tags: it.getTags()]
		}
		
		JSON.registerObjectMarshaller(Product) { Product it ->
			return [name: it.getIceName()]
		}
		
 		JSON.registerObjectMarshaller(UserTagKey) { UserTagKey it ->
			return [name: it.name, aliases: it.aliases]
		}
		 
		JSON.registerObjectMarshaller(TagMappingTerm) { TagMappingTerm it ->
			return [key: it.key, operator: it.operator.toString(), values: it.values, terms: it.terms]
		}
		
		JSON.registerObjectMarshaller(TagMappings) { TagMappings it ->
			return [name: it.name, owners: it.owners, parent: it.parent, start: it.start, include: it.include, exclude: it.exclude, maps: it.maps]
		}
		
		JSON.registerObjectMarshaller(TagConfig) { TagConfig it ->
			return [name: it.name, values: it.values, aliases: it.aliases, displayAliases: it.displayAliases, mapped: it.mapped]
		}
   }
}
