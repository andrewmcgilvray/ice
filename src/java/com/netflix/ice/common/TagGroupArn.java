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
package com.netflix.ice.common;

import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Operation;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Tag;
import com.netflix.ice.tag.UsageType;
import com.netflix.ice.tag.Zone;

public abstract class TagGroupArn<T extends Tag> extends TagGroup {
	private static final long serialVersionUID = 1L;
	
	public final T arn;
	
	private final int hashcode;

	protected TagGroupArn(Account account, Region region, Zone zone,
			Product product, Operation operation, UsageType usageType,
			ResourceGroup resourceGroup, T arn) {
		super(account, region, zone, product, operation, usageType,
				resourceGroup);
		this.arn = arn;
		this.hashcode = genHashCode();
	}

    @Override
    public String toString() {
        return super.toString() + ",\"" + arn + "\"";
    }

    @Override
    public String compareKey() {
    	return arn.name;
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o)
    		return true;
        if (o == null)
            return false;
        if (!super.equals(o))
        	return false;
        TagGroupArn<?> other = (TagGroupArn<?>)o;
        return this.arn == other.arn;
    }

    @Override
    public int hashCode() {
    	return hashcode;
    }
    
    private int genHashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((this.arn != null) ? this.arn.hashCode() : 0);
        return result;
    }
}
