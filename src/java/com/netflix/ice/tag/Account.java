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
package com.netflix.ice.tag;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

public class Account extends Tag {
	private static final long serialVersionUID = 1L;
	
	// Account ID goes into the parent name since it's immutable. Hide the value so it can't be accessed directly
	// All other values associated with the account can be modified in the AWS Organizations service,
	// so we can't make them final.
	private String iceName; // Name assigned to account for display in the dashboards
	private String awsName; // Name of account returned by Organizations
	private String email; // Email address of the account
	private List<String> parents; // parent organizational units as defined by the Organizations service
	private String status;  // status as returned by the Organizations service
	private List<String> accessGroups; // names of groups account is assigned to for controlling access
	private Map<String, String> tags;

    public Account(String accountId, String accountName, List<String> parents) {
        super(accountId);
        this.iceName = accountName;
        this.awsName = null;
        this.email = null;
        this.parents = parents;
        this.status = null;
        this.accessGroups = null;
        this.tags = null;
    }
    
    public Account(String accountId, String accountName, String awsName, String email, List<String> parents, String status, List<String> accessGroups, Map<String, String> tags) {
    	super(accountId);
        this.iceName = StringUtils.isEmpty(accountName) ? awsName : accountName;
        this.awsName = awsName;
        this.email = email;
        this.parents = parents;
        this.status = status;
        this.accessGroups = accessGroups;
        this.tags = tags;
    }
    
    public void update(Account a) {
		this.iceName = a.iceName;
		this.awsName = a.awsName;
		this.email = a.email;
		this.parents = a.parents;
		this.status = a.status;
		this.accessGroups = a.accessGroups;
		this.tags = a.tags;
    }
    
    @Override
    public String getName() {
        return this.iceName;
    }

    public String getId() {
    	return super.name;
    }

	public String getIceName() {
		return iceName;
	}

	public String getAwsName() {
		return awsName;
	}

	public String getEmail() {
		return email;
	}

	public List<String> getParents() {
		return parents;
	}

	public String getStatus() {
		return status;
	}
	
	public List<String> getAccessGroups() {
		return accessGroups;
	}

	public Map<String, String> getTags() {
		return tags;
	}
	
	public static String[] header() {
		return new String[] {"ICE Name", "AWS Name", "ID", "Email", "Organization Path", "Access Groups", "Status", "Tags"};
	}
	
	public String[] values() {
		List<String> tagSet = Lists.newArrayList();
		if (tags != null) {
			for (Entry<String, String> e: tags.entrySet()) {
				tagSet.add(String.join("=", e.getKey(), e.getValue()));
			}
		}
		return new String[]{
			getIceName(),
			getAwsName(),
			getId(),
			getEmail(),
			String.join("/", parents),
			accessGroups == null || accessGroups.size() == 0 ? "" : String.join("/", accessGroups),
			getStatus(),
			String.join(",", tagSet)
		};
	}
	
	public static String[] headerWithoutTags() {
		return new String[] {"ICE Name", "AWS Name", "ID", "Email", "Organization Path", "Access Groups", "Status"};
	}
	
	public List<String> values(Collection<String> tagKeys) {
		List<String> values = Lists.newArrayList();
		values.add(getIceName());
		values.add(getAwsName());
		values.add(getId());
		values.add(getEmail());
		values.add(String.join("/", parents));
		values.add(accessGroups == null || accessGroups.size() == 0 ? "" : String.join("/", accessGroups));
		values.add(getStatus());
		for (String key: tagKeys) {
			String v = tags.get(key);
			values.add(v == null ? "" : v);
		}
		return values;
	}

}
