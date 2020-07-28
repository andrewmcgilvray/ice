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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Account extends Tag {
	private static final long serialVersionUID = 1L;

	// Configuration separator values used for account default settings
    private static final String defaultTagSeparator = "/";
    private static final String defaultTagEffectiveDateSeparator = "=";

	// Account ID goes into the parent name since it's immutable. Hide the value so it can't be accessed directly
	// All other values associated with the account can be modified in the AWS Organizations service,
	// so we can't make them final.
	private String iceName; // Name assigned to account for display in the dashboards
	private String awsName; // Name of account returned by Organizations
	private String email; // Email address of the account
	private List<String> parents; // parent organizational units as defined by the Organizations service
	private String status;  // status as returned by the Organizations service
	private String joinedMethod;
	private String joinedDate;
	private Map<String, String> tags;
	private Map<String, DefaultTag> defaultTags;

    public Account(String accountId, String accountName, List<String> parents) {
        super(accountId);
        this.iceName = accountName;
        this.awsName = null;
        this.email = null;
        this.parents = parents;
        this.status = null;
        this.joinedMethod = null;
        this.joinedDate = null;
        this.tags = null;
        this.defaultTags = null;
    }
    
    public Account(String accountId, String accountName, String awsName, String email, List<String> parents, String status, String joinedMethod, String joinedDate, Map<String, String> tags) {
    	super(accountId);
        this.iceName = StringUtils.isEmpty(accountName) ? awsName : accountName;
        this.awsName = awsName;
        this.email = email;
        this.parents = parents;
        this.status = status;
        this.joinedMethod = joinedMethod;
        this.joinedDate = joinedDate;
        this.tags = tags;
		initDefaultTags();
    }
    
    public void update(Account a) {
		this.iceName = a.iceName;
		this.awsName = a.awsName;
		this.email = a.email;
		this.parents = a.parents;
		this.status = a.status;
        this.joinedMethod = a.joinedMethod;
        this.joinedDate = a.joinedDate;
		this.tags = a.tags;
		initDefaultTags();
    }
    
    public void initDefaultTags() {
    	defaultTags = getDefaultTags(tags);
    }
    
    private Map<String, DefaultTag> getDefaultTags(Map<String, String> tags) {
    	Map<String, DefaultTag> dt = Maps.newHashMap();
    	if (tags != null) {
        	for (String key: tags.keySet())
        		dt.put(key, new DefaultTag(tags.get(key)));
    	}
    	return dt;
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
	
	public String getJoinedMethod() {
		return joinedMethod;
	}
	
	public String getJoinedDate() {
		return joinedDate;
	}
	
	public Map<String, String> getTags() {
		return tags;
	}
	
	public static String[] header() {
		return new String[] {"ID", "ICE Name", "AWS Name", "Email", "Organization Path", "Status", "Joined Method", "Joined Date", "Tags"};
	}
	
	public String[] values() {
		List<String> tagSet = Lists.newArrayList();
		if (tags != null) {
			for (Entry<String, String> e: tags.entrySet()) {
				tagSet.add(String.join("=", e.getKey(), e.getValue()));
			}
		}
		return new String[]{
			getId(),
			getIceName(),
			getAwsName(),
			getEmail(),
			String.join("/", parents),
			getStatus(),
			joinedMethod,
			joinedDate,
			String.join(",", tagSet)
		};
	}
	
	public static String[] headerWithoutTags() {
		return new String[] {"ID", "ICE Name", "AWS Name", "Email", "Organization Path", "Status", "Joined Method", "Joined Date"};
	}
	
	public List<String> values(Collection<String> tagKeys, boolean onlyEffective) {
		List<String> values = Lists.newArrayList();
		values.add(getId());
		values.add(getIceName());
		values.add(getAwsName());
		values.add(getEmail());
		values.add(String.join("/", parents));
		values.add(getStatus());
		values.add(joinedMethod);
		values.add(joinedDate);
		if (onlyEffective) {
			long now = DateTime.now().getMillis();
			for (String key: tagKeys) {
				values.add(getDefaultUserTagValue(key, now));
			}
		}
		else {
			for (String key: tagKeys) {
				String v = tags.get(key);
				values.add(v == null ? "" : v);
			}
		}
		return values;
	}

    public String getDefaultUserTagValue(String tagKey, long startMillis) {
    	// return the default user tag value if there is one.
    	Account.DefaultTag dt = defaultTags == null ? null : defaultTags.get(tagKey);
    	return dt == null ? null : dt.getValue(startMillis);
    }

    // Default user tag values for the account. These are returned if the requested resource doesn't
    // have a tag value nor a mapped value. Map key is the tag key name.
    private class DefaultTag {
    	private class DateValue {
    		public long startMillis;
    		public String value;
    		
    		DateValue(long startMillis, String value) {
    			this.startMillis = startMillis;
    			this.value = value;
    		}
    	}
    	private List<DateValue> timeOrderedValues;
    	
    	DefaultTag(String config) {
    		Map<Long, String> sortedMap = Maps.newTreeMap();
    		String[] dateValues = config.split(defaultTagSeparator);
    		for (String dv: dateValues) {
    			String[] parts = dv.split(defaultTagEffectiveDateSeparator);
    			if (dv.contains(defaultTagEffectiveDateSeparator)) {
    				// If only one part, it's the start time and value should be empty
        			sortedMap.put(new DateTime(parts[0], DateTimeZone.UTC).getMillis(), parts.length < 2 ? "" : parts[1]);    				
    			}
    			else {
    				// If only one part, it's the value that starts at time 0
    				sortedMap.put(parts.length < 2 ? 0 : new DateTime(parts[0], DateTimeZone.UTC).getMillis(), parts[parts.length < 2 ? 0 : 1]);
    			}
    		}
    		timeOrderedValues = Lists.newArrayList();
    		for (Long start: sortedMap.keySet())
    			timeOrderedValues.add(new DateValue(start, sortedMap.get(start)));
    	}
    	
    	String getValue(long startMillis) {
    		String value = null;
    		for (DateValue dv: timeOrderedValues) {
    			if (dv.startMillis > startMillis)
    				break;
    			value = dv.value;
    		}
    		return value;
    	}
    }

}
