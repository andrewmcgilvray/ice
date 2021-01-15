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
package com.netflix.ice.basic;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.common.LineItem;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.common.TagConfig;
import com.netflix.ice.common.TagMappings;
import com.netflix.ice.processor.TagMapper;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.ResourceGroup.ResourceException;
import com.netflix.ice.tag.UserTagKey;

public class BasicResourceService extends ResourceService {
    private final static Logger logger = LoggerFactory.getLogger(BasicResourceService.class);

    protected final List<String> customTags;
    private final List<UserTagKey> userTagKeys;
    private final boolean includeReservationIds;
    
    
    // Map of tags where each tag has a list of aliases. Outer key is the payerAccountId.
    private Map<String, Map<String, TagConfig>> tagConfigs;
    
    // Map of tag values to canonical name. All keys are lower case.
    // Maps are nested by Payer Account ID, Tag Key, then Value
    private Map<String, Map<String, Map<String, String>>> tagValuesInverted;
    
    // Map containing the lineItem column indeces that match the canonical tag keys specified by CustomTags
    // Key is the Custom Tag name (without the "user:" prefix). First index in the list is always the exact
    // custom tag name match if present.
    private Map<String, List<Integer>> tagLineItemIndeces;
    
    private final Map<String, Integer> tagResourceGroupIndeces;
    
    private static final String USER_TAG_PREFIX = "user:";
    private static final String AWS_TAG_PREFIX = "aws:";
    private static final String reservationIdsKeyName = "RI/SP ID";
    
    /**
     * Map keyed off payer account that holds the list of time-ordered tag mappers for each custom tag.
     * TagMappers are applied if active and either no tag value has yet been applied or the force flag
     * is set for the mapper rule.
     * 
     * Primary map key is the payer account ID,
     * List index is the tag key index,
     * Secondary map key is the start time for the mapper rule in milliseconds
     * .
     *  <pre>
     *  tagMappers:
     *    <payerAcctId1>:
     *    - <startMillis1>:
     *        - <tagMapperA>
     *        - <tagMapperB>
     *    - <startMillis2>:
     *        - <tagMapperC>
     *        ...
     *    <payerAcctId2>:
     *      ...
     *  </pre>
     */
    private Map<String, List<Map<Long, List<TagMapper>>>> tagMappers;
    
    public BasicResourceService(ProductService productService, String[] customTags, boolean includeReservationIds) {
		super();
		this.includeReservationIds = includeReservationIds;
		this.customTags = Lists.newArrayList(customTags);
		if (includeReservationIds)
			this.customTags.add(reservationIdsKeyName);
		this.tagValuesInverted = Maps.newHashMap();
		this.tagResourceGroupIndeces = Maps.newHashMap();
		for (int i = 0; i < customTags.length; i++)
			tagResourceGroupIndeces.put(customTags[i], i);
				
		userTagKeys = Lists.newArrayList();
		for (String tag: this.customTags) {
			if (!tag.isEmpty())
				userTagKeys.add(UserTagKey.get(tag));
		}
		
		this.tagConfigs = Maps.newHashMap();
		this.tagValuesInverted = Maps.newHashMap();
		this.tagMappers = Maps.newHashMap();
	}
    
    @Override
    public Map<String, Map<String, TagConfig>> getTagConfigs() {
    	return tagConfigs;
    }

    @Override
    public void setTagConfigs(String payerAccountId, List<TagConfig> tagConfigs) {    	
    	if (tagConfigs == null) {
    		// Remove existing configs and indeces
    		this.tagConfigs.remove(payerAccountId);
    		this.tagValuesInverted.remove(payerAccountId);
    		return;
    	}
    	
    	Map<String, TagConfig> configs = Maps.newHashMap();
    	for (TagConfig config: tagConfigs) {
    		if (!customTags.contains(config.name)) {
    			logger.warn("Ignoring configurations for tag \"" + config.name + "\" from payer account " + payerAccountId + ", not in customTags list.");
    			continue;
    		}
    		
    		configs.put(config.name, config);
    		
        	// Add any display aliases to the user tags
    		if (config.displayAliases != null) {
				UserTagKey tagKey = UserTagKey.get(config.name);
				tagKey.addAllAliases(config.displayAliases);
    		}
    	}
    	this.tagConfigs.put(payerAccountId, configs);
    	
    	
    	// Create inverted indexes for each of the tag value alias sets
		Map<String, Map<String, String>> indeces = Maps.newHashMap();
		for (TagConfig config: configs.values()) {
			if (config.values == null || config.values.isEmpty())
				continue;
			
			Map<String, String> invertedIndex = Maps.newConcurrentMap();
			for (Entry<String, List<String>> entry: config.values.entrySet()) {			
				for (String val: entry.getValue()) {
			    	// key is all lower case and strip out all whitespace
					invertedIndex.put(val.toLowerCase().replace(" ", ""), entry.getKey());
				}
				// Handle upper/lower case differences of key and remove any whitespace
				invertedIndex.put(entry.getKey().toLowerCase().replace(" ", ""), entry.getKey());
			}
			indeces.put(config.name, invertedIndex);
		}
		this.tagValuesInverted.put(payerAccountId, indeces);
		
		// Create the maps setting tags based on the values of other tags
		List<Map<Long, List<TagMapper>>> mapped = Lists.newArrayList();
		for (int tagIndex = 0; tagIndex < customTags.size(); tagIndex++) {
			String tagKey = customTags.get(tagIndex);
			TagConfig tc = configs.get(tagKey);
			if (tc == null || tc.mapped == null || tc.mapped.isEmpty()) {
				mapped.add(null);
				continue;
			}
			Map<Long, List<TagMapper>> mappers = Maps.newTreeMap();
			for (TagMappings m: tc.mapped) {
				TagMapper mapper = new TagMapper(tagIndex, m, tagResourceGroupIndeces);
				List<TagMapper> l = mappers.get(mapper.getStartMillis());
				if (l == null) {
					l = Lists.newArrayList();
					mappers.put(mapper.getStartMillis(), l);
				}
				l.add(mapper);
			}
			mapped.add(mappers);
		}
		this.tagMappers.put(payerAccountId, mapped);
    }

	@Override
    public void init() {		
    }
	
	@Override
	public List<String> getCustomTags() {
		return customTags;
	}
	
	@Override
	public List<UserTagKey> getUserTagKeys() {
		return userTagKeys;
	}
	
	@Override
	public int getUserTagIndex(String tag) {
		Integer index = tagResourceGroupIndeces.get(tag);
		return index == null ? -1 : index;
	}

    @Override
    public ResourceGroup getResourceGroup(Account account, Region region, Product product, LineItem lineItem, long millisStart) {
    	if (customTags.size() == 0)
    		return null;

        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.size()];
       	for (int i = 0; i < customTags.size(); i++) {
       		tags[i] = getUserTagValue(lineItem, customTags.get(i));
       	}
       	
       	// Handle any tag mapping
    	List<Map<Long, List<TagMapper>>> tagMappersForPayerAccount = tagMappers.get(lineItem.getPayerAccountId());
    	
       	for (int i = 0; i < customTags.size(); i++) {
       		String v = tags[i];
       		
       		// Apply tag mappers if any
       		if (tagMappersForPayerAccount != null) {
       	    	Map<Long, List<TagMapper>> tagMappersForKey = tagMappersForPayerAccount.get(i);
	        	if (tagMappersForKey != null)
	        		v = getMappedUserTagValue(tagMappersForKey, millisStart, account, tags, tags[i]);
       		}
       		
       		// Apply default mappings if any
        	if (v == null || v.isEmpty())
        		v = account.getDefaultUserTagValue(customTags.get(i), millisStart);
        	if (v == null)
        		v = ""; // never return null entries
        	tags[i] = v;
        }
       	
		try {
			// We never use null entries, so should never throw
			return ResourceGroup.getResourceGroup(tags);
		} catch (ResourceException e) {
			logger.error("Error creating resource group from user tags in line item" + e);
		}
		return null;
    }
    
    // Used by the reservation capacity poller
    @Override
    public ResourceGroup getResourceGroup(Account account, Product product, List<com.amazonaws.services.ec2.model.Tag> reservedInstanceTags, long millisStart) {
    	if (customTags.size() == 0)
    		return null;
    	
        // Build the resource group based on the values of the custom tags
    	String[] tags = new String[customTags.size()];
       	for (int i = 0; i < customTags.size(); i++) {
           	String v = null;
   			// find first matching key with a legitimate value
       		for (com.amazonaws.services.ec2.model.Tag riTag: reservedInstanceTags) {
       			if (riTag.getKey().toLowerCase().equals(customTags.get(i).toLowerCase())) {
       				v = riTag.getValue();
       				if (v != null && !v.isEmpty())
       					break;
       			}
       		}
        	if (v == null || v.isEmpty())
        		v = account.getDefaultUserTagValue(customTags.get(i), millisStart);
        	if (v == null)
        		v = ""; // never return null entries
        	tags[i] = v;
       	}
		try {
			// We never use null entries, so should never throw
			return ResourceGroup.getResourceGroup(tags);
		} catch (ResourceException e) {
			logger.error("Error creating resource group from user tags in line item" + e);
		}
		return null;
    }
    
    private String getMappedUserTagValue(Map<Long, List<TagMapper>> tagMappersForKey, long startMillis, Account account, String[] tags, String value) {
    	// return the user tag value for the specified account if there is a mapping configured.
    	
    	// Get the time-ordered values
    	Collection<List<TagMapper>> timeOrderedListsOfTagMappers = tagMappersForKey.values();
    	
    	for (List<TagMapper> tml: timeOrderedListsOfTagMappers) {
    		for (TagMapper tm: tml)
    			value = tm.apply(startMillis, account.getId(), tags, value);
    	}	
    	
    	return value;
    }
    
    /**
     * Efficient string replace to remove spaces - much faster than String.replace()
     */
    private String stripSpaces(String s) {
    	if (s.isEmpty())
    		return "";
    	
    	char[] ca = s.toCharArray();
    	StringBuilder ret = new StringBuilder(ca.length);
    	
    	for (int i = 0; i < ca.length; i++) {
    		if (ca[i] == ' ')
    			continue;
    		ret.append(ca[i]);
    	}    	
    	return ret.toString();
    }
    
    @Override
    public String getUserTagValue(LineItem lineItem, String tag) {
    	if (includeReservationIds && tag == reservationIdsKeyName) {
    		String id = lineItem.getReservationArn();
    		if (id.isEmpty())
    			id = lineItem.getSavingsPlanArn();
    		return id;
    	}
    	
    	Map<String, Map<String, String>> indeces = tagValuesInverted.get(lineItem.getPayerAccountId());    	
    	Map<String, String> invertedIndex = indeces == null ? null : indeces.get(tag);
    	
    	// Grab the first non-empty value
    	for (int index: tagLineItemIndeces.get(tag)) {
    		if (lineItem.getResourceTagsSize() > index) {
    	    	// cut all white space from tag value
    			String val = stripSpaces(lineItem.getResourceTag(index));
    			
    			if (!StringUtils.isEmpty(val)) {
    				if (invertedIndex != null && invertedIndex.containsKey(val.toLowerCase())) {
	    				val = invertedIndex.get(val.toLowerCase());
	    			}
	    			return val;
    			}
    		}
    	}
    	return null;
    }
    
    @Override
    public boolean[] getUserTagCoverage(LineItem lineItem) {
    	boolean[] userTagCoverage = new boolean[userTagKeys.size()];
        for (int i = 0; i < userTagKeys.size(); i++) {
        	String v = getUserTagValue(lineItem, userTagKeys.get(i).name);
        	userTagCoverage[i] = !StringUtils.isEmpty(v);
        }    	
    	return userTagCoverage;
    }

    @Override
    public void commit() {

    }
    
    @Override
    public void initHeader(String[] header, String payerAccountId) {
    	tagLineItemIndeces = Maps.newHashMap();
    	Map<String, TagConfig> configs = tagConfigs.get(payerAccountId);
    	
    	/*
    	 * Create a list of billing report line item indeces for
    	 * each of the configured user tags. The list will first have
    	 * the exact match for the name if it exists in the report
    	 * followed by any case variants and specified aliases
    	 */
    	for (UserTagKey tagKey: userTagKeys) {
    		String fullTag = USER_TAG_PREFIX + tagKey.name;
    		List<Integer> indeces = Lists.newArrayList();
    		tagLineItemIndeces.put(tagKey.name, indeces);
    		
    		// First check the preferred key name
    		int index = -1;
    		for (int i = 0; i < header.length; i++) {
    			if (header[i].equals(fullTag)) {
    				index = i;
    				break;
    			}
    		}
    		if (index >= 0) {
    			indeces.add(index);
    		}
    		// Look for alternate names
            for (int i = 0; i < header.length; i++) {
            	if (i == index) {
            		continue;	// skip the exact match we handled above
            	}
            	if (fullTag.equalsIgnoreCase(header[i])) {
            		indeces.add(i);
            	}
            }
            // Look for aliases
            if (configs != null && configs.containsKey(tagKey.name)) {
            	TagConfig config = configs.get(tagKey.name);
            	if (config != null && config.aliases != null) {
	            	for (String alias: config.aliases) {
	            		String fullAlias = alias.startsWith(AWS_TAG_PREFIX) ? alias : USER_TAG_PREFIX + alias;
	                    for (int i = 0; i < header.length; i++) {
	                    	if (fullAlias.equalsIgnoreCase(header[i])) {
	                    		indeces.add(i);
	                    	}
	                    }
	            	}
            	}
            }
    	}
    }
}
