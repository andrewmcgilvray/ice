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

import com.amazonaws.services.ec2.model.Tag;
import com.netflix.ice.processor.LineItem;
import com.netflix.ice.tag.ResourceGroup;
import com.netflix.ice.tag.Account;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.Region;
import com.netflix.ice.tag.UserTagKey;

import java.util.List;
import java.util.Map;

/**
 * Please see a sample of subclass in SampleMapDbResourceService class.
 */
public abstract class ResourceService {
	public static final int MAX_CUSTOM_TAGS = 32;

    /**
     * Subclass can choose different technologies to store the mapping of resource ids and resource group names.
     * E.g. SampleMapDbResourceService used MapDB to maintain a mapping of instance ids and application names.
     * You can initialize you mapping here.
     * You can also get the reference to ProcessorConfig instance here.
     */
    abstract public void init();
    
    abstract public List<String> getCustomTags();
    abstract public List<UserTagKey> getUserTagKeys();

    abstract public Map<String, Map<String, TagConfig>> getTagConfigs();
    
    abstract public TagMappings getTagMappings(String tagKey, String name);
    
    /**
     * Get resource group. Subclass can maintain a mapping of resource ids and resource group names.
     * Users can also choose to add user-defined tags in the billing file. E.g. in SampleMapDbResourceService,
     * the auto-scaling-group name is used to generate the resource group name.
     * @param tagGroup
     * @param lineItem: the line item in the billing file. You can access your user-defined tags here.
     * @param millisStart
     * @return
     */
    abstract public ResourceGroup getResourceGroup(TagGroup tagGroup, LineItem lineItem, long millisStart);
    
    abstract public ResourceGroup getResourceGroup(Account account, Product product, List<Tag> reservedInstanceTags, long millisStart);

    /**
     * Consolidate tag value if configs specify an alias
     */
    abstract public String getCanonicalValue(int keyIndex, String value, String payerAccountId);

    /**
     * Commit resource mappings. This method will be called at the end of billing file processing to commit your mappings.
     */
    abstract public void commit();
    
    /**
     * Initialize the billing file user tags from the userTags header
     */
    abstract public void initHeader(String[] header, String payerAccountId);
    
    abstract public void setTagConfigs(String payerAccountId, List<TagConfig> tagConfigs);

    abstract public boolean[] getUserTagCoverage(LineItem lineItem);

    /**
     * Get the user tag index in ResourceGroup for the requested tag
     */
	abstract public int getUserTagIndex(String tag);

}
