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
package com.netflix.ice.processor.postproc;

import java.util.List;
import java.util.Map;

import com.netflix.ice.common.TagMappings;
import com.netflix.ice.processor.config.DerivedConfig;
import com.netflix.ice.processor.config.S3BucketConfig;
import com.netflix.ice.processor.config.KubernetesConfig;

public class AllocationConfig {
	private S3BucketConfig s3Bucket;
	private KubernetesConfig kubernetes;
	private DerivedConfig derived;
	// Input tag map = UserTag Key is the map key, Report Column name is the map value.
	private Map<String, String> in;
	// Output tag map = UserTag Key is the map key, Report Column name is the map value.
	private Map<String, String> out;
	private Map<String, List<TagMappings>> tagMaps;
	private boolean enableWildcards; // enable use of glob-style wildcards for allocation report tag value matching
	
	public S3BucketConfig getS3Bucket() {
		return s3Bucket;
	}
	public void setS3Bucket(S3BucketConfig s3Bucket) {
		this.s3Bucket = s3Bucket;
	}
	public KubernetesConfig getKubernetes() {
		return kubernetes;
	}
	public void setKubernetes(KubernetesConfig kubernetes) {
		this.kubernetes = kubernetes;
	}
	public DerivedConfig getDerived() {
		return derived;
	}
	public void setDerived(DerivedConfig derived) {
		this.derived = derived;
	}
	public Map<String, String> getIn() {
		return in;
	}
	public void setIn(Map<String, String> in) {
		this.in = in;
	}
	public Map<String, String> getOut() {
		return out;
	}
	public void setOut(Map<String, String> out) {
		this.out = out;
	}	
	public Map<String, List<TagMappings>> getTagMaps() {
		return tagMaps;
	}
	public void setTagMaps(Map<String, List<TagMappings>> tagMaps) {
		this.tagMaps = tagMaps;
	}
	public boolean isEnableWildcards() {
		return enableWildcards;
	}
	public void setEnableWildcards(boolean enableWildcards) {
		this.enableWildcards = enableWildcards;
	}
}
