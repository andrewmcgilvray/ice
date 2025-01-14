<%--

    Copyright 2013 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

--%>

<%@ page contentType="text/html;charset=UTF-8" %>
<%@ page import="com.netflix.ice.reader.ReaderConfig" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Tag Statistics</title>
</head>
<body>
<div class="list" style="margin: auto; width: 1200px; padding: 20px 30px" ng-controller="statisticsCtrl">
  <h1>User Tag Statistics ${params.month == null ? "" : params.month}</h1>
  <g:set var="statistics" value="${ReaderConfig.getInstance().managers.getUserTagStatistics(params.month)}" scope="request"/>
  <div>Total TagGroups if no user tags: ${statistics.nonResourceTagGroups}</div>
  <div>Total TagGroups with user tags: ${statistics.resourceTagGroups}</div>
  <table>
	<thead>
	  <tr>
		<th>Key</th>
		<th>Values</th>
		<th>Case Variations</th>
		<th>TagGroup Permutation Contribution</th>
	  </tr>
	</thead>
	<tbody>
	  <g:set var="odd" value="${0}"/>
	  <g:each in="${statistics.userTagStats}" var="stats">
	    <tr class="${odd == 0 ? 'even' : 'odd'}">
		  <td>${stats.key}</td>
		  <td>${stats.values}</td>
		  <td>${stats.caseVariations}</td>
		  <td>${stats.permutationContribution}</td>
	    </tr>
	    <g:set var="odd" value="${odd ^ 1}"/>
	  </g:each>
	</tbody>
  </table>
  <h1>Post-Processor Statistics ${params.month == null ? ReaderConfig.getInstance().managers.getLatestProcessedMonth() : params.month}</h1>
  <g:set var="postProcessorStats" value="${ReaderConfig.getInstance().managers.getPostProcessorStats(params.month)}" scope="request"/>
  <div class="list">
    <g:if test="${postProcessorStats != null}">
	  <table style="width: 100%;">
	    <thead>
	      <tr>
	        <g:each in="${postProcessorStats[0]}" var="col">
	          <th>${col}</th>
	        </g:each>
	      </tr>
	    </thead>
	    <tbody>
	      <g:set var="odd" value="${0}"/>
	      <g:each in="${postProcessorStats.subList(1, postProcessorStats.size())}" var="stats">
	        <tr class="${odd == 0 ? 'even' : 'odd'}">
	          <g:each in="${stats}" var="col">
	            <td>${col}</td>
	          </g:each>
	        </tr>
	      <g:set var="odd" value="${odd ^ 1}"/>
	      </g:each>
	    </tbody>
	  </table>
	</g:if>
  </div>

</div>
</body>
</html>