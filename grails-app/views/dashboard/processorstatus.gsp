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

<%@ page import="com.netflix.ice.reader.ReaderConfig" %>

<%@ page contentType="text/html;charset=UTF-8" %>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Processor Status</title>
</head>
<body>
<div class="list" style="margin: auto; width: 1200px; padding: 20px 30px" ng-controller="processorStatusCtrl">
  <h1>Processor Status
	<g:if test="${ReaderConfig.getInstance().enableReprocessRequests}">
	  <span class="processorStatusControls">
	    <img style="padding-right: 4px" src="${resource(dir: '/')}images/refresh.png" border="0" ng-click="refresh()"/>
	    <img ng-show="processorState == 'stopped'" src="${resource(dir: '/')}images/redball.png" border="0"/>
	    <img ng-show="processorState == 'pending' || processorState == 'stopping'" src="${resource(dir: '/')}images/yellowball.png" border="0"/>
	    <img ng-show="processorState == 'running'" src="${resource(dir: '/')}images/greenball.png" border="0"/>
		{{processorState}}&nbsp;&nbsp;
        <button ng-disabled="isDisabled()" type="submit" class="processorStatusButton" ng-click="process()">Start</button>
      </span>
	</g:if>
  </h1>
  <table>
	<thead>
	  <tr>
		<th>Month</th>
		<th>Last Processed</th>
		<th>Elapsed Time</th>
		<th>Reprocess</th>
		<th>Account</th>
		<th>Report</th>
		<th>Last Modified</th>
	  </tr>
	</thead>
	<tbody ng-repeat="status in statusArray" ng-init="trClass=getTrClass($index)">
	  <tr class="{{trClass}}">
	    <td rowspan="{{status.reports.length}}">{{status.month}}</td>
	    <td rowspan="{{status.reports.length}}">{{status.lastProcessed}}</td>
	    <td rowspan="{{status.reports.length}}">{{status.elapsedTime}}</td>
	    <td rowspan="{{status.reports.length}}">
	      <g:if test="${ReaderConfig.getInstance().enableReprocessRequests}">
	        <input type="checkbox" ng-model="statusArray[$index].reprocess" ng-change="updateStatus($index)"/>
	      </g:if>
	      <g:else>
	        {{status.reprocess}}
	      </g:else>
		</td>
	    <td>{{status.reports[0].accountName}}</td>
	    <td>{{status.reports[0].key}}</td>
	    <td>{{status.reports[0].lastModified}}</td>
	  </tr>
	  <tr ng-repeat="report in status.reports" ng-show="$index > 0" class="{{trClass}}">
        <td>{{report.accountName}}</td>
	    <td>{{report.key}}</td>
	    <td>{{report.lastModified}}</td>
	  </tr>
	</tbody>
  </table>
</div>
</body>
</html>