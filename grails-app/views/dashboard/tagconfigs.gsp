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
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <title>Tag Configurations</title>
</head>
<body>
<div class="" style="margin: auto; width: 1200px; padding: 20px 30px" ng-controller="tagconfigsCtrl">
  <div ng-repeat="payer in payers" class="list">
    <h1>Tag Settings for {{payer}}</h1>
    <h2>Mappings<span class="resourcesButtons"><input ng-model="filter_mappings" type="text" class="resourcesFilter" placeholder="Filter"/>
    </span></h2>   
	<table style="width: 100%;">
	  <thead>
	    <tr>
	      <th ng-click="order(mappedValues[payer], 'destKey')">Destination Tag</th>
	      <th ng-click="order(mappedValues[payer], 'destValue')">Destination Value</th>
	      <th ng-click="order(mappedValues[payer], 'srcValue')">Source</th>
	      <th>Start</th>
	      <th>Account Filter</th>
	    </tr>
	  </thead>
	  <tbody>
	    <tr ng-repeat="mappedValue in mappedValues[payer] | filter:filter_mappings" class="{{getTrClass($index)}}">
	      <td>{{mappedValue.destKey}}</td>
	      <td>{{mappedValue.destValue}}</td>
	      <td>{{mappedValue.src}}</td>
	      <td>{{mappedValue.start}}</td>
	      <td>{{mappedValue.filter}}</td>
	    </tr>
	  </tbody>
	</table>
	<h2>Consolidations<span class="resourcesButtons"><input ng-model="filter_consolidations" type="text" class="resourcesFilter" placeholder="Filter"/>
	</span></h2>
	<table style="width: 100%;">
	  <thead>
	    <tr>
	      <th ng-click="order(consolidations[payer], 'key')">Tag Key</th>
	      <th>Tag Key Aliases</th>
	      <th ng-click="order(consolidations[payer], 'value')">Tag Value</th>
	      <th>Tag Value Aliases</th>
	    </tr>
	  </thead>
	  <tbody>
	    <tr ng-repeat="consolidation in consolidations[payer] | filter:filter_consolidations" class="{{getTrClass($index)}}">
	      <td>{{consolidation.key}}</td>
	      <td>{{consolidation.keyAliases}}</td>
	      <td>{{consolidation.value}}</td>
	      <td>{{consolidation.valueAliases}}</td>
	    </tr>
	  </tbody>
	</table>
  </div>
</div>
</body>
</html>