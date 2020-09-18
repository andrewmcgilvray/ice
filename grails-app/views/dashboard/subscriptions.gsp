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
  <title>Subscriptions</title>
</head>
<body>
<div class="" style="margin: auto; width: 1800px; padding: 20px 30px" ng-controller="subscriptionsCtrl">
  <h1>Subscriptions</h1>
  <input type="radio" ng-model="ri_sp" value="RI" id="radio_ri" ng-click="update()"> <label for="radio_ri" style="cursor: pointer">Reserved Instances</label>&nbsp;&nbsp;
  <input type="radio" ng-model="ri_sp" value="SP" id="radio_sp" ng-click="update()"> <label for="radio_sp" style="cursor: pointer">Savings Plans</label>
  <span style="padding-left: 20px">Month:	
    <select ng-model="month" ng-options="m for m in months" ng-change="update()"></select>
  </span> 
  <span class="resourcesButtons">
    <input ng-model="filter_subscriptions" type="text" class="resourcesFilter" placeholder="Filter"/>&nbsp;
    <a href="javascript:void(0)" style="background-image: url(${resource(dir: '/')}images/tango/16/actions/document-save.png)"
       ng-click="download()"
       ng-disabled="subscriptions.length == 0">Download</a>	       
  </span>
  <div class="list">
	  <table style="width: 100%;">
	    <thead>
	      <tr>
	        <th ng-repeat="col in header" ng-click="order($index)">{{col}}</th>
	      </tr>
	    </thead>
	    <tbody>
	      <tr ng-repeat="row in subscriptions | filter:filter_subscriptions" class="{{getTrClass($index)}}">
	        <td ng-repeat="col in row">{{col}}</td>
	      </tr>
	    </tbody>
	  </table>
  </div>
</div>
</body>
</html>