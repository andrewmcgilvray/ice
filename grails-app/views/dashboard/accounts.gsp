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
  <title>Accounts</title>
</head>
<body>
<div class="" style="margin: auto; width: 1600px; padding: 20px 30px" ng-controller="accountsCtrl">
  <h1>Accounts  	
	    <span class="resourcesButtons">
	      <input ng-model="filter_accounts" type="text" class="resourcesFilter" placeholder="Filter"/>&nbsp;
	      <a href="javascript:void(0)" style="background-image: url(${resource(dir: '/')}images/tango/16/actions/document-save.png)"
	       ng-click="download()"
	       ng-disabled="accounts.length == 0">Download</a>	       
	    </span>
  </h1>
  
  <div class="list">
	  <table style="width: 100%;">
	    <thead>
	    <tr>
	      <th ng-click="order(accounts, 'name')">ICE Name</th>
	      <th ng-click="order(accounts, 'awsName')">AWS Name</th>
	      <th ng-click="order(accounts, 'id')">ID</th>
	      <th ng-click="order(accounts, 'path')">Organization Path</th>
	      <th ng-click="order(accounts, 'status')">Status</th>
	      <th ng-click="order(accounts, 'accessGroupsStr')">Access Groups</th>
	      <th ng-click="order(accounts, 'tagsStr')">Tags</th>
	      <th ng-click="order(accounts, 'email')">Email</th>
	    </tr>
	    </thead>
	    <tbody>
	    <tr ng-repeat="account in accounts | filter:filter_accounts" class="{{getTrClass($index)}}">
	      <td>{{account.name}}</td>
	      <td>{{account.awsName}}</td>
	      <td>{{account.id}}</td>
	      <td>{{account.path}}</td>
	      <td>{{account.status}}</td>
	      <td>{{account.accessGroupsStr}}</td>
	      <td>{{account.tagsStr}}</td>
	      <td>{{account.email}}</td>
	    </tr>
	    </tbody>
	  </table>
  </div>
</div>
</body>
</html>