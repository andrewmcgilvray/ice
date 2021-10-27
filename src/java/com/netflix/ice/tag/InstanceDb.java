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

public enum InstanceDb {
	// I have no idea what description fields are used for reservations other than mysql and postgresql -jmr 7/2018
	
    aurora(".aurora", ":0016", "aurora", true), // "RDS running Amazon Aurora"
    aurorapg(".aurorapg", ":0021", "aurorapg", true), // "RDS running Amazon Aurora PostgreSQL"
    mariadb(".mariadb", ":0018", "mariadb", true), // "RDS running MariaDB (LI)"
    mysql(".mysql", ":0002", "mysql", true), // "RDS running MySQL (BYOL)"
    mysqlvmw(".mysqlvmw", ":0110", "mysqlvmware", true), // "RDS running MySQL (BYOL) On-Premises VMware"
    mysql2(".mysql2", ":0210", "mysql2", true), // "RDS running MySQL (BYOL)"
    orclstdbyol(".orclstdbyol", ":0004", "orclstdbyol", true), // "RDS running Oracle Standard (BYOL)"
    orclentbyol(".orclentbyol", ":0005", "orclentbyol", true), // "RDS running Oracle Enterprise (BYOL)"
    orclent(".orclent", ":0410", "orclent", false), // "RDS running Oracle Enterprise"
    orclstd1byol(".orclstd1byol", ":0003", "orclstd1byol", true), // "RDS running Oracle Standard One (BYOL)"
    orclstd1(".orclstd1", ":0006", "orclstd1", false), // "RDS running Oracle Standard One"
    orclstd2byol(".orclstd2byol", ":0019", "orclstd2byol", true), // "RDS running Oracle Standard Two (BYOL)"
    orclstd2(".orclstd2", ":0020", "orclstd2", false), // "RDS running Oracle Standard Two"
    postgres(".postgres", ":0014", "postgresql", true), // "RDS running PostgreSQL (BYOL)"
    postgresvmw(".postgresvmw", ":0120", "postgresqlvmware", true), // "RDS running PostgreSQL On-Premises VMware"
    postgres2(".postgres2", ":0220", "postgresql2", true), // "RDS running PostgreSQL (BYOL)"
    sqlstdbyol(".sqlstdbyol", ":0008", "sqlstdbyol", true), // "RDS running SQL Server Standard (BYOL)"
    sqlentbyol(".sqlentbyol", ":0009", "sqlentbyol", true), // "RDS running SQL Server Enterprise (BYOL)"
    sqlentvmw(".sqlentvmw", ":0100", "sqlentvmware", true), // "RDS running SQL Server Enterprise On-Premises VMware with Customer provided license"
    sqlexp(".sqlexp", ":0010", "sqlexp", false), // "RDS running SQL Server Express"
    sqlweb(".sqlweb", ":0011", "sqlweb", false), // "RDS running SQL Server Web"
    sqlstd(".sqlstd", ":0012", "sqlstd", false), // "RDS running SQL Server Standard"
    sqlent(".sqlent", ":0015", "sqlent", false), // "RDS running SQL Server Enterprise"
    sqlopweb(".sqlopweb", ":0232", "sqlopweb", false), // "RDS running SQL Server (on-premise for Outpost) Web"
    sqlopstd(".sqlopstd", ":0231", "sqlopstd", false), // "RDS running SQL Server (on-premise for Outpost) Standard"
    sqlopent(".sqlopent", ":0230", "sqlopent", false), // "RDS running SQL Server (on-premise for Outpost) Enterprise"
    others(".others", ":others", "others", false);

    public final String usageType;
    public final String code;
    public final String description;
    public final boolean familySharing;	// Does the DB type allow family instance type sharing of Reserved Instances?

    InstanceDb(String usageType, String code, String description, boolean sharing) {
        this.usageType = usageType;
        this.code = code.toLowerCase();
        this.description = description.toLowerCase();
        this.familySharing = sharing;
    }

    public static InstanceDb withCode(String code) {
        for (InstanceDb db: InstanceDb.values()) {
            if (code.toLowerCase().equals(db.code))
                return db;
        }
        return others;
    }

    /**
     * withDescription() returns the InstanceDb value based on the product description in an
     * RDS ReservedDBInstance object.
     */
    public static InstanceDb withDescription(String description) {
    	String descLC = description.toLowerCase();
        for (InstanceDb db: InstanceDb.values()) {
            if (descLC.startsWith(db.description))
                return db;
        }
        return others;
    }
}
