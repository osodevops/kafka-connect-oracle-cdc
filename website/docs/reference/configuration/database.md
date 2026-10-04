---
title: "Database"
description: "Database properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 2
---

# Database properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.database.host` | string | none | high | Oracle Database host. Alternative to cdc.database.url. |
| `cdc.database.port` | int | `1521` | medium | Oracle listener port. |
| `cdc.database.service` | string | none | high | Service name (preferred over SID). In a CDB this is the CDB$ROOT service. |
| `cdc.database.sid` | string | none | low | SID, for databases without a service name. |
| `cdc.database.url` | string | none | medium | Full JDBC URL (TNS descriptor, LDAP naming or wallet). Overrides host, port, service and SID. |
| `cdc.database.user` | string | required | high | Mining user. In a CDB this must be a common user (C## prefix) with the grants from oracle-cdc-doctor setup-sql, including CONTAINER_DATA=ALL. |
| `cdc.database.password` | password | required | high | Password for the mining user. Use a Connect config provider; the value is never logged. |
| `cdc.database.wallet.location` | string | none | low | Directory of an Oracle wallet for TLS or stored credentials. |
| `cdc.database.tls.truststore.location` | string | none | low | Trust store file for TLS connections. |
| `cdc.database.tls.truststore.password` | password | none | low | Trust store password. |
| `cdc.database.tls.truststore.type` | string | `JKS` | low | Trust store type (JKS or PKCS12). |
| `cdc.database.kerberos.ccache` | string | none | low | Kerberos credential cache file (Phase 2). |
| `cdc.database.connection.properties` | string | none | low | Extra Oracle JDBC driver properties as k=v;k=v. |
| `cdc.database.idle.timeout.ms` | long | `300000` | low | Idle network guard: an application-level probe runs at half this interval so load balancers with idle timeouts never hang the connector. |
| `cdc.database.pdbs` | list | empty | high | Pluggable databases to capture, comma separated. Leave empty for a non-CDB database. |
| `cdc.database.fan.enabled` | boolean | `false` | low | Subscribe to RAC Fast Application Notification events. |
