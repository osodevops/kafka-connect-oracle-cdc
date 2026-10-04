# Naming, Trademark and SEO Research

**Status:** Research, source-linked. Collected 4 October 2026. Semrush data pulled the same day (US, UK, DE, IN databases).
**Question:** Can we put "oracle" in the repository name and the domain name, does it matter for SEO, and what should the website domain be (one domain per connector)?

---

## 1. Short answer

| Asset | Can we use "oracle"? | Evidence | Decision |
|---|---|---|---|
| Domain name | No. Oracle's guidelines prohibit it and Oracle enforces it through domain disputes, including against a site with real training content | Sections 2 and 3 | `kafkacdcconnector.com` (section 6); "oracle" appears in URL paths and page copy instead |
| Repository name | Technically against the guideline for open-source distribution names, but it is the ecosystem norm and there is no evidence of enforcement against repositories; a rename is cheap because GitHub redirects renamed repositories | Sections 2 and 4 | `osodevops/kafka-connect-oracle-cdc` |
| Product name | No; this is the clearest rule | Section 2 | "OSO CDC Connector for Oracle Database" |
| Page titles, headings, descriptions, GitHub description and topics | Yes, as descriptive references ("for Oracle Database", "Oracle CDC") | Section 2 | Use freely, with the trademark notice |

The SEO cost of keeping "oracle" out of the domain is small: our own exact-match domain for Salesforce has delivered almost nothing, and nobody in the top ten for "oracle cdc" has "oracle" in the domain apart from Oracle itself (section 5).

## 2. What Oracle's guidelines say

From the [Third Party Usage Guidelines for Oracle Trademarks](https://www.oracle.com/legal/trademarks/):

- Product names: "Do not use Oracle trademarks or potentially confusing variations as all or part of your company, product or service names." The example given: "'XYZ for Oracle database' not 'OraXYZ or XYZ Oracle'".
- Taglines are allowed: "accurate, descriptive tag lines such as 'for Oracle database,' ... and 'works with Oracle software' in connection with your product or service name", in smaller type than your own name and never in Oracle red.
- Domains: "Do not use Oracle trademarks or potentially confusing variations in your Internet domain name. This helps prevent Internet users from being confused as to whether you or Oracle is the source of the Web site."
- Open source: "Without a license or permission, you may not incorporate Oracle trademarks in the name of your distribution or other products that incorporate open source elements."
- Reference use is fine: "You may generally use Oracle trademarks to refer to the associated Oracle products or services."

## 3. Does Oracle enforce it?

| Case | What happened |
|---|---|
| `oracleappstechnical.com`, WIPO D2022-3022 | A site offering online training on Oracle applications. The panel ordered transfer to Oracle, finding the site did not disclose its lack of relationship with Oracle and risked misleading users into thinking it was operated by or affiliated with Oracle ([decision text](https://www.fullegal.com/panel/karar-arama/wpo-domain-name-decision-d2022-3022-for-oracl-18331085)) |
| `oracecorp.com`, WIPO D2022-4284 | Typosquat with pay-per-click links; transferred ([WIPO decision](https://www.wipo.int/amc/en/domains/decisions/pdf/2022/d2022-4284.pdf)) |
| `oeaclecloud.com` and others, WIPO D2020-0450 | Typosquats on Oracle Cloud ([WIPO decision](https://www.wipo.int/amc/en/domains/decisions/text/2020/d2020-0450.html)) |
| Oracle v. Sonoo Jaiswal (javatpoint), Delhi High Court, 12 February 2024 | Over JAVA, not ORACLE, but instructive: a long-running training site was ordered not to use JAVA in its domain or company names. Oracle told the court it "does not object to the use of its trademark JAVA by the developer community insofar as it is descriptive of their knowledge of, proficiency in, or use of the JAVA programming language" ([court order](https://images.assettype.com/barandbench/2024-02/1d3bd5b6-c8c6-40c9-8960-8f94d643ae66/Oracle_America_Inc_v_Sonoo_Jaiswal_and_Ors.pdf)) |
| CryptoOracle, US federal court, 2024 | Oracle sued over continued use of the CRYPTOORACLE name on domains and social accounts after an agreement to stop ([complaint](https://cdn.worldipreview.com/files/2024/11/f90688f0-ae4b-11ef-98cc-751a93f4950b-Oracle%20TM%20suit.pdf)) |

Counter-example: [oracle-base.com](https://oracle-base.com/misc/site-info) has run since 2000 and ranks well (Semrush: about 5,000 US organic keywords, about 5,300 monthly visits). Its author, Tim Hall, is an Oracle ACE Director, so this is a community site Oracle tolerates rather than a precedent for a commercial vendor.

**Conclusion:** a commercial product site at a domain containing "oracle" is the exact pattern Oracle has won transfers for. If that happens after we have built links and rankings, we lose the domain and its SEO equity at once. That is the worst outcome for SEO, not the best.

## 4. What everyone else does

| Vendor or project | Product name | Repository or package | Domain and URL |
|---|---|---|---|
| Confluent | Oracle CDC Source Connector | `confluentinc/kafka-connect-oracle-cdc` on [Confluent Hub](https://www.confluent.io/hub/confluentinc/kafka-connect-oracle-cdc) | confluent.io (Oracle partner; has an XStream agreement with Oracle) |
| Debezium (Red Hat, IBM) | Debezium connector for Oracle | Module `debezium-connector-oracle` | [debezium.io](https://debezium.io/documentation/reference/stable/connectors/oracle.html) |
| A2 Solutions | oracdc | [averemee-si/oracdc](https://github.com/averemee-si/oracdc), Maven group `solutions.a2.oracle`; listed as "Oracle CDC Source Connector by A2 Solutions" on [Confluent Hub](https://www.confluent.io/hub/a2solutions/oracdc-kafka) | a2-solutions.eu |
| erdemcer | kafka-connect-oracle | [erdemcer/kafka-connect-oracle](https://github.com/erdemcer/kafka-connect-oracle) (354 stars, LogMiner) | GitHub only |
| thake | Logminer Kafka Connect | [thake/logminer-kafka-connect](https://github.com/thake/logminer-kafka-connect) | GitHub only |
| Striim | Oracle Reader, OJet | n/a | [striim.com/connectors/oracle](https://www.striim.com/connectors/oracle/) |
| Estuary | Oracle Database connector | n/a | [estuary.dev/source/oracle](https://estuary.dev/source/oracle/) |
| Airbyte | Oracle DB connector | `source-oracle` | [docs.airbyte.com](https://docs.airbyte.com/integrations/sources/oracle) |
| Redpanda, Decodable, Flink CDC | Oracle CDC connector | `oracle-cdc` | Brand domains with `/oracle-cdc` paths |

Pattern: repository and package names include "oracle" freely and descriptively. Product brands avoid it or use the "X for Oracle" form. No commercial vendor puts "oracle" in its domain; they all use a brand domain with an `/oracle` or `/oracle-cdc` path.

## 5. SEO evidence (Semrush)

### 5.1 Exact-match domains are not what ranks

- **Our own test:** `salesforcekafkaconnector.com` ranks for 2 US keywords with an estimated 0 monthly visits, and for nothing in the UK (Semrush domain overview, 4 October 2026). The exact-match domain has not produced traffic on its own.
- **Top ten for "oracle cdc" (US):** striim.com (blog), domo.com, confluent.io (Hub page), qlik.com, asktom.oracle.com, airbyte.com, **github.com/averemee-si/oracdc (position 7)**, learn.microsoft.com, medium.com, decodable.co. No domain contains "oracle" except Oracle's own.
- **Top ten for "oracle cdc connector" (US):** docs.confluent.io, nightlies.apache.org (Flink CDC), confluent.io, medium.com, **github.com/averemee-si/oracdc (position 5)**, redpanda.com, tapdata.io, reddit.com, bryteflow.com, decodable.co.
- **"oracle logminer":** Oracle docs (positions 1, 2 and 7), striim.com, wikipedia.org, oracle-base.com, cdata, oceanbase, YouTube, Stack Overflow.
- **What wins is content plus authority:** Striim ranks first or second for "oracle cdc", "oracle change data capture", "oracle replication", "oracle database replication" and "oracle logminer" with long-form blog posts on a brand domain (striim.com: about 3,700 US organic keywords).

**Implications:**
1. A GitHub repository ranks on page one for Oracle CDC terms. The repository name, description and README heading are worth more to us than the domain, which supports putting "oracle-cdc" in the repository name.
2. We win the domain side with content on the connector's own domain: pages such as `/oracle-cdc-kafka`, `/debezium-oracle-alternative`, `/confluent-oracle-cdc-alternative`, `/oracle-logminer-guide`, `/goldengate-alternative`.

### 5.2 Keyword demand (monthly searches)

Full table in `oracle_naming_keywords.csv`. Head terms:

| Keyword | US | UK | DE | IN | US CPC | US KD |
|---|---|---|---|---|---|---|
| oracle cdc | 210 | 30 | 20 | 110 | $8.42 | 26 |
| oracle change data capture | 210 | 20 | 20 | 110 | $8.42 | 18 |
| oracle replication | 170 | 30 | 20 | 30 | $14.58 | 19 |
| change data capture oracle | 90 | 20 | 20 | 110 | $4.77 | 20 |
| oracle logminer | 90 | 20 | 20 | 50 | $7.89 | 23 |
| logminer | 90 | 20 | 20 | 30 | $6.85 | 6 |
| cdc oracle | 50 | 0 | 10 | 110 | $8.42 | 19 |
| kafka connect oracle | 40 | 20 | 20 | 30 | n/a | n/a |
| kafka oracle connector | 40 | 20 | 20 | 10 | n/a | n/a |
| oracle kafka connector | 40 | 20 | 20 | 0 | n/a | n/a |
| kafka connect oracle cdc | 40 | 20 | 0 | 0 | n/a | n/a |
| debezium oracle | 20 | 20 | 20 | 30 | n/a | n/a |
| oracle xstream | 30 | 20 | 20 | 10 | $15.00 | n/a |
| oracle cdc connector | 30 | 20 | 10 | 0 | $6.61 | 21 |
| oracle to snowflake | 50 | 20 | 10 | 0 | $81.65 | 22 |
| oracle goldengate kafka | 20 | 10 | 20 | 20 | n/a | n/a |
| goldengate alternative | 20 | 10 | 0 | 20 | n/a | n/a |

Wider terms from Semrush's related-keyword report for "oracle cdc" (US): change data capture 1,900 (KD 61), cdc tools 390, kafka cdc 140, goldengate cdc 110, golden gate cdc 110, cdc connector 50 (Semrush related-keywords report).

**Reading the data:**
- These head terms add up to only a few hundred searches a month per country. The 1,680 figure in the connector ranking came from the long tail of Kafka-plus-Oracle queries. That makes ranking on many long-tail pages (error messages, configuration questions, migration guides) more important than one exact-match domain.
- Keyword difficulty is low (under 30) for nearly every Oracle CDC term, so a focused content hub can rank.
- CPCs show buyer intent: oracle replication at $14.58, oracle xstream at $15.00, oracle cdc at $8.42 and oracle to snowflake at $81.65. Those are the pages to write first.
- India is the second-largest market for these terms ("oracle cdc" 110, "redo log" 140), which matters for content and community, though it is less relevant for enterprise support sales.
- Debezium's own Oracle page ranks in positions 20 to 40 for many Confluent Oracle CDC property-name queries (for example "connector.class io.confluent.connect.oracle.cdc.oraclecdcsourceconnector", 90 a month). A Confluent migration page that documents every property by name (PRD-04) can take that long tail.

## 6. Website domain selection

Constraint: one domain per connector, as with `salesforcekafkaconnector.com`.

### 6.1 Words we cannot or should not use

| Word | Status | Evidence |
|---|---|---|
| oracle | Prohibited in domains and product names | Sections 2 and 3 |
| logminer | Listed by Oracle as an Oracle trademark | Oracle8i documentation notice: "LogMiner ... are trademarks or registered trademarks of Oracle Corporation" ([Oracle docs](https://docs.oracle.com/cd/A87862_01/NT817CLI/server.817/a76956.pdf)) |
| goldengate | Oracle product mark | Oracle product name |
| kafka | Allowed in a domain with extra words that clearly differ from the bare mark, for a directly related, differently named product; not allowed alone, and not in a product's primary brand | ASF: domains "must include additional words or characters that clearly differentiate" and "may only be used to provide services and/or differently-named software products that are directly related to the Apache product" ([ASF domain policy](https://www.apache.org/foundation/marks/domains.html)); no Apache mark in "the primary or secondary branding of any third party product" ([ASF FAQ](https://www.apache.org/foundation/marks/faq/)) |

### 6.2 Search demand for the words we can use (Semrush, monthly)

| Keyword | US | UK | US CPC | US KD |
|---|---|---|---|---|
| kafka cdc | 140 | 20 | $6.12 | 20 |
| cdc kafka | 110 | 20 | $6.12 | 30 |
| kafka cdc connector | 50 | 20 | $3.26 | 27 |
| cdc connector | 50 | 20 | $6.73 | 26 |
| kafka change data capture | 50 | 20 | $7.00 | 18 |
| kafka connect cdc | 40 | 20 | n/a | 24 |
| debezium alternative | 40 | 0 | $7.95 | n/a |
| oracle scn | 30 | 20 | n/a | n/a |
| system change number | 20 | 0 | n/a | n/a |
| cdc kafka connector | 10 | 0 | n/a | n/a |

The top ten for "kafka cdc" (US) is all content pages on brand domains: confluent.io, medium.com, aerospike.com, factorhouse.io, youtube.com, docs.ditto.live, paimon.apache.org, medium.com, qlik.com, conduktor.io.

### 6.3 Shortlist (registry lookups via RDAP, 4 October 2026)

| Domain | .com status | Assessment |
|---|---|---|
| **kafkacdcconnector.com** | Unregistered | **Chosen.** Same pattern as `salesforcekafkaconnector.com`; exact match for "kafka cdc connector" and contains "kafka cdc"; clearly a connector, which suits the ASF "directly related" test |
| kafkacdc.com | Unregistered | Shortest and matches the top term, but reads like a product named "Kafka CDC", which is weaker under the ASF policy |
| cdcconnector.com | Unregistered | No trademark questions, but generic and loses "kafka" |
| scnconnector.com | Unregistered | Oracle DBAs recognise SCN; almost no search value |
| scnflow.com, commitstream.com, cdcbridge.com, dbcdc.com, kcdc.com | Registered | Not available |

Trade-off accepted: `kafkacdcconnector.com` uses the generic "Kafka CDC" phrase for Oracle. A later CDC connector for another database (for example DB2) gets its own system-named domain such as `db2kafkaconnector.com`.

Action: register `kafkacdcconnector.com` now (and the `.io` and `.dev` variants, also unregistered on 4 October 2026, to prevent squatting). Registrar premium pricing was not checked.

## 7. Recommendation

| Item | Choice |
|---|---|
| Product name | OSO CDC Connector for Oracle Database ("OSO CDC Connector") |
| Repository | `osodevops/kafka-connect-oracle-cdc` |
| GitHub description | "Oracle Database CDC source connector for Apache Kafka Connect. LogMiner based, exactly-once, Apache-2.0. Drop-in alternative to Confluent Oracle CDC and Debezium Oracle." |
| GitHub topics | `oracle`, `oracle-cdc`, `logminer`, `change-data-capture`, `kafka-connect`, `kafka`, `debezium-alternative`, `goldengate-alternative` |
| Maven | `sh.oso:kafka-connect-oracle-cdc` (plugin), `sh.oso:oracle-cdc-core`, `sh.oso:oracle-cdc-doctor` |
| Confluent Hub listing | "OSO CDC Connector for Oracle Database (Oracle CDC Source)", the same form A2 uses |
| Domain | `kafkacdcconnector.com` (approved 4 October 2026); one domain for this connector, as with `salesforcekafkaconnector.com` |
| Notice | README, site footer and Hub listing: "Oracle and Java are registered trademarks of Oracle and/or its affiliates. This project is not affiliated with or endorsed by Oracle." The disclaimer is what the oracleappstechnical.com site lacked |
| If Oracle objects to the repository name | Rename to `kafka-connect-oso-cdc`; GitHub redirects old URLs, so links keep working |

## 8. Content plan that follows from the data

| Priority | Page | Target terms |
|---|---|---|
| 1 | Oracle CDC to Kafka: the complete guide | oracle cdc, oracle change data capture, kafka connect oracle, oracle kafka connector |
| 2 | Confluent Oracle CDC alternative and migration (every property documented) | confluent oracle cdc, the property-name long tail |
| 3 | Debezium Oracle alternative: problems and fixes (based on `debezium_oracle_pain_points.md`) | debezium oracle, debezium oracle connector |
| 4 | Oracle LogMiner guide for CDC | oracle logminer, logminer, oracle log mining, redo log |
| 5 | GoldenGate and XStream alternatives | goldengate alternative, oracle goldengate kafka, oracle xstream |
| 6 | Oracle replication to Snowflake, Postgres and Iceberg through Kafka | oracle replication, oracle to snowflake, oracle to postgres replication |
| 7 | Error pages (ORA-01291, ORA-01555, ORA-00310, ORA-01013 with LogMiner) generated from our runbooks | Long tail |
