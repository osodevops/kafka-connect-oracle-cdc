# Test Lab, Licensing and Budget

**Status:** Draft for approval, 4 October 2026
**Replaces:** The "licensed Oracle lab on OSO infrastructure" assumption in earlier drafts. Everything below runs on Sion's workstation, a dev AWS account and the OCI free tier.

---

## 1. Summary

- **Software licences: no purchase needed for development and testing.** Oracle offers Enterprise Edition as a "Full-featured, free version" that is "Ideal for developing, prototyping, and testing in a non-production environment" ([Oracle downloads](https://www.oracle.com/asean/database/technologies/oracle-database-software-downloads.html)). The OTN terms limit this to internal development, testing, prototyping and demonstration ([OTN License](https://www.oracle.com/downloads/licenses/standard-license.html)). Two conditions matter to us; see section 5.
- **Local first.** A large x86-64 Linux workstation runs the whole matrix, including a two-node RAC cluster and a Data Guard standby, at no cost.
- **AWS dev account for the rest.** RDS for Oracle (which you cannot run locally), a RAC host if the workstation cannot run it, and burst capacity for soaks. Expected spend is roughly 100 to 450 US dollars a month with scheduled shutdown (section 4).
- **OCI Always Free for Autonomous Database.** Two Autonomous Database instances at no cost, enough for the Phase 3 per-PDB mining work.

## 2. What runs where

| Target | Local workstation | AWS dev account | OCI free tier |
|---|---|---|---|
| Oracle Database Free 23ai and 26ai (CI images, two PDBs) | Yes | CI runners | No |
| 19c EE and 21c EE single instance (`container-registry.oracle.com/database/enterprise`) | Yes | Optional | No |
| Two-node RAC 19c or 21c on Podman | Yes, if the host is x86-64 Oracle Linux 8.10 or later (or an OL8 VM) | Yes, one large EC2 host | No |
| 26ai RAC on Podman | Yes, same requirements | Yes | No |
| Active Data Guard physical standby | Yes (two containers) | Optional | No |
| Kubernetes (Oracle AI Database Operator) | Yes (kind or k3s) | Optional (EKS not needed) | No |
| Amazon RDS for Oracle, non-CDB and CDB | No | Yes, only during qualification runs | No |
| Autonomous Database (per-PDB mining) | No | No | Yes, two Always Free instances |
| Kafka, Connect, Toxiproxy, Grafana | Yes | Yes (containers on EC2, no MSK) | No |
| Amazon MSK Connect smoke test | No | Yes, short release runs only | No |

### 2.1 RAC facts that drive the design

- Oracle supports RAC on Podman for production from 19c (19.16) and 21c (21.7), on Oracle Linux 8.10 or later; the prebuilt image is `container-registry.oracle.com/database/rac_ru`, currently tagged 21.16 ([oracle/docker-images RAC](https://github.com/oracle/docker-images/blob/main/OracleDatabase/RAC/OracleRealApplicationClusters/README.md)).
- Oracle's 19c target configuration uses 16 GB RAM plus 16 GB swap per node ([19c RAC on Podman](https://docs.oracle.com/en/database/oracle/oracle-database/19/racpd/target-configuration-oracle-rac-podman.html)); the 26ai configuration lists 32 GB RAM ([26ai RAC on Podman](https://docs.oracle.com/en/database/oracle/oracle-database/26/racpd/target-configuration-oracle-rac-podman.html)). Shared storage is a block device of at least 50 GB visible to both nodes, or NFS ([RAC image guide](https://github.com/oracle/docker-images/blob/main/OracleDatabase/RAC/OracleRealApplicationClusters/docs/rac-container/racimage/README.md)).
- Kubernetes: an Oracle blog from 2023 said OraOperator did not support RAC ([Oracle blog](https://blogs.oracle.com/coretec/oracle-database-now-containernative)); the v2.2.0 release now lists RAC and ASM storage lifecycle management ([Oracle AI Database Operator v2.2.0](https://blogs.oracle.com/database/announcing-oracle-ai-database-operator-for-kubernetes-v2-2-0)). We use Podman for RAC because it is Oracle's certified path, and Kubernetes for single-instance and Data Guard topologies.
- Apple Silicon: 19c EE exists for Linux ARM (aarch64) for single instance ([dbi services](https://www.dbi-services.com/blog/running-an-oracle-database-19c-on-apple-silicon-apple-macbook-air-m1/)). Oracle's RAC on Podman guides are for x86-64 Oracle Linux, so RAC on an Apple Silicon Mac is not a realistic target. If the workstation is a Mac, RAC runs in AWS.

### 2.2 Workstation sizing for the full local matrix

| Component | vCPU | RAM | Disk |
|---|---|---|---|
| Two-node RAC (19c or 21c) | 4 to 8 | 32 GB plus 32 GB swap (64 GB plus for 26ai) | 50 GB shared plus 100 GB |
| Data Guard pair (19c) | 4 | 16 GB | 100 GB |
| 21c and Free 23ai and 26ai single instances | 6 | 24 GB | 60 GB |
| Kafka (three brokers), Connect (two workers), Toxiproxy, Grafana, Prometheus | 6 | 16 GB | 100 GB |
| Workload generator and correctness oracle | 4 | 8 GB | 20 GB |
| **Total (everything at once)** | **24 to 28** | **about 100 to 130 GB** | **about 500 GB NVMe** |

Running suites one topology at a time halves this.

## 3. AWS dev account design

All infrastructure as code in `lab/aws/` (Terraform), so nothing is clicked by hand.

| Resource | Spec | When it runs |
|---|---|---|
| `cdc-lab-rac` EC2 | m7i.8xlarge (32 vCPU, 128 GiB), Oracle Linux 8 AMI, gp3 root plus a 100 GB gp3 volume shared by both RAC containers on the same host | Working hours or nightly window; Spot where possible |
| `cdc-lab-rac-multihost` (optional) | Two m7i.2xlarge plus one io2 Multi-Attach volume for real two-host RAC ([EBS Multi-Attach](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volumes-multi.html)) | Release qualification only |
| `cdc-lab-rds-noncdb` | RDS for Oracle 19c SE2 License Included, db.m5.large, single AZ | Created for a qualification run, destroyed after |
| `cdc-lab-rds-cdb` | Same, CDB architecture | Phase 3 per-PDB work |
| Kafka and Connect | Containers on the RAC host (no MSK) | With the host |
| MSK Connect smoke test | Smallest configuration | One short run per release |
| Guardrails | AWS Budgets alarm, mandatory `project=cdc-lab` and `ttl` tags, a Lambda or scheduled stop for anything past its TTL, no public database endpoints (SSM Session Manager only) | Always |

RDS licence model: License Included is available only for Standard Edition 2; Enterprise Edition on RDS is bring-your-own-licence ([AWS RDS licensing](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Oracle.Concepts.Licensing.html), [AWS SE2 blog](https://aws.amazon.com/blogs/database/rethink-oracle-standard-edition-two-on-amazon-rds-for-oracle/)). SE2 License Included is enough to qualify LogMiner behaviour on RDS. We do not run RDS EE, because the OTN terms do not cover a BYOL RDS deployment.

## 4. Budget

Prices are London (eu-west-2) on-demand unless stated; check in the AWS Pricing Calculator before approving.

| Item | Unit price | Usage assumption | Monthly |
|---|---|---|---|
| m7i.8xlarge RAC host, on-demand | $1.8648 per hour ([InstanceFinder](https://instancefinder.com/aws/i/m7i.8xlarge.html)) | 8 hours a day, 22 days (176 hours) | about $330 |
| Same host on Spot | from about $0.28 per hour in eu-west-2 ([InstanceFinder](https://instancefinder.com/aws/i/m7i.8xlarge.html)) | 176 hours | about $50 to $90 |
| Same host left running 24x7 on-demand | $1.8648 per hour | 730 hours | about $1,360 (avoid) |
| m6i.4xlarge (16 vCPU, 64 GiB) as a smaller host | $0.888 per hour ([DoiT](https://www.doit.com/compute/compute/aws/eu-west-2/m6i.large)) | 176 hours | about $155 |
| RDS SE2 License Included, db.m5.large | about $0.51 per hour (Tokyo list price; London will be similar, [comparison](https://www.issoh.co.jp/tech/details/10885/)) | 40 hours of qualification a month | about $20 to $25 plus storage |
| EBS gp3 storage | standard gp3 rates | 500 GB | about $40 to $50 |
| OCI Autonomous Database, two Always Free instances | free; 20 GB each, 30 sessions, stopped after 7 days idle ([Oracle docs](https://docs.oracle.com/en/cloud/paas/autonomous-database/serverless/adbsb/autonomous-always-free.html)) | Phase 3 | $0 |
| OCI Ampere A1 VM | free; 4 cores and 24 GB ([Ampere](https://amperecomputing.com/products/ecosystem/oracle-cloud)) | Optional ARM test client | $0 |

| Scenario | Monthly estimate |
|---|---|
| Local workstation runs everything, AWS only for RDS and MSK checks | about $25 to $75 |
| Workstation is a Mac, so RAC runs in AWS on Spot with scheduled stop | about $120 to $200 |
| RAC in AWS on-demand during working hours, plus RDS and storage | about $400 to $450 |
| Release month with two-host RAC and a 72-hour soak | add about $150 to $300 |

Nightly T2 runs stay on the workstation or the RAC host's scheduled window. Nothing runs 24x7 by default.

## 5. Licensing conditions to respect

| Condition | Source | What we do |
|---|---|---|
| Development, testing, prototyping and demonstration only, "only as long as Your application has not been used for any data processing, business, commercial, or production purposes" | [OTN License](https://www.oracle.com/downloads/licenses/standard-license.html) | Lab databases hold synthetic data only. Once customers run the connector in production, the right basis for continued lab use needs a legal view; an Oracle PartnerNetwork membership is the usual route for ISVs and is worth pricing before 1.0 GA |
| Benchmark results "may not be disclosed without Oracle's prior consent" | [OTN License](https://www.oracle.com/downloads/licenses/standard-license.html) | We do **not** publish database benchmark figures. Our docs publish the benchmark harness so customers can measure on their own licensed systems. Internal comparisons with Debezium stay internal. This supersedes the earlier plan to publish benchmark results |
| Oracle may audit use | [OTN License](https://www.oracle.com/downloads/licenses/standard-license.html) | Lab inventory kept in `lab/README.md` (what runs where, which image, which terms) |
| Container images must be pulled by the user who accepts Oracle's terms | [oracle/docker-images RAC](https://github.com/oracle/docker-images/blob/main/OracleDatabase/RAC/OracleRealApplicationClusters/README.md) | Our derived images live in a private registry only; public CI uses Oracle Database Free, which Oracle describes as free to develop, deploy and distribute ([Oracle downloads](https://www.oracle.com/asean/database/technologies/oracle-database-software-downloads.html)) |
| RDS License Included use is subject to the AWS Service Terms | [AWS RDS for Oracle FAQs](https://aws.amazon.com/rds/oracle/faqs/) | Standard dev account use |

## 6. Repository additions

```
lab/
  README.md                 inventory and licence basis per environment
  local/
    compose/                Oracle Free, 19c, 21c, Kafka, Connect, Toxiproxy, Grafana
    rac/                    Podman scripts for two-node RAC (19c, 21c, 26ai)
    dataguard/              primary and standby containers
    k8s/                    kind or k3s with the Oracle AI Database Operator
  aws/
    terraform/              RAC host, optional multi-host RAC, RDS, budgets, TTL stopper
    Makefile                make up-rac, make down, make rds-qualify
  oci/
    terraform/              Always Free Autonomous Database pair
```

## 7. Acceptance criteria

- [ ] `make -C lab/local up-all` brings up the full local matrix on the workstation from a clean machine in under two hours (images cached afterwards).
- [ ] `make -C lab/aws up-rac` produces a working two-node RAC with archive logging and the capture user in under 90 minutes, and `make down` removes everything.
- [ ] AWS Budgets alarm fires at 80 per cent of the agreed monthly limit; any resource past its `ttl` tag is stopped automatically.
- [ ] No lab database is reachable from the internet.
- [ ] `lab/README.md` records the licence basis for every environment.
