# lab: environments, inventory and licence basis

| Environment | Where | Basis | Status |
|---|---|---|---|
| `local/compose` | Workstation Docker | Oracle Database Free (free to develop and test; base image pulled by the user), Apache Kafka | Available |
| `local/k8s` | minikube (docker driver) on the workstation: Strimzi Kafka and Connect, Oracle Database Free StatefulSet | Oracle Database Free; Apache Kafka via Strimzi | Available (Phase 0) |
| `local/rac`, `local/dataguard` | x86-64 Linux workstation only | OTN development licence | Phase 2 |
| `aws/` | Dev AWS account (Terraform) | OTN development licence on EC2; RDS SE2 License Included | Phase 2 |
| `oci/` | OCI Always Free | Autonomous Database Always Free | Phase 3 |

Lab databases hold synthetic data only. No benchmark figures measured here are published
(`docs/07_test_lab_and_budget.md` section 5). Nothing in any lab is reachable from the internet.
