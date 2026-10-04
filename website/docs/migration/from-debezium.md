---
title: Migrating from Debezium Oracle
description: Translate the connector configuration, take over at the Debezium offset SCN and verify.
---

# Migrating from Debezium Oracle

**Status:** Phase 1d. `migrate_from_debezium.py` translates a Debezium Oracle connector
configuration, `takeover_scn.py` reads the Debezium offset and produces the start position, and
`verify_cutover.py` compares the Debezium-materialised state with the new connector's output.
The default envelope is Debezium compatible so consumers need no change.
