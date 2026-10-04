---
title: Migrating from Confluent Oracle CDC Source
description: Property mapping, takeover SCN and cutover verification.
---

# Migrating from Confluent Oracle CDC Source

**Status:** Phase 2. The translator maps Confluent's public property list to `cdc.*`, the
takeover tool derives a start SCN from the Confluent offsets, and the cutover verifier compares
state at a quiesced SCN. The Confluent-compatible record format lets existing consumers keep
running. Confluent property names and behaviour are taken only from Confluent's public
documentation.
