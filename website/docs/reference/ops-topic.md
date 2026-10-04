---
title: Ops topic
description: Versioned operational events the connector publishes about itself.
---

# Ops topic

**Status:** planned for Phase 1b. The connector publishes versioned events (position advanced,
transaction journaled, transaction split, snapshot chunk completed, DDL applied, task stopped
with error code) to an ops topic so operators and the doctor can explain lag without reading
worker logs.
