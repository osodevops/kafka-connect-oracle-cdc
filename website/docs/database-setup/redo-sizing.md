---
title: Redo sizing and archive retention
description: Online redo group sizing, archive destinations and how long archived logs must stay.
---

# Redo sizing and archive retention

**Status:** guidance; the `redo-profile` and `sizing` doctor commands that measure a database
arrive in Phase 1d.

- Keep archived logs at least as long as the connector could be down plus its mining lag. The
  connector stops with `CDC-2002 LOG_PURGED` rather than skip a purged log.
- Prefer a local archive destination with free space monitoring over a fast recovery area that
  can fill and halt the database.
- Small online redo groups cause frequent switches and more LogMiner session restarts; very
  large groups delay the point at which a change is visible in archive-only mode.
- An archived log deleted from disk still shows `DELETED=NO` in `V$ARCHIVED_LOG` until RMAN
  crosschecks it; the connector's purge detection checks the file, not just the catalog.
