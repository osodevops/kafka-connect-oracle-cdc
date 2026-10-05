# Copyright 2026 OSO DevOps Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Reading a topic from the beginning to its end with confluent-kafka (``read_committed``)."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from typing import Protocol

from .common import ToolError
from .records import KafkaRecord


class TopicSource(Protocol):
    def read(self, topic: str) -> Iterator[KafkaRecord]: ...


class KafkaTopicSource:
    def __init__(self, bootstrap: str, extra: dict[str, str], timeout_s: float = 60.0) -> None:
        self.config = {
            **extra,
            "bootstrap.servers": bootstrap,
            "group.id": f"oso-cdc-verify-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "isolation.level": "read_committed",
            "auto.offset.reset": "earliest",
        }
        self.timeout_s = timeout_s

    def read(self, topic: str) -> Iterator[KafkaRecord]:
        try:
            from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
        except ImportError as e:  # pragma: no cover - a dependency of the project
            raise ToolError("confluent-kafka is not installed; run `uv sync`.") from e
        consumer = Consumer(self.config)
        try:
            meta = consumer.list_topics(topic, timeout=self.timeout_s)
            info = meta.topics.get(topic)
            if info is None or info.error is not None:
                raise ToolError(f"Topic {topic} does not exist or cannot be read.")
            ends: dict[int, int] = {}
            assignment = []
            for p in sorted(info.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, p), timeout=self.timeout_s
                )
                if high > low:
                    ends[p] = high
                    assignment.append(TopicPartition(topic, p, low))
            if not assignment:
                return
            consumer.assign(assignment)
            remaining = set(ends)
            last_progress = time.monotonic()
            while remaining:
                msg = consumer.poll(1.0)
                if msg is None:
                    if time.monotonic() - last_progress > self.timeout_s:
                        raise ToolError(
                            f"Reading {topic} stalled with partitions {sorted(remaining)} unread."
                        )
                    continue
                last_progress = time.monotonic()
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        remaining.discard(msg.partition())
                        continue
                    raise ToolError(f"Reading {topic} failed: {msg.error()}")
                yield KafkaRecord(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=msg.key(),
                    value=msg.value(),
                    headers=list(msg.headers() or []),
                )
                if msg.offset() >= ends[msg.partition()] - 1:
                    remaining.discard(msg.partition())
        except KafkaException as e:
            raise ToolError(f"Reading {topic} failed: {e}") from None
        finally:
            consumer.close()
