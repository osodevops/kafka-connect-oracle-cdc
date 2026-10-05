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
"""Migration tools for the OSO CDC Connector for Oracle Database (PRD-04).

Configuration translators from the Debezium Oracle connector and the Confluent Oracle CDC
Source connector, the takeover offset calculator and the cutover verifier.
"""

from importlib import metadata

try:
    __version__ = metadata.version("oso-cdc-migration")
except metadata.PackageNotFoundError:  # running from a source checkout without installing
    __version__ = "0.1.0.dev0"

OSO_CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector"
