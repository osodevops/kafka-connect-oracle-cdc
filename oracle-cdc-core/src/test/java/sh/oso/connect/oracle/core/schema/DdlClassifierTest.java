/*
 * Copyright 2026 OSO DevOps Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sh.oso.connect.oracle.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** SCH-1 and SCH-5: every statement form maps to what the engine must do with it. */
class DdlClassifierTest {

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "create table app.t (id number primary key)|CREATE_TABLE",
        "CREATE GLOBAL TEMPORARY TABLE t (id number)|CREATE_TABLE",
        "create unique index i on t(id)|NO_SCHEMA_CHANGE",
        "create or replace view v as select * from t|NO_SCHEMA_CHANGE",
        "alter table \"APP\".\"T\" add (c varchar2(10) default 'x')|COLUMNS",
        "ALTER TABLE t ADD c NUMBER|COLUMNS",
        "alter table t add (v as (id * 2) virtual)|COLUMNS",
        "alter table t drop column c|COLUMNS",
        "alter table t drop (a, b)|COLUMNS",
        "alter table t set unused column c|COLUMNS",
        "alter table t drop unused columns|NO_SCHEMA_CHANGE",
        "alter table t modify (c varchar2(50) not null)|COLUMNS",
        "alter table t rename column a to b|COLUMNS",
        "alter table t rename to t2|RENAME_TABLE",
        "rename t to t2|RENAME_TABLE",
        "alter table t add constraint pk primary key (id)|CONSTRAINTS",
        "alter table t add (constraint u unique (c))|CONSTRAINTS",
        "alter table t drop primary key|CONSTRAINTS",
        "alter table t disable constraint pk|CONSTRAINTS",
        "alter table t modify primary key disable|CONSTRAINTS",
        "alter table t add supplemental log data (all) columns|SUPPLEMENTAL_LOG",
        "alter table t drop supplemental log data (primary key) columns|SUPPLEMENTAL_LOG",
        "alter table t add partition p3 values less than (300)|PARTITION",
        "alter table t drop partition p1|PARTITION",
        "alter table t split partition p2 at (150) into (partition p2a, partition p2b)|PARTITION",
        "alter table t merge partitions p1, p2 into partition p12|PARTITION",
        "alter table t truncate partition p1|PARTITION",
        "alter table t exchange partition p1 with table x|PARTITION",
        "alter table t move online|NO_SCHEMA_CHANGE",
        "alter table t enable row movement|NO_SCHEMA_CHANGE",
        "alter table t shrink space|NO_SCHEMA_CHANGE",
        "alter table t modify lob (c) (cache)|NO_SCHEMA_CHANGE",
        "truncate table t|TRUNCATE",
        "drop table t purge|DROP_TABLE",
        "drop index i|NO_SCHEMA_CHANGE",
        "comment on column t.c is 'it''s a column'|NO_SCHEMA_CHANGE",
        "grant select on t to bob|NO_SCHEMA_CHANGE",
        "/* tool */ -- note\\n alter table t add (z number)|COLUMNS",
        "alter table t frobnicate|UNKNOWN",
        "flashback table t to before drop|UNKNOWN",
        "alter table t rename something|UNKNOWN"
      })
  void classifies(String ddl, DdlClassifier.Kind expected) {
    assertThat(DdlClassifier.classify(ddl.replace("\\n", "\n"))).isEqualTo(expected);
  }

  @org.junit.jupiter.api.Test
  void nothingOrBlankIsUnknown() {
    assertThat(DdlClassifier.classify(null)).isEqualTo(DdlClassifier.Kind.UNKNOWN);
    assertThat(DdlClassifier.classify("  ")).isEqualTo(DdlClassifier.Kind.UNKNOWN);
  }
}
