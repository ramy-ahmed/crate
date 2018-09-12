/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.blob.v2.BlobIndex;
import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewMetaData;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelationNameTest extends CrateUnitTest {

    @Test
    public void testIndexName() throws Exception {
        RelationName ti = new RelationName(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.indexName(), is("t"));
        ti = new RelationName("s", "t");
        assertThat(ti.indexName(), is("s.t"));
    }

    @Test
    public void testFromIndexName() throws Exception {
        assertThat(RelationName.fromIndexName("t"), is(new RelationName(Schemas.DOC_SCHEMA_NAME, "t")));
        assertThat(RelationName.fromIndexName("s.t"), is(new RelationName("s", "t")));

        PartitionName pn = new PartitionName(new RelationName("s", "t"), ImmutableList.of(new BytesRef("v1")));
        assertThat(RelationName.fromIndexName(pn.asIndexName()), is(new RelationName("s", "t")));

        pn = new PartitionName(new RelationName("doc", "t"), ImmutableList.of(new BytesRef("v1")));
        assertThat(RelationName.fromIndexName(pn.asIndexName()), is(new RelationName(Schemas.DOC_SCHEMA_NAME, "t")));
    }

    @Test
    public void testFromIndexNameCreatesCorrectBlobRelationName() {
        RelationName relationName = new RelationName("blob", "foobar");
        String indexName = relationName.indexName();
        assertThat(BlobIndex.isBlobIndex(indexName), is(true));
        assertThat(RelationName.fromIndexName(indexName), is(relationName));
    }

    @Test
    public void testDefaultSchema() throws Exception {
        RelationName ti = new RelationName(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.schema(), is("doc"));
        assertThat(ti, is(new RelationName("doc", "t")));
    }

    @Test
    public void testFQN() throws Exception {
        RelationName ti = new RelationName(Schemas.DOC_SCHEMA_NAME, "t");
        assertThat(ti.fqn(), is("doc.t"));

        ti = new RelationName("s", "t");
        assertThat(ti.fqn(), is("s.t"));
    }

    @Test
    public void testFqnFromIndexName() throws Exception {
        assertThat(RelationName.fqnFromIndexName("t1"), is(Schemas.DOC_SCHEMA_NAME + ".t1"));
        assertThat(RelationName.fqnFromIndexName("my_schema.t1"), is("my_schema.t1"));
        assertThat(RelationName.fqnFromIndexName(".partitioned.t1.abc"), is(Schemas.DOC_SCHEMA_NAME + ".t1"));
        assertThat(RelationName.fqnFromIndexName("my_schema..partitioned.t1.abc"), is("my_schema.t1"));
    }

    @Test
    public void testFqnFromIndexNameUnsupported3Parts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid index name: my_schema.t1.foo");
        RelationName.fqnFromIndexName("my_schema.t1.foo");
    }

    @Test
    public void testFqnFromIndexNameUnsupportedMoreThan5Parts() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid index name: my_schema..partitioned.t1.abc.foo");
        RelationName.fqnFromIndexName("my_schema..partitioned.t1.abc.foo");
    }

    @Test
    public void testResolveRelationForFqn() {
        QualifiedName fqn = new QualifiedName(Arrays.asList("schema", "t"));
        RelationName relation = RelationName.resolveRelation(fqn, new SearchPath(), mock(Schemas.class));

        assertThat(relation.schema(), is("schema"));
        assertThat(relation.name(), is("t"));
    }

    @Test
    public void testResolveRelationReturnsRelationFromSearchPath() {
        Schemas schemas = mock(Schemas.class);
        RelationName targetRelation = new RelationName("secondSchema", "t");
        when(schemas.getTableInfo(targetRelation)).thenReturn(mock(TableInfo.class));
        when(schemas.getTableInfo(not(eq(targetRelation)))).thenThrow(new RelationUnknown(targetRelation));

        SearchPath searchPath = new SearchPath(ImmutableList.of("firstSchema", "secondSchema"));
        RelationName resolvedRelation = RelationName.resolveRelation(new QualifiedName("t"), searchPath, schemas);

        assertThat(resolvedRelation, is(targetRelation));
    }

    @Test
    public void testResolveRelationResolvesViews() {
        Schemas schemas = mock(Schemas.class);
        when(schemas.getTableInfo(any(RelationName.class))).thenThrow(new RelationUnknown("v", null));

        RelationName targetView = new RelationName("secondSchema", "v");
        when(schemas.resolveView(targetView)).thenReturn(mock(ViewMetaData.class));

        SearchPath searchPath = new SearchPath(ImmutableList.of("firstSchema", "secondSchema"));
        RelationName resolvedRelation = RelationName.resolveRelation(new QualifiedName("v"), searchPath, schemas);

        assertThat(resolvedRelation, is(targetView));
    }

    @Test
    public void testResolveRelationForMissingRelationThrowsResourceUnknownException() {
        Schemas schemas = mock(Schemas.class);
        when(schemas.getTableInfo(any(RelationName.class))).thenThrow(new RelationUnknown("t", null));

        SearchPath searchPath = new SearchPath(ImmutableList.of("firstSchema", "secondSchema", "thirdSchema"));

        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 't' unknown");
        RelationName.resolveRelation(new QualifiedName("t"), searchPath, schemas);
    }
}
