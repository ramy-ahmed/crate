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

import java.util.ArrayList;
import java.util.List;

/**
 * As writing fully qualified table names is usually tedious. This class models a list of schemas the system will use in
 * order to determine which table is meant by the user.
 */
public final class SearchPath {

    static final String PG_CATALOG_SCHEMA = "pg_catalog";
    private final boolean pgCatalogIsSetExplicitly;
    private final List<String> searchPath;

    public SearchPath() {
        pgCatalogIsSetExplicitly = false;
        searchPath = ImmutableList.of(PG_CATALOG_SCHEMA, Schemas.DOC_SCHEMA_NAME);
    }

    public SearchPath(ImmutableList<String> schemas) {
        assert schemas.size() > 0 : "Expecting at least one schema in the search path";
        pgCatalogIsSetExplicitly = schemas.contains(PG_CATALOG_SCHEMA);
        if (pgCatalogIsSetExplicitly) {
            this.searchPath = schemas;
        } else {
            ArrayList<String> completeSearchPath = new ArrayList<>(1 + schemas.size());
            completeSearchPath.add(PG_CATALOG_SCHEMA);
            completeSearchPath.addAll(schemas);
            this.searchPath = ImmutableList.copyOf(completeSearchPath);
        }
    }

    public String currentSchema() {
        if (pgCatalogIsSetExplicitly) {
            return searchPath.get(0);
        } else {
            return searchPath.get(1);
        }
    }

    public String defaultSchema() {
        return currentSchema();
    }

    List<String> searchPath() {
        return searchPath;
    }
}
