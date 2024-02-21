/*
 * Copyright 2024 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
package org.opensearch.remoteprocessor.processors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.pb.action.search.SearchSource;
import org.opensearch.search.GenericSearchExtBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.test.OpenSearchTestCase;

public class ProtoTranslationUtilsTests extends OpenSearchTestCase {
    // Oh boy, here we go.

    private org.opensearch.action.search.SearchRequest osRequest;

    private org.opensearch.action.search.SearchResponse osResponse;

    @Before
    public void setup() throws Exception {
        setupSearchRequest();
        setupSearchResponse();
    }

    private void setupSearchRequest() throws IOException {
        this.osRequest = new org.opensearch.action.search.SearchRequest("index");
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        // Give it a match query
        ssb.query(new MatchQueryBuilder("field", "query text"));
        // Give it a search ext
        XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, "{\"field\":\"name\"}");
        parser.nextToken();
        ssb.ext(List.of(GenericSearchExtBuilder.fromXContent(parser)));

        ssb.from(1);
        ssb.size(3);

        this.osRequest.source(ssb);
        this.osRequest.pipeline("test-pipeline");
        this.osRequest.requestCache(Boolean.FALSE);
    }

    private void setupSearchResponse() throws IOException {
        org.opensearch.search.SearchHit[] hitArray = new org.opensearch.search.SearchHit[2];
        hitArray[0] = new org.opensearch.search.SearchHit(0, "doc-0", Map.of(), Map.of());
        hitArray[0].score(4.0f);
        hitArray[0].sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8))));
        hitArray[0].shard(new SearchShardTarget("node", new ShardId("index", "indexid", 0), null, null));
        hitArray[1] = new org.opensearch.search.SearchHit(1, "doc-1", Map.of(), Map.of());
        hitArray[1].score(1.3f);
        hitArray[1]
            .sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap("{\"field\":\"other value\"}".getBytes(StandardCharsets.UTF_8))));
        hitArray[1].shard(new SearchShardTarget("node", new ShardId("index", "indexid", 0), null, null));
        org.opensearch.search.SearchHits hits = new org.opensearch.search.SearchHits(
            hitArray,
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            4.0f
        );
        org.opensearch.action.search.SearchResponseSections srs = new org.opensearch.action.search.SearchResponseSections(
            hits,
            null,
            null,
            false,
            false,
            new SearchProfileShardResults(Map.of()),
            1
        );
        this.osResponse = new org.opensearch.action.search.SearchResponse(
            srs,
            "scrollid",
            3,
            3,
            0,
            21L,
            new ShardSearchFailure[0],
            new SearchResponse.Clusters(1, 1, 0)
        );
    }

    public void testSearchRequestToPB_HappyPath() throws IOException {
        org.opensearch.pb.action.search.SearchRequest pbRequest = ProtoTranslationUtils.SearchRequestOsToPb(this.osRequest);
        assertEquals("test-pipeline", pbRequest.getPipeline());
        assertEquals(Boolean.FALSE, pbRequest.getRequestCache());
        SearchSource source = pbRequest.getSource();
        assertEquals(ProtoTranslationUtils.toCbor(this.osRequest.source()), source.getSourceBytes());
    }

    public void testSearchResponseToPB_AndBack() throws IOException {
        org.opensearch.pb.action.search.SearchResponse pbResponse = ProtoTranslationUtils.SearchResponseOsToPb(this.osResponse);
        SearchResponse newOsResponse = ProtoTranslationUtils.SearchResponsePbToOs(pbResponse, this.osResponse);
        assertEquals(this.osResponse.getInternalResponse().hits().getAt(0), newOsResponse.getInternalResponse().hits().getAt(0));
        assertEquals(this.osResponse.getInternalResponse().hits().getAt(1), newOsResponse.getInternalResponse().hits().getAt(1));
        assertEquals(
            this.osResponse.getInternalResponse().hits().getMaxScore(),
            newOsResponse.getInternalResponse().hits().getMaxScore(),
            0.0000000000001
        );
        assertEquals(this.osResponse.getScrollId(), newOsResponse.getScrollId());
        assertEquals(this.osResponse.getTotalShards(), newOsResponse.getTotalShards());
        // I think you get the point
    }
}
