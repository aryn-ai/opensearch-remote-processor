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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.pb.action.search.Aggregations;
import org.opensearch.pb.action.search.Clusters;
import org.opensearch.pb.action.search.DocumentField;
import org.opensearch.pb.action.search.HighlightField;
import org.opensearch.pb.action.search.IndicesOptions;
import org.opensearch.pb.action.search.NestedIdentity;
import org.opensearch.pb.action.search.PhaseTook;
import org.opensearch.pb.action.search.Scroll;
import org.opensearch.pb.action.search.SearchExtBuilder;
import org.opensearch.pb.action.search.SearchHit;
import org.opensearch.pb.action.search.SearchHits;
import org.opensearch.pb.action.search.SearchRequest;
import org.opensearch.pb.action.search.SearchResponseSections;
import org.opensearch.pb.action.search.SearchShardFailure;
import org.opensearch.pb.action.search.SearchShardTarget;
import org.opensearch.pb.action.search.SearchSortValues;
import org.opensearch.pb.action.search.SearchSource;
import org.opensearch.pb.action.search.Suggest;
import org.opensearch.pb.action.search.TotalHits;
import org.opensearch.pb.action.search.SearchRequest.SearchType;
import org.opensearch.search.profile.SearchProfileShardResults;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;

public class ProtoTranslationUtils {
    
    public static org.opensearch.pb.action.search.SearchRequest SearchRequestOsToPb(org.opensearch.action.search.SearchRequest request) throws IOException {

        SearchRequest.Builder b = org.opensearch.pb.action.search.SearchRequest.newBuilder()
            .addAllIndices(Arrays.asList(request.indices()))
            .setBatchedReduceSize(request.getBatchedReduceSize())
            .setCcsMinimizeRoundTrips(request.isCcsMinimizeRoundtrips())
            .setIndicesOptions(
                IndicesOptions.newBuilder()
                .setAllowNoIndices(request.indicesOptions().allowNoIndices())
                .setForbidAliasesToMultipleIndices(!request.indicesOptions().allowAliasesToMultipleIndices())
                .setForbidClosedIndices(request.indicesOptions().forbidClosedIndices())
                .setIgnoreAliases(request.indicesOptions().forbidClosedIndices())
                .setIgnoreThrottled(request.indicesOptions().ignoreThrottled())
                .setIgnoreUnavailable(request.indicesOptions().ignoreUnavailable())
                .setWildcardStatesClosed(request.indicesOptions().expandWildcardsClosed())
                .setWildcardStatesHidden(request.indicesOptions().expandWildcardsHidden())
                .setWildcardStatesOpen(request.indicesOptions().expandWildcardsOpen())
                .build())
            .setMaxConcurrentShardRequests(request.getMaxConcurrentShardRequests())
            .setRequestCache(Boolean.TRUE.equals(request.requestCache()))
            .setSearchType(
                request.searchType() == org.opensearch.action.search.SearchType.QUERY_THEN_FETCH ? 
                    SearchType.QUERY_THEN_FETCH : SearchType.DFS_QUERY_THEN_FETCH)
            .setSource(
                SearchSource.newBuilder()
                .setSourceBytes(toCbor(request.source()))
                .build());
        //Optional fields
        if (request.getCancelAfterTimeInterval() != null) {
            b.setCancelAfterMillis(request.getCancelAfterTimeInterval().millis());
        }
        if (request.scroll() != null) {
            b.setScroll(
                Scroll.newBuilder()
                .setKeepAliveMillis(request.scroll().keepAlive().millis())
                .build());
        }
        if (request.isPhaseTook() != null) {
            b.setPhaseTook(request.isPhaseTook());
        }
        if (request.getPreFilterShardSize() != null) {
            b.setPrefilterSize(request.getPreFilterShardSize());
        }
        if (request.pipeline() != null) {
            b.setPipeline(request.pipeline());
        }
        if (request.routing() != null) {
            b.setRouting(request.routing());
        }
        if (request.preference() != null) {
            b.setPreference(request.preference());
        }


        return b.build();
    }

    public static org.opensearch.pb.action.search.SearchResponse SearchResponseOsToPb(org.opensearch.action.search.SearchResponse response) throws IOException {

        List<SearchExtBuilder> newExts = new ArrayList<>(response.getInternalResponse().getSearchExtBuilders().size());
        for (org.opensearch.search.SearchExtBuilder seb : response.getInternalResponse().getSearchExtBuilders()) {
            newExts.add(SearchExtBuilder.newBuilder().setSearchExts(toCbor(seb)).build());
        }

        Map<String, Long> phaseTookMap;
        if(response.getPhaseTook() != null) {
            PipedOutputStream os = new PipedOutputStream();
            PipedInputStream is = new PipedInputStream(os);
            OutputStreamStreamOutput osso = new OutputStreamStreamOutput(os);
            InputStreamStreamInput issi = new InputStreamStreamInput(is);
            response.getPhaseTook().writeTo(osso);
            phaseTookMap = issi.readMap(StreamInput::readString, StreamInput::readLong);
            issi.close();
        } else {
            phaseTookMap = new HashMap<>();
        }

        SearchHits.Builder shb = SearchHits.newBuilder()
            .setMaxScore(response.getHits().getMaxScore())
            .setTotalHits(
                TotalHits.newBuilder()
                .setValue(response.getHits().getTotalHits().value)
                .setRelation(
                    response.getHits().getTotalHits().relation == org.apache.lucene.search.TotalHits.Relation.EQUAL_TO ?
                    TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                ))
            .addAllHits(
                Arrays.asList(response.getHits().getHits()).stream()
                .map(ProtoTranslationUtils::translateHitToPb)
                .collect(Collectors.toList()));
        if(response.getHits().getCollapseField() != null) {
            shb.setCollapseField(response.getHits().getCollapseField());
        }
        if(response.getHits().getCollapseValues() != null) {
            shb.addAllCollapseValues(
                Arrays.asList(response.getHits().getCollapseValues())
                    .stream()
                    .map(ProtoTranslationUtils::javaToBytes)
                    .collect(Collectors.toList())
            );
        }

        SearchResponseSections.Builder srsb = SearchResponseSections.newBuilder()
            .setHits(shb.build())
            .setTimedOut(Boolean.TRUE.equals(response.isTimedOut()))
            .setTerminatedEarly(Boolean.TRUE.equals(response.isTerminatedEarly()))
            .setNumReducePhases(response.getNumReducePhases())
            .addAllSearchExts(newExts);
        // Optional fields
        if(response.getAggregations() != null) {
            srsb.setAggregations(
                Aggregations.newBuilder()
                .setAggregations(toCbor(response.getAggregations()))
                .build()
            );
        }
        if(response.getSuggest() != null) {
            srsb.setSuggest(
                Suggest.newBuilder()
                .setSuggestions(toCbor(response.getSuggest()))
                .build()
            );
        }  

        org.opensearch.pb.action.search.SearchResponse.Builder srb = org.opensearch.pb.action.search.SearchResponse.newBuilder()
            .setClusters(
                response.getClusters() == null ? null :
                Clusters.newBuilder()
                .setSkipped(response.getClusters().getSkipped())
                .setSuccessful(response.getClusters().getSuccessful())
                .setTotal(response.getClusters().getTotal())
                .build())
            .setInternalResponse(srsb.build())
            .setTotalShards(response.getTotalShards())
            .setSuccessfulShards(response.getSuccessfulShards())
            .setSkippedShards(response.getSkippedShards())
            .addAllShardFailures(
                Arrays.asList(response.getShardFailures())
                .stream()
                .map(ssf -> 
                    SearchShardFailure.newBuilder()
                    .setReason(ssf.reason())
                    .setTarget(
                        SearchShardTarget.newBuilder()
                        .setIndexId(ssf.index())
                        .setNodeId(ssf.shard().getNodeId())
                        .setShardId(ssf.shard().toString())
                        .build())
                    .build())
                .collect(Collectors.toList()))
            .setTookInMillis(response.getTook() == null ? null : response.getTook().millis())
            .setPhaseTook(PhaseTook.newBuilder().putAllPhaseTookMap(phaseTookMap).build());
        
        if(response.getScrollId() != null) {
            srb.setScrollId(response.getScrollId());
        }
        if(response.pointInTimeId() != null) {
            srb.setPointInTimeId(response.pointInTimeId());
        }

        return srb.build();
    }

    public static ByteString toCbor(ToXContent serializable) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = XContentType.CBOR.contentBuilder(baos);
        serializable.toXContent(builder, null);
        builder.close();
        return ByteString.copyFrom(baos.toByteArray());
    }

    public static ByteString javaToBytes(Object obj) {
        Gson gson = new Gson();
        return ByteString.copyFromUtf8(gson.toJson(obj));
    }

    private static SearchHit translateHitToPb(org.opensearch.search.SearchHit hit) {
        HashMap<String, DocumentField> newDocFields = new HashMap<>();
        HashMap<String, DocumentField> newMetaFields = new HashMap<>();
        for (String key: hit.getFields().keySet()) {
            org.opensearch.common.document.DocumentField df = hit.removeDocumentField(key);
            if(df == null) {
                newMetaFields.put(key, 
                    DocumentField.newBuilder()
                    .setName(hit.field(key).getName())
                    .addAllValues(
                        Arrays.asList(hit.field(key).getValues())
                        .stream()
                        .map(ProtoTranslationUtils::javaToBytes)
                        .collect(Collectors.toList()))
                    .build());
            } else {
                hit.setDocumentField(key, df);
                newDocFields.put(key, 
                    DocumentField.newBuilder()
                    .setName(df.getName())
                    .addAllValues(
                        Arrays.asList(df.getValues())
                        .stream()
                        .map(ProtoTranslationUtils::javaToBytes)
                        .collect(Collectors.toList()))
                    .build());
            }
        }

        HashMap<String, HighlightField> newHighlightFields = new HashMap<>();
        for (String key: hit.getHighlightFields().keySet()) {
            newHighlightFields.put(key, 
                HighlightField.newBuilder()
                .setName(hit.getFields().get(key).getName())
                .addAllFragments(
                    Arrays.asList(hit.getHighlightFields().get(key).getFragments())
                    .stream()
                    .map(frag -> frag.string())
                    .collect(Collectors.toList()))
                .build());
        }

        SearchHit.Builder shb =  SearchHit.newBuilder()
            .setDocId(hit.docId())
            .setScore(hit.getScore())
            .setId(hit.getId())
            .setVersion(hit.getVersion())
            .setSeqNo(hit.getSeqNo())
            .setPrimaryTerm(hit.getPrimaryTerm())
            .setSource(ByteString.copyFrom(hit.getSourceAsString(), Charset.defaultCharset()))
            .putAllDocumentFields(newDocFields)
            .putAllMetaFields(newMetaFields)
            .putAllHighlightFields(newHighlightFields)
            .setSortValues(
                SearchSortValues.newBuilder()
                .addAllFormattedSortValues(
                    Arrays.asList(hit.getSortValues())
                    .stream().map(ProtoTranslationUtils::javaToBytes)
                    .collect(Collectors.toList()))
                .addAllRawSortValues(
                    Arrays.asList(hit.getRawSortValues())
                    .stream().map(ProtoTranslationUtils::javaToBytes)
                    .collect(Collectors.toList()))
                .build())
            .addAllMatchedQueries(Arrays.asList(hit.getMatchedQueries()))
            .setShard(
                SearchShardTarget.newBuilder()
                .setIndexId(hit.getShard().getIndex())
                .setNodeId(hit.getShard().getNodeId())
                .setShardId(hit.getShard().getShardId().toString())
                .build())
            .setIndex(hit.getIndex());

        if(hit.getNestedIdentity() != null) {
            shb.setNestedId(
                NestedIdentity.newBuilder()
                .setField(hit.getNestedIdentity().getField().string())
                .setOffset(hit.getNestedIdentity().getOffset())
                .build());
        }
        if(hit.getClusterAlias() != null) {
            shb.setClusterAlias(hit.getClusterAlias());
        }
        
        return shb.build();
    }

    private static org.opensearch.search.SearchHit findHitWithId(org.opensearch.action.search.SearchResponse ogResponse, int docId) {
        for(org.opensearch.search.SearchHit h : ogResponse.getHits()) {
            if(h.docId() == docId) {
                return h;
            }
        }
        return null;
    }

    private static org.opensearch.search.SearchHit TranslateHitToOs(SearchHit hit, org.opensearch.search.SearchHit ogHit) {
        Map<String, org.opensearch.common.document.DocumentField> docFieldMap = new HashMap<>();
        Map<String, org.opensearch.common.document.DocumentField> metaFieldMap = new HashMap<>();
        for(String key: hit.getDocumentFieldsMap().keySet()) {
            docFieldMap.put(key, ogHit.field(key));
        }
        for(String key: hit.getMetaFieldsMap().keySet()) {
            metaFieldMap.put(key, ogHit.field(key));
        }

        Map<String, org.opensearch.search.fetch.subphase.highlight.HighlightField> highlightFieldMap = new HashMap<>();
        for(String key: hit.getHighlightFieldsMap().keySet()) {
            String[] fragStrings = new String[hit.getHighlightFieldsMap().get(key).getFragmentsCount()];
            hit.getHighlightFieldsMap().get(key).getFragmentsList().toArray(fragStrings);
            highlightFieldMap.put( key, 
                new org.opensearch.search.fetch.subphase.highlight.HighlightField(
                    hit.getHighlightFieldsMap().get(key).getName(),
                    Text.convertFromStringArray(fragStrings)
                )
            );
        }

        org.opensearch.search.SearchHit newHit = new org.opensearch.search.SearchHit(
            hit.getDocId(),
            hit.getId(),
            docFieldMap,
            metaFieldMap
        );
        newHit.score(hit.getScore());
        newHit.version(hit.getVersion());
        newHit.setSeqNo(hit.getSeqNo());
        newHit.setPrimaryTerm(hit.getPrimaryTerm());
        newHit.sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap(hit.getSource().toByteArray())));
        newHit.highlightFields(highlightFieldMap);
        newHit.matchedQueries(hit.getMatchedQueriesList().toArray(new String[hit.getMatchedQueriesCount()]));
        newHit.shard(ogHit.getShard());
        
        return newHit;
    }

    public static org.opensearch.action.search.SearchResponse SearchResponsePbToOs(org.opensearch.pb.action.search.SearchResponse response, org.opensearch.action.search.SearchResponse originalResponse) throws IOException {

        org.opensearch.search.suggest.Suggest newSuggest;
        if(response.getInternalResponse().getSuggest().getSuggestions().isEmpty()) {
            newSuggest = null;
        } else {
            XContentParser suggestParser = XContentType.CBOR.xContent().createParser(null, null, response.getInternalResponse().getSuggest().getSuggestions().toByteArray());
            newSuggest = org.opensearch.search.suggest.Suggest.fromXContent(suggestParser);
        }

        org.opensearch.search.aggregations.Aggregations newAggs;
        if(response.getInternalResponse().getAggregations().getAggregations().isEmpty()) {
            newAggs = null;
        } else {
            XContentParser aggsParser = XContentType.CBOR.xContent().createParser(null, null, response.getInternalResponse().getAggregations().getAggregations().toByteArray());
            newAggs = org.opensearch.search.aggregations.Aggregations.fromXContent(aggsParser);
        }

        ArrayList<org.opensearch.search.SearchHit> newHits = new ArrayList<>();
        for(SearchHit hit : response.getInternalResponse().getHits().getHitsList()) {
            org.opensearch.search.SearchHit ogHit = findHitWithId(originalResponse, hit.getDocId());
            newHits.add(TranslateHitToOs(hit, ogHit));
        }
        org.opensearch.search.SearchHit[] hitBuf = new org.opensearch.search.SearchHit[newHits.size()];

        return new SearchResponse(
            new org.opensearch.action.search.SearchResponseSections(
                new org.opensearch.search.SearchHits(
                    newHits.toArray(hitBuf), 
                    new org.apache.lucene.search.TotalHits(
                        response.getInternalResponse().getHits().getTotalHits().getValue(),
                        response.getInternalResponse().getHits().getTotalHits().getRelation() == TotalHits.Relation.EQUAL_TO ?
                            org.apache.lucene.search.TotalHits.Relation.EQUAL_TO :
                            org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                    ), 
                    response.getInternalResponse().getHits().getMaxScore(), 
                    originalResponse.getHits().getSortFields(), 
                    response.getInternalResponse().getHits().getCollapseField(), 
                    originalResponse.getHits().getCollapseValues()), 
                newAggs, 
                newSuggest, 
                response.getInternalResponse().getTimedOut(), 
                response.getInternalResponse().getTerminatedEarly(), 
                new SearchProfileShardResults(originalResponse.getProfileResults()), 
                response.getInternalResponse().getNumReducePhases(), 
                originalResponse.getInternalResponse().getSearchExtBuilders()),
            response.hasScrollId() ? response.getScrollId() : null, 
            response.getTotalShards(), 
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTookInMillis(),
            new SearchResponse.PhaseTook(response.getPhaseTook().getPhaseTookMapMap()),
            originalResponse.getShardFailures(),
            new SearchResponse.Clusters(
                response.getClusters().getTotal(), 
                response.getClusters().getSuccessful(), 
                response.getClusters().getSkipped()),
            response.hasPointInTimeId() ? response.getPointInTimeId() : null
        );

    }
}
