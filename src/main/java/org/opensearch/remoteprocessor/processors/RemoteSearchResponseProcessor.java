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
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.grpc.ProcessResponseRequest;
import org.opensearch.grpc.ProcessResponseResponse;
import org.opensearch.grpc.RemoteProcessorServiceGrpc;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class RemoteSearchResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    public static final String TYPE = "remote_processor";
    public static final String ENDPOINT_FIELD = "endpoint";
    public static final String PROCESSOR_NAME_FIELD = "processor_name";

    private final String endpoint;
    private final String processorName;
    private final RemoteProcessorServiceGrpc.RemoteProcessorServiceStub rpsRpcClient;

    protected RemoteSearchResponseProcessor(String tag, String description, boolean ignoreFailure, String endpoint, String processorName) {
        super(tag, description, ignoreFailure);
        this.endpoint = endpoint;
        this.processorName = processorName;
        ChannelCredentials creds = InsecureChannelCredentials.create();
        ManagedChannel chan = Grpc.newChannelBuilder(this.endpoint, creds).build();
        this.rpsRpcClient = RemoteProcessorServiceGrpc.newStub(chan);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        throw new UnsupportedOperationException("Use processResponseAsync instead since this processor makes network calls");
    }

    @Override
    public void processResponseAsync(
        SearchRequest request,
        SearchResponse response,
        PipelineProcessingContext ctx,
        ActionListener<SearchResponse> listener
    ) {
        try {
            // Serialize the request, response, context, and then send it to endpoint.
            // Wait for response, deserialize it, and return it.
            ProcessResponseRequest grpcRequest = buildRequest(request, response, ctx);
            System.out.println(grpcRequest.toString());
            sendRequest(
                grpcRequest,
                ActionListener.wrap(grpcResponse -> { listener.onResponse(parseResponse(grpcResponse, response)); }, e -> {
                    listener.onFailure(e);
                })
            );
        } catch (IOException e) {
            System.out.println(e.getMessage());
            listener.onFailure(e);
        }

    }

    private ProcessResponseRequest buildRequest(SearchRequest request, SearchResponse response, PipelineProcessingContext ctx)
        throws IOException {
        return ProcessResponseRequest
            .newBuilder()
            .setSearchRequest(ProtoTranslationUtils.SearchRequestOsToPb(request))
            .setSearchResponse(ProtoTranslationUtils.SearchResponseOsToPb(response))
            .setProcessorName(this.processorName)
            .build();
    }

    private void sendRequest(ProcessResponseRequest request, ActionListener<ProcessResponseResponse> listener) {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                this.rpsRpcClient.processResponse(request, new StreamObserver<ProcessResponseResponse>() {
                    @Override
                    public void onNext(ProcessResponseResponse response) {
                        System.out.println(response.toString());
                        listener.onResponse(response);
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.out.println(t.getMessage());
                        listener.onFailure(new RuntimeException(t));
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("Finished Listening");
                    }
                });
                return null;
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private SearchResponse parseResponse(ProcessResponseResponse response, SearchResponse originalResponse) throws IOException {
        return ProtoTranslationUtils.SearchResponsePbToOs(response.getSearchResponse(), originalResponse);
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public String getProcessorName() {
        return this.processorName;
    }

    public static class Factory implements Processor.Factory<SearchResponseProcessor> {

        @Override
        public SearchResponseProcessor create(
            Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            String endpoint = getConfigParamAsString(config, ENDPOINT_FIELD);
            String processorName = getConfigParamAsString(config, PROCESSOR_NAME_FIELD);
            return new RemoteSearchResponseProcessor(tag, description, ignoreFailure, endpoint, processorName);
        }

        private static String getConfigParamAsString(Map<String, Object> config, String key) throws IllegalArgumentException {
            Object obj = config.remove(key);
            if (obj == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Missing field %s", key));
            }
            if (!(obj instanceof String)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be of type string", key));
            }
            return (String) obj;
        }

    }

}
