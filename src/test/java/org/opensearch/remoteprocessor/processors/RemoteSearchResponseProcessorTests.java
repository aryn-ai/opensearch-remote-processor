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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

public class RemoteSearchResponseProcessorTests extends OpenSearchTestCase {

    private RemoteSearchResponseProcessor.Factory factory;

    @Before
    public void setup() {
        this.factory = new RemoteSearchResponseProcessor.Factory();
    }

    public void testCreateProcessor_HappyPath() throws Exception {
        SearchResponseProcessor processor = factory
            .create(
                Map.of(),
                "remote",
                "processor for RPCs",
                false,
                new HashMap<>(
                    Map
                        .of(
                            RemoteSearchResponseProcessor.ENDPOINT_FIELD,
                            "rps:2796/RemoteProcessorService/ProcessResponse",
                            RemoteSearchResponseProcessor.PROCESSOR_NAME_FIELD,
                            "debug"
                        )
                ),
                null
            );
        assert (processor instanceof RemoteSearchResponseProcessor);
        RemoteSearchResponseProcessor remoteProcessor = (RemoteSearchResponseProcessor) processor;
        assert (remoteProcessor.getDescription().equals("processor for RPCs"));
        assert (remoteProcessor.getTag().equals("remote"));
        assert (remoteProcessor.getEndpoint().equals("rps:2796/RemoteProcessorService/ProcessResponse"));
        assert (remoteProcessor.getProcessorName().equals("debug"));
    }

    public void testCreateProcessor_NoEndpoint_ThenFail() {
        assertThrows(
            String.format(Locale.ROOT, "Missing field %s", RemoteSearchResponseProcessor.ENDPOINT_FIELD),
            IllegalArgumentException.class,
            () -> {
                factory
                    .create(
                        Map.of(),
                        "remote",
                        "processor for RPCs",
                        false,
                        new HashMap<>(Map.of(RemoteSearchResponseProcessor.PROCESSOR_NAME_FIELD, "debug")),
                        null
                    );
            }
        );
    }

    public void testCreateProcessor_BadEndpoint_ThenFail() {
        assertThrows(
            String.format(Locale.ROOT, "%s must be of type string", RemoteSearchResponseProcessor.ENDPOINT_FIELD),
            IllegalArgumentException.class,
            () -> {
                factory
                    .create(
                        Map.of(),
                        "remote",
                        "processor for RPCs",
                        false,
                        new HashMap<>(
                            Map
                                .of(
                                    RemoteSearchResponseProcessor.ENDPOINT_FIELD,
                                    2796,
                                    RemoteSearchResponseProcessor.PROCESSOR_NAME_FIELD,
                                    "debug"
                                )
                        ),
                        null
                    );
            }
        );
    }

    public void testCreateProcessor_NoProcessorName_ThenFail() {
        assertThrows(
            String.format(Locale.ROOT, "Missing field %s", RemoteSearchResponseProcessor.PROCESSOR_NAME_FIELD),
            IllegalArgumentException.class,
            () -> {
                factory
                    .create(
                        Map.of(),
                        "remote",
                        "processor for RPCs",
                        false,
                        new HashMap<>(
                            Map.of(RemoteSearchResponseProcessor.ENDPOINT_FIELD, "rps:2796/RemoteProcessorService/ProcessResponse")
                        ),
                        null
                    );
            }
        );
    }

    public void testCreateProcessor_BadProcessorName_ThenFail() {
        assertThrows(
            String.format(Locale.ROOT, "%s must be of type string", RemoteSearchResponseProcessor.PROCESSOR_NAME_FIELD),
            IllegalArgumentException.class,
            () -> {
                factory
                    .create(
                        Map.of(),
                        "remote",
                        "processor for RPCs",
                        false,
                        new HashMap<>(
                            Map
                                .of(
                                    RemoteSearchResponseProcessor.ENDPOINT_FIELD,
                                    "rps:2796/RemoteProcessorService/ProcessResponse",
                                    RemoteSearchResponseProcessor.PROCESSOR_NAME_FIELD,
                                    1532
                                )
                        ),
                        null
                    );
            }
        );
    }

    // Unfortunately it seems like the only way to test the grpc stuff is via integ tests. So that'll happen there.

}
