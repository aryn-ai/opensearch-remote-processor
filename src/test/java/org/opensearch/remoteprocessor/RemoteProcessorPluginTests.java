/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remoteprocessor;

import java.io.IOException;
import java.util.Map;

import org.opensearch.remoteprocessor.processors.RemoteSearchResponseProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

public class RemoteProcessorPluginTests extends OpenSearchTestCase {
    
    public void testPluginHasProcessor() throws IOException {
        RemoteProcessorPlugin plugin = new RemoteProcessorPlugin();
        Map<String, Processor.Factory<SearchResponseProcessor>> factories = plugin.getResponseProcessors(null);
        assert (factories.containsKey(RemoteSearchResponseProcessor.TYPE));
        assert (factories.get(RemoteSearchResponseProcessor.TYPE) instanceof RemoteSearchResponseProcessor.Factory);
        plugin.close();
    }
}
