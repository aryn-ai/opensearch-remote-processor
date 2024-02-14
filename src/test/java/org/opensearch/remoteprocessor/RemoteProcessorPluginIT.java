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
package org.opensearch.remoteprocessor;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class RemoteProcessorPluginIT extends OpenSearchRestTestCase {

    public void testPluginInstalled() throws IOException, ParseException {
        try(RestClient client = client()) {
            Response response = client.performRequest(new Request("GET", "/_cat/plugins"));
            String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

            logger.info("response body: {}", body);
            org.hamcrest.MatcherAssert.assertThat("did not contain plugin", body, containsString("remote-processor"));
        }
    }
}
