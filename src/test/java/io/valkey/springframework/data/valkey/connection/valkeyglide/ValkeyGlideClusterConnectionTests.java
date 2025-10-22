// TODO: Temporarily commented out - will be fully implemented later
/*
 * Copyright 2011-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
package io.valkey.springframework.data.valkey.connection.valkeyglide;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import io.valkey.springframework.data.valkey.connection.ClusterTopology;
import io.valkey.springframework.data.valkey.connection.ValkeyClusterConnection;
import io.valkey.springframework.data.valkey.connection.ValkeyClusterNode;
import io.valkey.springframework.data.valkey.connection.ValkeyConnectionFactory;
import io.valkey.springframework.data.valkey.connection.ValkeyNode.NodeType;

/**
 * Integration tests for {@link ValkeyGlideClusterConnection}.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
/*
public class ValkeyGlideClusterConnectionTests {

    ValkeyClusterConnection connection;
    ValkeyGlideConnectionFactory connectionFactory;

    @BeforeEach
    void setUp() throws Exception {
        // Setup will be extended as we implement more tests
        // For now, we only test the basic structure to ensure compilation and proper connection
        connectionFactory = new ValkeyGlideConnectionFactory(DefaultValkeyGlideClientConfiguration.builder().build());
    }

    @AfterEach
    void tearDown() {
        if (connection != null) {
            connection.close();
        }
        
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void testClusterGetNodes() {
        // This test can be enabled when working with a real cluster
        // It simply verifies that the connection can get cluster nodes information
        
        // Skip the test for now as we don't have a cluster setup in CI
        // When implementing a real cluster test, uncomment and adapt the assertions below
        
        /*
        connection = connectionFactory.getClusterConnection();
        
        Iterable<ValkeyClusterNode> nodes = connection.clusterGetNodes();
        
        assertThat(nodes).isNotNull();
        assertThat(nodes).isNotEmpty();
        
        for (ValkeyClusterNode node : nodes) {
            assertThat(node.getHost()).isNotNull();
            assertThat(node.getPort()).isGreaterThan(0);
        }
        *//*
    }

    @Test
    void testClusterGetSlaves() {
        // This test can be enabled when working with a real cluster
        // It verifies that the connection can get slave nodes information for a master
        
        // Skip the test for now as we don't have a cluster setup in CI
        // When implementing a real cluster test, uncomment and adapt the assertions below
        
        /*
        connection = connectionFactory.getClusterConnection();
        
        Iterable<ValkeyClusterNode> nodes = connection.clusterGetNodes();
        ValkeyClusterNode master = null;
        
        for (ValkeyClusterNode node : nodes) {
            if (node.getType() == NodeType.MASTER) {
                master = node;
                break;
            }
        }
        
        assumeThat(master).isNotNull();
        
        Set<ValkeyClusterNode> slaves = connection.clusterGetSlaves(master);
        
        assertThat(slaves).isNotNull();
        *//*
    }
}
*/
