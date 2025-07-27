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
package org.springframework.data.redis.connection.valkeyglide;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisScriptingCommands} for Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideScriptingCommands implements RedisScriptingCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideScriptingCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideScriptingCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        Assert.notNull(script, "Script must not be null!");
        Assert.notNull(returnType, "ReturnType must not be null!");
        Assert.notNull(keysAndArgs, "Keys and args must not be null!");
        
        Object[] args = new Object[2 + 1 + keysAndArgs.length];
        args[0] = script;
        args[1] = String.valueOf(numKeys);
        System.arraycopy(keysAndArgs, 0, args, 2, keysAndArgs.length);

        return convertResult(connection.execute("EVAL", args), returnType);
    }

    @Override
    public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        Assert.notNull(scriptSha1, "Script SHA1 must not be null!");
        Assert.notNull(returnType, "ReturnType must not be null!");
        Assert.notNull(keysAndArgs, "Keys and args must not be null!");
        
        Object[] args = new Object[2 + 1 + keysAndArgs.length];
        args[0] = scriptSha1;
        args[1] = String.valueOf(numKeys);
        System.arraycopy(keysAndArgs, 0, args, 2, keysAndArgs.length);

        return convertResult(connection.execute("EVALSHA", args), returnType);
    }

    @Override
    public <T> T evalSha(byte[] scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
        return evalSha(new String(scriptSha1), returnType, numKeys, keysAndArgs);
    }

    @Override
    public void scriptFlush() {
        connection.execute("SCRIPT", "FLUSH");
    }

    @Override
    public List<Boolean> scriptExists(String... scriptSha1s) {
        Assert.notNull(scriptSha1s, "Script SHA1s must not be null!");
        
        if (scriptSha1s.length == 0) {
            return Collections.emptyList();
        }

        Object[] args = new Object[1 + scriptSha1s.length];
        args[0] = "EXISTS";
        for (int i = 0; i < scriptSha1s.length; i++) {
            args[i + 1] = scriptSha1s[i];
        }

        List<Long> result = (List<Long>) connection.execute("SCRIPT", args);
        List<Boolean> exists = new ArrayList<>(result.size());
        for (Long value : result) {
            exists.add(value == 1);
        }
        return exists;
    }

    public List<Boolean> scriptExists(byte[]... scriptSha1s) {
        String[] sha1s = new String[scriptSha1s.length];
        for (int i = 0; i < scriptSha1s.length; i++) {
            sha1s[i] = new String(scriptSha1s[i]);
        }
        return scriptExists(sha1s);
    }

    @Override
    public String scriptLoad(byte[] script) {
        Assert.notNull(script, "Script must not be null!");

        return new String((byte[]) connection.execute("SCRIPT", "LOAD", script));
    }

    @Override
    public void scriptKill() {
        connection.execute("SCRIPT", "KILL");
    }
    
    @SuppressWarnings("unchecked")
    private <T> T convertResult(Object result, ReturnType returnType) {
        if (result == null) {
            return null;
        }
        
        switch (returnType) {
            case BOOLEAN:
                return (T) Boolean.valueOf(result.toString().equals("1"));
            case INTEGER:
                return (T) Long.valueOf(result.toString());
            case STATUS:
                return (T) result.toString();
            case MULTI:
                return (T) result;
            case VALUE:
                return (T) result;
            default:
                throw new IllegalArgumentException("Unsupported return type: " + returnType);
        }
    }
}
