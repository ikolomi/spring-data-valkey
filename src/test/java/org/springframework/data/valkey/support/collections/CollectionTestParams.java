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
package org.springframework.data.valkey.support.collections;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.valkey.DoubleAsStringObjectFactory;
import org.springframework.data.valkey.ObjectFactory;
import org.springframework.data.valkey.Person;
import org.springframework.data.valkey.PersonObjectFactory;
import org.springframework.data.valkey.RawObjectFactory;
import org.springframework.data.valkey.StringObjectFactory;
import org.springframework.data.valkey.connection.jedis.JedisConnectionFactory;
import org.springframework.data.valkey.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.valkey.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.valkey.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.valkey.core.ValkeyTemplate;
import org.springframework.data.valkey.core.StringValkeyTemplate;
import org.springframework.data.valkey.serializer.Jackson2JsonValkeySerializer;
import org.springframework.data.valkey.serializer.OxmSerializer;
import org.springframework.data.valkey.serializer.StringValkeySerializer;
import org.springframework.data.valkey.test.XstreamOxmSerializerSingleton;
import org.springframework.data.valkey.test.extension.ValkeyStanalone;

/**
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public abstract class CollectionTestParams {

	public static Collection<Object[]> testParams() {

		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonValkeySerializer<Person> jackson2JsonSerializer = new Jackson2JsonValkeySerializer<>(Person.class);
		StringValkeySerializer stringSerializer = StringValkeySerializer.UTF_8;

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<String> doubleAsStringObjectFactory = new DoubleAsStringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		ValkeyTemplate<String, String> stringTemplate = new StringValkeyTemplate(jedisConnFactory);
		ValkeyTemplate<String, Person> personTemplate = new ValkeyTemplate<>();
		personTemplate.setConnectionFactory(jedisConnFactory);
		personTemplate.afterPropertiesSet();

		ValkeyTemplate<String, String> xstreamStringTemplate = new ValkeyTemplate<>();
		xstreamStringTemplate.setConnectionFactory(jedisConnFactory);
		xstreamStringTemplate.setDefaultSerializer(serializer);
		xstreamStringTemplate.afterPropertiesSet();

		ValkeyTemplate<String, Person> xstreamPersonTemplate = new ValkeyTemplate<>();
		xstreamPersonTemplate.setConnectionFactory(jedisConnFactory);
		xstreamPersonTemplate.setValueSerializer(serializer);
		xstreamPersonTemplate.afterPropertiesSet();

		// jackson2
		ValkeyTemplate<String, Person> jackson2JsonPersonTemplate = new ValkeyTemplate<>();
		jackson2JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		ValkeyTemplate<byte[], byte[]> rawTemplate = new ValkeyTemplate<>();
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setKeySerializer(stringSerializer);
		rawTemplate.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(ValkeyStanalone.class);

		ValkeyTemplate<String, String> stringTemplateLtc = new StringValkeyTemplate(lettuceConnFactory);
		ValkeyTemplate<String, Person> personTemplateLtc = new ValkeyTemplate<>();
		personTemplateLtc.setConnectionFactory(lettuceConnFactory);
		personTemplateLtc.afterPropertiesSet();

		ValkeyTemplate<String, Person> xstreamStringTemplateLtc = new ValkeyTemplate<>();
		xstreamStringTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xstreamStringTemplateLtc.setDefaultSerializer(serializer);
		xstreamStringTemplateLtc.afterPropertiesSet();

		ValkeyTemplate<String, Person> xstreamPersonTemplateLtc = new ValkeyTemplate<>();
		xstreamPersonTemplateLtc.setValueSerializer(serializer);
		xstreamPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xstreamPersonTemplateLtc.afterPropertiesSet();

		ValkeyTemplate<String, Person> jackson2JsonPersonTemplateLtc = new ValkeyTemplate<>();
		jackson2JsonPersonTemplateLtc.setValueSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLtc.afterPropertiesSet();

		ValkeyTemplate<byte[], byte[]> rawTemplateLtc = new ValkeyTemplate<>();
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setKeySerializer(stringSerializer);
		rawTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringTemplate }, //
				{ doubleAsStringObjectFactory, stringTemplate }, //
				{ personFactory, personTemplate }, //
				{ stringFactory, xstreamStringTemplate }, //
				{ personFactory, xstreamPersonTemplate }, //
				{ personFactory, jackson2JsonPersonTemplate }, //
				{ rawFactory, rawTemplate },

				// lettuce
				{ stringFactory, stringTemplateLtc }, //
				{ personFactory, personTemplateLtc }, //
				{ doubleAsStringObjectFactory, stringTemplateLtc }, //
				{ personFactory, personTemplateLtc }, //
				{ stringFactory, xstreamStringTemplateLtc }, //
				{ personFactory, xstreamPersonTemplateLtc }, //
				{ personFactory, jackson2JsonPersonTemplateLtc }, //
				{ rawFactory, rawTemplateLtc } });
	}
}
