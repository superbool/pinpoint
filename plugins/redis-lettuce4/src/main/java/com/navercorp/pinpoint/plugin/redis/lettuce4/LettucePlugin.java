/*
 * Copyright 2018 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.plugin.redis.lettuce4;

import com.navercorp.pinpoint.bootstrap.instrument.InstrumentClass;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentException;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentMethod;
import com.navercorp.pinpoint.bootstrap.instrument.Instrumentor;
import com.navercorp.pinpoint.bootstrap.instrument.MethodFilters;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformCallback;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplate;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplateAware;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPlugin;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPluginSetupContext;
import com.navercorp.pinpoint.plugin.redis.lettuce4.interceptor.AttachEndPointInterceptor;
import com.navercorp.pinpoint.plugin.redis.lettuce4.interceptor.LettuceMethodInterceptor;
import com.navercorp.pinpoint.plugin.redis.lettuce4.interceptor.RedisClientConstructorInterceptor;

import java.security.ProtectionDomain;

/**
 * @author jaehong.kim
 */
public class LettucePlugin implements ProfilerPlugin, TransformTemplateAware {
    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());

    private TransformTemplate transformTemplate;

    @Override
    public void setup(ProfilerPluginSetupContext context) {
        final LettucePluginConfig config = new LettucePluginConfig(context.getConfig());

        if (!config.isEnable()) {
            logger.info("{} disabled", this.getClass().getSimpleName());
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("{} version range=[4.4.0.Final], config={}", this.getClass().getSimpleName(), config);
        }

        // Set endpoint
        addRedisClient();

        // Attach endpoint
        addDefaultConnectionFuture();
        addStatefulRedisConnection();

        // Commands
        addRedisCommands(config);
    }

    private void addRedisClient() {
        transformTemplate.transform("com.lambdaworks.redis.RedisClient", RedisClientTransform.class);
    }

    public static class RedisClientTransform implements TransformCallback {
        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader classLoader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            final InstrumentClass target = instrumentor.getInstrumentClass(classLoader, className, classfileBuffer);
            target.addField(EndPointAccessor.class);

            // Set endpoint
            final InstrumentMethod constructor = target.getConstructor("com.lambdaworks.redis.resource.ClientResources", "com.lambdaworks.redis.RedisURI");
            if (constructor != null) {
                constructor.addInterceptor(RedisClientConstructorInterceptor.class);
            }

            // Attach endpoint
            for (InstrumentMethod method : target.getDeclaredMethods(MethodFilters.name("connect", "connectAsync", "connectPubSub", "connectPubSubAsync", "connectSentinel", "connectSentinelAsync"))) {
                method.addScopedInterceptor(AttachEndPointInterceptor.class, LettuceConstants.REDIS_SCOPE);
            }

            return target.toBytecode();
        }
    }

    private void addDefaultConnectionFuture() {
        transformTemplate.transform("com.lambdaworks.redis.DefaultConnectionFuture", DefaultConnectionFutureTransform.class);
    }

    public static class DefaultConnectionFutureTransform implements TransformCallback {
        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader classLoader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            final InstrumentClass target = instrumentor.getInstrumentClass(classLoader, className, classfileBuffer);
            target.addField(EndPointAccessor.class);

            // Attach endpoint
            for (InstrumentMethod method : target.getDeclaredMethods(MethodFilters.name("get", "join"))) {
                method.addScopedInterceptor(AttachEndPointInterceptor.class, LettuceConstants.REDIS_SCOPE);
            }

            return target.toBytecode();
        }
    }

    private void addStatefulRedisConnection() {
        addStatefulRedisConnection("com.lambdaworks.redis.StatefulRedisConnectionImpl");
        addStatefulRedisConnection("com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnectionImpl");
        addStatefulRedisConnection("com.lambdaworks.redis.cluster.StatefulRedisClusterPubSubConnectionImpl");
        addStatefulRedisConnection("com.lambdaworks.redis.masterslave.StatefulRedisMasterSlaveConnectionImpl");
        addStatefulRedisConnection("com.lambdaworks.redis.sentinel.StatefulRedisSentinelConnectionImpl");
    }

    private void addStatefulRedisConnection(final String className) {
        transformTemplate.transform(className, AddEndPointAccessorTransform.class);
    }

    public static class AddEndPointAccessorTransform implements TransformCallback {
        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader classLoader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            final InstrumentClass target = instrumentor.getInstrumentClass(classLoader, className, classfileBuffer);
            target.addField(EndPointAccessor.class);
            return target.toBytecode();
        }
    }

    private void addRedisCommands(final LettucePluginConfig config) {
        // Commands
        addAbstractRedisCommands("com.lambdaworks.redis.AbstractRedisAsyncCommands", AbstractRedisCommandsTransform.class, true);

        addAbstractRedisCommands("com.lambdaworks.redis.RedisAsyncCommandsImpl", AbstractRedisCommandsTransform.class, false);
        addAbstractRedisCommands("com.lambdaworks.redis.cluster.RedisAdvancedClusterAsyncCommandsImpl", AbstractRedisCommandsTransform.class, false);
        addAbstractRedisCommands("com.lambdaworks.redis.cluster.RedisClusterPubSubAsyncCommandsImpl", AbstractRedisCommandsTransform.class, false);
        addAbstractRedisCommands("com.lambdaworks.redis.pubsub.RedisPubSubAsyncCommandsImpl", AbstractRedisCommandsTransform.class, false);

        // Reactive
        addAbstractRedisCommands("com.lambdaworks.redis.AbstractRedisReactiveCommands", AbstractRedisCommandsTransform.class, true);

        addAbstractRedisCommands("com.lambdaworks.redis.cluster.RedisAdvancedClusterReactiveCommandsImpl", AbstractRedisCommandsTransform.class, false);
        addAbstractRedisCommands("com.lambdaworks.redis.cluster.RedisClusterPubSubReactiveCommandsImpl", AbstractRedisCommandsTransform.class, false);
        addAbstractRedisCommands("com.lambdaworks.redis.pubsub.RedisPubSubReactiveCommandsImpl", AbstractRedisCommandsTransform.class, false);
        addAbstractRedisCommands("com.lambdaworks.redis.RedisReactiveCommandsImpl", AbstractRedisCommandsTransform.class, false);
        addAbstractRedisCommands("com.lambdaworks.redis.sentinel.RedisSentinelReactiveCommandsImpl", AbstractRedisCommandsTransform.class, false);
    }

    private void addAbstractRedisCommands(final String className, Class<? extends TransformCallback> transformCallback, boolean getter) {
        transformTemplate.transform(className, transformCallback, new Object[]{getter}, new Class[] {boolean.class});
    }

    public static class AbstractRedisCommandsTransform implements TransformCallback {
        private final boolean getter;

        public AbstractRedisCommandsTransform(boolean getter) {
            this.getter = getter;
        }

        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader classLoader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            final InstrumentClass target = instrumentor.getInstrumentClass(classLoader, className, classfileBuffer);

            if (getter) {
                target.addGetter(StatefulConnectionGetter.class, "connection");
            }
            final LettuceMethodNameFilter lettuceMethodNameFilter = new LettuceMethodNameFilter();
            for (InstrumentMethod method : target.getDeclaredMethods(MethodFilters.chain(lettuceMethodNameFilter, MethodFilters.modifierNot(MethodFilters.SYNTHETIC)))) {
                try {
                    method.addScopedInterceptor(LettuceMethodInterceptor.class, LettuceConstants.REDIS_SCOPE);
                } catch (Exception e) {
                    final PLogger logger = PLoggerFactory.getLogger(this.getClass());
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unsupported method {}", method, e);
                    }
                }
            }
            return target.toBytecode();
        }
    }

    @Override
    public void setTransformTemplate(TransformTemplate transformTemplate) {
        this.transformTemplate = transformTemplate;
    }
}