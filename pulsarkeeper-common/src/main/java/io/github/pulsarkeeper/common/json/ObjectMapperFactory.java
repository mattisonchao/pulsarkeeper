package io.github.pulsarkeeper.common.json;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectMapperFactory {

    public static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper();
        // forward compatibility for the properties may go away in the future
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.findAndRegisterModules();
        return mapper;
    }

    public static ObjectMapper createYaml() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        // forward compatibility for the properties may go away in the future
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.findAndRegisterModules();
        return mapper;
    }

    private static final FastThreadLocal<ObjectMapper> JSON_MAPPER = new FastThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() throws Exception {
            return create();
        }
    };

    private static final FastThreadLocal<ObjectMapper> YAML_MAPPER = new FastThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() throws Exception {
            return createYaml();
        }
    };

    public static ObjectMapper getThreadLocal() {
        return JSON_MAPPER.get();
    }

    public static ObjectMapper getThreadLocalYaml() {
        return YAML_MAPPER.get();
    }
}
