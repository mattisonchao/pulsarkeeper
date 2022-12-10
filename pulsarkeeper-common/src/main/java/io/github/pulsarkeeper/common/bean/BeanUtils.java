package io.github.pulsarkeeper.common.bean;

import java.beans.FeatureDescriptor;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.BeanWrapperImpl;

public class BeanUtils extends org.springframework.beans.BeanUtils {

    public static void copyPropertiesIgnoreNull(Object source, Object target) {
        BeanWrapperImpl beanWrapper = new BeanWrapperImpl(source);
        List<String> ignoreProperties = Arrays.stream(beanWrapper.getPropertyDescriptors())
                .filter(descriptor -> Objects.isNull(beanWrapper.getPropertyValue(descriptor.getName())))
                .map(FeatureDescriptor::getName)
                .collect(Collectors.toList());
        org.springframework.beans.BeanUtils.copyProperties(source, target, ignoreProperties.toArray(String[]::new));
    }

}
