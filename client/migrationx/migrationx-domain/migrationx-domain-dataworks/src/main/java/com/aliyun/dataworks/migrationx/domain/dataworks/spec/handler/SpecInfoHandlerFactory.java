package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.SetUtils;
import org.reflections.Reflections;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
@Slf4j
public class SpecInfoHandlerFactory {

    private static final SpecInfoHandlerFactory INSTANCE = new SpecInfoHandlerFactory();

    private List<SpecExtractInfoHandler> handlers;

    private SpecInfoHandlerFactory() {

    }

    public static SpecInfoHandlerFactory getInstance() {
        return INSTANCE;
    }

    private synchronized void init() {
        // check handlers empty twice
        if (CollectionUtils.isNotEmpty(handlers)) {
            return;
        }
        log.info("start init spec info handler factory by thread: {}", Thread.currentThread().getName());
        List<Class<? extends SpecExtractInfoHandler>> declaredClasses = getHandlerClasses();

        List<SpecExtractInfoHandler> list = Lists.newArrayList();
        for (Class<?> declaredClass : declaredClasses) {
            if (SpecExtractInfoHandler.class.isAssignableFrom(declaredClass) && !SpecExtractInfoHandler.class.equals(declaredClass)) {
                try {
                    SpecExtractInfoHandler handler = (SpecExtractInfoHandler)declaredClass.getDeclaredConstructor().newInstance();
                    list.add(handler);
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    list.clear();
                    log.error("init spec info handler factory error", e);
                    throw new RuntimeException(e);
                }
            }
        }
        handlers = ListUtils.unmodifiableList(list);
        log.info("end init spec info handler factory, support classes: {}", handlers);
    }

    public SpecExtractInfoHandler getHandler(Specification<DataWorksWorkflowSpec> specification) {
        if (CollectionUtils.isEmpty(handlers)) {
            init();
        }
        SpecExtractInfoHandler result = ListUtils.emptyIfNull(handlers).stream().filter(handler -> handler.support(specification))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("unsupported specification"));
        log.info("get handler: {}, specKind: {}", result.getClass(), specification.getKind());
        return result;
    }

    public List<SpecExtractInfoHandler> getHandlers() {
        if (CollectionUtils.isEmpty(handlers)) {
            init();
        }
        return handlers;
    }

    private List<Class<? extends SpecExtractInfoHandler>> getHandlerClasses() {
        Reflections reflections = new Reflections(SpecExtractInfoHandler.class.getPackage().getName());
        Set<Class<? extends SpecExtractInfoHandler>> clzSet = reflections.getSubTypesOf(SpecExtractInfoHandler.class);
        return SetUtils.emptyIfNull(clzSet).stream()
            .filter(SpecExtractInfoHandler.class::isAssignableFrom)
            .filter(clazz -> !(Modifier.isAbstract(clazz.getModifiers()) || Modifier.isInterface(clazz.getModifiers())))
            .collect(Collectors.toList());
    }

}
