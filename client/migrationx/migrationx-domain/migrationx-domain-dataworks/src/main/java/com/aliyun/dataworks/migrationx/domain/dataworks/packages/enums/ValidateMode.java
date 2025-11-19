package com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums;

import lombok.Getter;

/**
 * Validation Mode Enumeration
 *
 * @author 莫泣
 * @date 2025-09-21
 */
@Getter
public enum ValidateMode {

    NORMAL("Normal"),
    PRE_VALIDATE("PreValidate"),
    ;

    ValidateMode(String label) {
        this.label = label;
    }

    private final String label;
}
