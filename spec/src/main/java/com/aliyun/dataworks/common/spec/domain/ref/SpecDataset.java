/*
 * Copyright (c) 2025, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.common.spec.domain.ref;

import com.aliyun.dataworks.common.spec.domain.SpecNoRefEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Dataset define
 *
 * @author jiemi
 * @date 2025/06/17
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class SpecDataset extends SpecNoRefEntity {
    /**
     * identifier
     */
    private String identifier;
    /**
     * version
     */
    private Integer version;
    /**
     * Path within the container at which the volume should be mounted
     */
    private String mountPath;
    /**
     * Mounted read-only if true, read-write otherwise (false or unspecified). Defaults to false.
     */
    private Boolean readOnly;
    /**
     * Extra options
     */
    private String extOptions;
}