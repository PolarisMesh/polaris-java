/*
 * Copyright 2002-2018 the original author or authors.
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

package com.tencent.polaris.factory.replace;

import com.tencent.polaris.factory.util.PropertyPlaceholderHelper;
import com.tencent.polaris.logging.LoggerFactory;
import org.slf4j.Logger;


/***
 * copy from https://github.com/spring-projects/spring-framework/blob/main/spring-core/src/main/java/org/springframework/util/SystemPropertyUtils.java
 */
public class SystemPropertyPlaceholderResolver implements PropertyPlaceholderHelper.PlaceholderResolver {

    private static final Logger logger = LoggerFactory.getLogger(PropertyPlaceholderHelper.class);

    @Override
    public String resolvePlaceholder(String placeholderName) {
        try {
            String propVal = System.getProperty(placeholderName);
            if (propVal == null) {
                // Fall back to searching the system environment.
                propVal = System.getenv(placeholderName);
            }
            return propVal;
        } catch (Throwable ex) {
            logger.error("Could not resolve placeholder '" + placeholderName + " as system property: " + ex);
            return null;
        }
    }
}
