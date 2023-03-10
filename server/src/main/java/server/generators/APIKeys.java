/*
 * Copyright 2019-2023 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server.generators;

import org.bouncycastle.crypto.generators.BCrypt;

import java.util.UUID;

public abstract class APIKeys {

    public static String generate() {
        // Stub
        return UUID.randomUUID().toString();
    }


    public static boolean validate( String apiKey ) {
        // Stub
        return true;
    }


}
