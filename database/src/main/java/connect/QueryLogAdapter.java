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

package connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

public class QueryLogAdapter {

    public QueryLogAdapter() {}

    public Optional<Connection> connect( String url, String user, String password ) {
        Properties props = new Properties();
        props.setProperty( "user", user );
        props.setProperty( "password", password );
        try {
            return Optional.of( DriverManager.getConnection( url, props ) );
        } catch ( SQLException e ) {
            throw new RuntimeException(e);
        }
    }

}
