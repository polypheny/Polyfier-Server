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

package logging;

import com.google.gson.Gson;
import io.javalin.websocket.WsContext;
import lombok.AllArgsConstructor;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArraySet;

@Plugin(name = "WebSocket", category = "Core", elementType = "appender", printObject = true)
public class WebSocketAppender extends AbstractAppender {
    private static final CopyOnWriteArraySet<WsContext> sessions = new CopyOnWriteArraySet<>();
    private static final LinkedList<WsContext> toRemove = new LinkedList<>();

    private WebSocketAppender( String name, @Nullable Filter filter, @Nullable PatternLayout patternLayout ) {
        super( name, filter, patternLayout, false, null );
    }

    public static void addSession( WsContext session ) {
        if ( sessions.contains( session ) ) {
            return;
        }
        sessions.add( session );
    }

    public static void removeSession(WsContext session) {
        sessions.remove( session );
    }

    @Override
    public void append(LogEvent event) {
        final String logMessage = ( (PatternLayout) getLayout() ).toSerializable( event );
        for (WsContext session : sessions) {
            try {
                session.send(new Gson().toJson(new LogMessage(logMessage)));
            } catch ( Exception exception ) {
                toRemove.add( session );
            }
        }
        if ( ! toRemove.isEmpty() ) {
            toRemove.forEach( sessions::remove );
            toRemove.clear();
        }
    }

    @SuppressWarnings("unused")
    @PluginFactory
    public static WebSocketAppender createAppender( @PluginAttribute("name") String name ) {
        return new WebSocketAppender( name, null, PatternLayout.newBuilder().withPattern("D [%-6p] %c{3} - %m%n").build() );
    }

    @AllArgsConstructor
    private static class LogMessage {
        private final String log;
    }

}
