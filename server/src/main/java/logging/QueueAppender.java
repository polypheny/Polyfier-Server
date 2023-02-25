package logging;

import com.google.gson.*;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Plugin(
        name = "QueueAppender",
        category = Core.CATEGORY_NAME,
        elementType = Appender.ELEMENT_TYPE)
public class QueueAppender extends AbstractAppender {
    Gson gson;

    private final ConcurrentLinkedDeque<String> events = new ConcurrentLinkedDeque<>();

    protected QueueAppender(String name, @Nullable  Filter filter, @Nullable Layout<? extends Serializable> layout ) {
        super(name, filter, layout);
        gson = new Gson();
    }

    @PluginFactory
    public static QueueAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Layout") @Nullable Layout<? extends Serializable> layout ) {
        return new QueueAppender(name, filter, layout);
    }

    @Override
    public void append(LogEvent event) {
        events.add( new String( getLayout().toByteArray( event ) ) );
    }

    public Optional<String> parse() {
        synchronized ( events ) {
            if ( events.isEmpty() ) {
                return Optional.empty();
            }

            final StringBuilder stringBuilder = new StringBuilder();

            events.forEach(event -> {
                stringBuilder.append(event);
            } );

            events.clear();

            return Optional.of( stringBuilder.toString() );
        }
    }


}
