package no.sysco.middleware.alpakka.files.javadsl;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.javadsl.Source;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.TimerGraphStageLogic;
import io.methvin.watcher.DirectoryChangeEvent;
import io.methvin.watcher.DirectoryChangeListener;
import io.methvin.watcher.DirectoryWatcher;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * RecursiveDirectoryChangesSource will emit elements every time there is a change to a watched directory in the local
 * file system or any of the subdirectories (new or existing). The enumeration consists of the path that was changed
 * and an enumeration describing what kind of change it was.
 */
public class RecursiveDirectoryChangesSource {

    public static Source<Pair<Path, DirectoryChange>, NotUsed> create(Path directoryPath, Duration pollInterval, int maxBufferSize) {
        return Source.fromGraph(new DirectoryWatcherSourceStage(directoryPath, pollInterval, maxBufferSize));
    }

    static class DirectoryWatcherSourceStage extends GraphStage<SourceShape<Pair<Path, DirectoryChange>>> {

        private final Path directoryPath;
        private final Duration pollInterval;
        private final int maxBufferSize;

        private final Outlet<Pair<Path, DirectoryChange>> outlet = Outlet.create("RecursiveDirectoryChangesSource.out");

        private final SourceShape<Pair<Path, DirectoryChange>> shape =
                new SourceShape<>(outlet);

        DirectoryWatcherSourceStage(Path directoryPath, Duration pollInterval, int maxBufferSize) {
            this.directoryPath = directoryPath;
            this.pollInterval = pollInterval;
            this.maxBufferSize = maxBufferSize;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
            return new TimerGraphStageLogic(shape) {
                private final Queue<Pair<Path, DirectoryChange>> buffer = new ArrayDeque<>();
                private final DirectoryWatcher watcher = DirectoryWatcher.builder()
                    .path(directoryPath)
                    .listener(getDirectoryChangeListener())
                    .fileHashing(false)
                    .build();

                private DirectoryChangeListener getDirectoryChangeListener() {
                    return new DirectoryChangeListener() {

                        @Override
                        public void onException(final Exception e) {
                            try {
                                watcher.close();
                            } catch (IOException e1) {}
                            failStage(e);
                        }

                        @Override
                        public void onEvent(final DirectoryChangeEvent event) {
                            if (DirectoryChangeEvent.EventType.OVERFLOW.equals(event.eventType())) {
                                throw new RuntimeException("Overflow from watch service: '" + directoryPath + "'");
                            } else {
                                final Path path = event.path();
                                final Path absolutePath = directoryPath.resolve(path);
                                final DirectoryChangeEvent.EventType change = event.eventType();

                                buffer.add(Pair.create(absolutePath, mapChange(change)));
                                if (buffer.size() > maxBufferSize) {
                                    throw new RuntimeException("Max event buffer size " + maxBufferSize + " reached for path: " + absolutePath);
                                }
                            }
                        }
                    };
                }

                {
                    setHandler(outlet, new AbstractOutHandler() {
                        @Override
                        public void onPull() throws Exception {
                            if (!buffer.isEmpty()) {
                                pushHead();
                            } else {
                                schedulePoll();
                            }
                        }

                    });
                }

                @Override
                public void onTimer(Object timerKey) throws Exception {
                    if (!isClosed(outlet)) {
                        if (!buffer.isEmpty()) {
                            pushHead();
                        } else {
                            schedulePoll();
                        }
                    }
                }

                @Override
                public void preStart() throws Exception {
                    watcher.watchAsync();
                }

                @Override
                public void postStop() throws Exception {
                    watcher.close();
                }

                private void pushHead() {
                    final Pair<Path, DirectoryChange> head = buffer.poll();
                    if (head != null) {
                        push(outlet, head);
                    }
                }

                private void schedulePoll() {
                    scheduleOnce("poll", pollInterval);
                }

                private DirectoryChange mapChange(DirectoryChangeEvent.EventType change) {
                    switch (change) {
                        case CREATE:
                            return DirectoryChange.Creation;
                        case DELETE:
                            return DirectoryChange.Deletion;
                        case MODIFY:
                            return DirectoryChange.Modification;
                        default:
                            throw new IllegalStateException("This " + change + " is not supported");
                    }
                }
            };
        }

        @Override
        public SourceShape<Pair<Path, DirectoryChange>> shape() {
            return shape;
        }
    }
}
