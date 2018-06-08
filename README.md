# alpakka-connectors

## File connectors

### Listening to changes in a directory
The RecursiveDirectoryChangesSource tries to improve the existing DirectoryChangesSource from alpakka with recursive folder monitoring. It will emit elements every time there is a change to a watched directory in the local file system or any of the subdirectories (new or existing). The enumeration consists of the path that was changed and an enumeration describing what kind of change it was.

```
import no.sysco.middleware.alpakka.files.javadsl.RecursiveDirectoryChangesSource;

final FileSystem fs = FileSystems.getDefault();
final Duration pollingInterval = Duration.of(1, ChronoUnit.SECONDS);
final int maxBufferSize = 1000;
final Source<Pair<Path, DirectoryChange>, NotUsed> changes =
  RecursiveDirectoryChangesSource.create(fs.getPath(path), pollingInterval, maxBufferSize);


changes.runForeach((Pair<Path, DirectoryChange> pair) -> {
  final Path changedPath = pair.first();
  final DirectoryChange change = pair.second();
  System.out.println("Path: " + changedPath + ", Change: " + change);
}, materializer);
```