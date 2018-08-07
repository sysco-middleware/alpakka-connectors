package no.sysco.middleware.alpakka.files.javadsl;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.javadsl.Sink;
import akka.stream.testkit.TestSubscriber;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class RecursiveDirectoryChangesSourceTest {

    private static final String BASEDIR = "target\\watch-dir";
    private static final Path BASE_PATH =FileSystems.getDefault().getPath(BASEDIR).toAbsolutePath();

    private static ActorSystem system;
    private static Materializer materializer;

    @BeforeClass
    public static void setupClass() {
        system = ActorSystem.create("test");
        materializer = ActorMaterializer.create(system);
    }

    @Before
    public void setup() throws IOException {
        //not really needed because every case cleans after itself, but better to check
        if(Files.exists(BASE_PATH)){
            Files.walk(BASE_PATH)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        Files.createDirectory(BASE_PATH);
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void sourceShouldEmitOnDirectoryChanges() throws Exception {
        final TestSubscriber.Probe<Pair<Path, DirectoryChange>> probe = TestSubscriber.probe(system);

        RecursiveDirectoryChangesSource.create(BASE_PATH, Duration.of(250,ChronoUnit.MILLIS), 200)
                .runWith(Sink.fromSubscriber(probe), materializer);

        probe.request(1);
        final Path createdFile = Files.createFile(BASE_PATH.resolve("test1file1.sample"));


        final Pair<Path, DirectoryChange> pair1 = probe.expectNext();
        assertEquals(DirectoryChange.Creation, pair1.second());
        assertEquals(createdFile, pair1.first());

        Files.delete(createdFile);

        TestSubscriber.Probe<Pair<Path, DirectoryChange>> probe1 = probe.request(10);
        final Pair<Path, DirectoryChange> pair5 = probe.requestNext();
        assertEquals(DirectoryChange.Deletion, pair5.second());
        assertEquals(createdFile, pair5.first());

        //a cleanup
        Files.delete(BASE_PATH);

        probe.cancel();
    }

    @Test
    public void sourceShouldEmitOnDirectoryChangesModifyFile() throws Exception {
        final TestSubscriber.Probe<Pair<Path, DirectoryChange>> probe = TestSubscriber.probe(system);

        RecursiveDirectoryChangesSource.create(BASE_PATH, Duration.of(250,ChronoUnit.MILLIS), 200)
                .runWith(Sink.fromSubscriber(probe), materializer);

        probe.request(1);
        final Path createdFile = Files.createFile(BASE_PATH.resolve("test1file1.sample"));


        final Pair<Path, DirectoryChange> pair1 = probe.expectNext();
        assertEquals(DirectoryChange.Creation, pair1.second());
        assertEquals(createdFile, pair1.first());

        Files.write(createdFile, "Some data".getBytes());

        //Expecting at least one modification. Note: there might be multiple events, depending on the OS
        final Pair<Path, DirectoryChange> pair2 = probe.requestNext();
        assertEquals(DirectoryChange.Modification, pair2.second());
        assertEquals(createdFile, pair2.first());

        probe.cancel();

        Files.delete(createdFile);
        //a cleanup
        Files.delete(BASE_PATH);

    }

    @Test
    public void sourceShouldEmitOnSubdirectoryChanges() throws IOException {
        final TestSubscriber.Probe<Pair<Path, DirectoryChange>> probe = TestSubscriber.probe(system);

        RecursiveDirectoryChangesSource.create(BASE_PATH, Duration.of(250,ChronoUnit.MILLIS), 200)
                .runWith(Sink.fromSubscriber(probe), materializer);

        probe.request(1);

        final Path createdSubfolder = Files.createDirectory(BASE_PATH.resolve("subfolder1"));

        final Pair<Path, DirectoryChange> subDirCreatedPair = probe.expectNext();
        assertEquals(DirectoryChange.Creation, subDirCreatedPair.second());
        assertEquals(createdSubfolder, subDirCreatedPair.first());

        final Path createdFile = Files.createFile(createdSubfolder.resolve("test1file1.sample"));
        final Pair<Path, DirectoryChange> fileCreatedPair = probe.requestNext();
        assertEquals(DirectoryChange.Creation, fileCreatedPair.second());
        assertEquals(createdFile, fileCreatedPair.first());

        Files.delete(createdFile);

        final Pair<Path, DirectoryChange> fileDeletedPair = probe.requestNext();
        assertEquals(DirectoryChange.Deletion, fileDeletedPair.second());
        assertEquals(createdFile, fileDeletedPair.first());

        Files.delete(createdSubfolder);

        Pair<Path, DirectoryChange> folderDeletedPair = probe.requestNext();
        if (folderDeletedPair.second().equals(DirectoryChange.Modification)){
            //windows sometimes emits modify event before delete
            folderDeletedPair = probe.requestNext();
        }
        assertEquals(DirectoryChange.Deletion, folderDeletedPair.second());
        assertEquals(createdSubfolder, folderDeletedPair.first());

        //a cleanup
        Files.delete(BASE_PATH);

        probe.cancel();
    }

    @Test
    public void emitMultipleChanges() throws Exception {
        final TestSubscriber.Probe<Pair<Path, DirectoryChange>> probe = TestSubscriber.probe(system);

        final int numberOfChanges = 10;

        RecursiveDirectoryChangesSource.create(BASE_PATH, Duration.of(250,ChronoUnit.MILLIS), 200)
                .runWith(Sink.fromSubscriber(probe), materializer);

        probe.request(numberOfChanges);

        final int halfRequested = numberOfChanges / 2;
        final List<Path> files = new ArrayList<>();

        for (int i = 0; i < halfRequested; i++) {
            final Path file = Files.createFile(BASE_PATH.resolve("test2files" + i));
            files.add(file);
        }

        for (int i = 0; i < halfRequested; i++) {
            probe.expectNext();
        }

        for (int i = 0; i < halfRequested; i++) {
            Files.delete(files.get(i));
        }

        for (int i = 0; i < halfRequested; i++) {
            probe.expectNext();
        }

        //a cleanup
        Files.delete(BASE_PATH);
        probe.cancel();
    }

    @Test
    public void emitMultipleChangesInSubfolders() throws Exception {
        final TestSubscriber.Probe<Pair<Path, DirectoryChange>> probe = TestSubscriber.probe(system);

        final int numberOfChanges = 10;

        RecursiveDirectoryChangesSource.create(BASE_PATH, Duration.of(250,ChronoUnit.MILLIS), 200)
                .runWith(Sink.fromSubscriber(probe), materializer);

        probe.request(numberOfChanges);

        final int halfRequested = numberOfChanges / 2;

        final List<Path> folders = new ArrayList<>();
        Path parentFolder = BASE_PATH;
        for (int i = 0; i < halfRequested; i++) {
            final Path newFolder = Files.createDirectory(parentFolder.resolve("testFolder" + i));
            folders.add(newFolder);
            parentFolder = newFolder;
        }

        for (int i = 0; i < halfRequested; i++) {
            probe.expectNext();
        }

        //need to delete folders in reverse order
        for (int i = halfRequested - 1; i >= 0; i--) {
            Files.delete(folders.get(i));
        }

        for (int i = halfRequested - 1; i >= 0; i--) {
            probe.expectNext();
        }

        //a cleanup
        Files.delete(BASE_PATH);

        probe.cancel();
    }
}

