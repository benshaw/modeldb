package ai.verta.modeldb;

import ai.verta.modeldb.blobs.BlobEquality;
import ai.verta.modeldb.blobs.BlobProtoEquality;
import ai.verta.modeldb.blobs.DiffAndMerge;
import ai.verta.modeldb.lineage.LineageServiceImplNegativeTest;
import ai.verta.modeldb.metadata.MetadataTest;
import ai.verta.modeldb.utils.ModelDBUtilsTest;
import ai.verta.modeldb.versioning.blob.visitors.ValidatorBlobDiffTest;
import ai.verta.modeldb.versioning.blob.visitors.ValidatorBlobTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  ProjectTest.class,
  ExperimentTest.class,
  ExperimentRunTest.class,
  CommentTest.class,
  HydratedServiceTest.class,
  DatasetTest.class,
  DatasetVersionTest.class,
  ModelDBUtilsTest.class,
  LineageTest.class,
  LineageServiceImplNegativeTest.class,
  FindProjectEntitiesTest.class,
  FindDatasetEntitiesTest.class,
  RepositoryTest.class,
  CommitTest.class,
  MetadataTest.class,
  DiffTest.class,
  BlobEquality.class,
  BlobProtoEquality.class,
  DiffAndMerge.class,
  ValidatorBlobTest.class,
  ValidatorBlobDiffTest.class,
  GlobalSharingTest.class
  //  ArtifactStoreTest.class
})
public class TestSequenceSuite {}
