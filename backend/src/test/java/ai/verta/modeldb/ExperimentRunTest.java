package ai.verta.modeldb;

import static ai.verta.modeldb.CollaboratorUtils.addCollaboratorRequestProjectInterceptor;
import static ai.verta.modeldb.RepositoryTest.createRepository;
import static org.junit.Assert.*;

import ai.verta.common.*;
import ai.verta.common.ArtifactTypeEnum.ArtifactType;
import ai.verta.common.CollaboratorTypeEnum.CollaboratorType;
import ai.verta.common.OperatorEnum.Operator;
import ai.verta.common.TernaryEnum.Ternary;
import ai.verta.common.ValueTypeEnum.ValueType;
import ai.verta.modeldb.GetExperimentRunById.Response;
import ai.verta.modeldb.common.exceptions.ModelDBException;
import ai.verta.modeldb.metadata.GenerateRandomNameRequest;
import ai.verta.modeldb.utils.ModelDBUtils;
import ai.verta.modeldb.versioning.Blob;
import ai.verta.modeldb.versioning.BlobExpanded;
import ai.verta.modeldb.versioning.CodeBlob;
import ai.verta.modeldb.versioning.Commit;
import ai.verta.modeldb.versioning.CreateCommitRequest;
import ai.verta.modeldb.versioning.DeleteCommitRequest;
import ai.verta.modeldb.versioning.DeleteRepositoryRequest;
import ai.verta.modeldb.versioning.EnvironmentBlob;
import ai.verta.modeldb.versioning.FileHasher;
import ai.verta.modeldb.versioning.GetBranchRequest;
import ai.verta.modeldb.versioning.GitCodeBlob;
import ai.verta.modeldb.versioning.PythonEnvironmentBlob;
import ai.verta.modeldb.versioning.PythonRequirementEnvironmentBlob;
import ai.verta.modeldb.versioning.RepositoryIdentification;
import ai.verta.modeldb.versioning.RepositoryNamedIdentification;
import ai.verta.modeldb.versioning.VersionEnvironmentBlob;
import ai.verta.uac.AddCollaboratorRequest;
import ai.verta.uac.CollaboratorPermissions;
import ai.verta.uac.GetUser;
import ai.verta.uac.UserInfo;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExperimentRunTest extends TestsInit {

  private static final Logger LOGGER = LogManager.getLogger(ExperimentRunTest.class);

  // Project Entities
  private static Project project;
  private static Project project2;
  private static Project project3;
  private static Map<String, Project> projectMap = new HashMap<>();

  // Experiment Entities
  private static Experiment experiment;
  private static Experiment experiment2;

  // ExperimentRun Entities
  private static ExperimentRun experimentRun;
  private static ExperimentRun experimentRun2;
  private static Map<String, ExperimentRun> experimentRunMap = new HashMap<>();

  @Before
  public void createEntities() {
    // Create all entities
    createProjectEntities();
    createExperimentEntities();
    createExperimentRunEntities();
  }

  @After
  public void removeEntities() {
    for (ExperimentRun run : new ExperimentRun[] {experimentRun, experimentRun2}) {
      DeleteExperimentRun deleteExperimentRun =
          DeleteExperimentRun.newBuilder().setId(run.getId()).build();
      DeleteExperimentRun.Response deleteExperimentRunResponse =
          experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
      assertTrue(deleteExperimentRunResponse.getStatus());
    }

    for (String projectId : projectMap.keySet()) {
      DeleteProject deleteProject = DeleteProject.newBuilder().setId(projectId).build();
      DeleteProject.Response deleteProjectResponse =
          projectServiceStub.deleteProject(deleteProject);
      LOGGER.info("Project deleted successfully");
      LOGGER.info(deleteProjectResponse.toString());
      assertTrue(deleteProjectResponse.getStatus());
    }

    project = null;
    project2 = null;
    project3 = null;
    projectMap = new HashMap<>();

    // Experiment Entities
    experiment = null;
    experiment2 = null;

    // ExperimentRun Entities
    experimentRun = null;
    experimentRun2 = null;

    experimentRunMap = new HashMap<>();
  }

  private static void createProjectEntities() {
    ProjectTest projectTest = new ProjectTest();

    // Create two project of above project
    CreateProject createProjectRequest =
        projectTest.getCreateProjectRequest("project-" + new Date().getTime());
    CreateProject.Response createProjectResponse =
        projectServiceStub.createProject(createProjectRequest);
    project = createProjectResponse.getProject();
    projectMap.put(project.getId(), project);
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected Project name",
        createProjectRequest.getName(),
        project.getName());

    // Create project2
    createProjectRequest = projectTest.getCreateProjectRequest("project-" + new Date().getTime());
    createProjectResponse = projectServiceStub.createProject(createProjectRequest);
    project2 = createProjectResponse.getProject();
    projectMap.put(project2.getId(), project2);
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected project name",
        createProjectRequest.getName(),
        project2.getName());

    // Create project3
    createProjectRequest = projectTest.getCreateProjectRequest("project-" + new Date().getTime());
    createProjectResponse = projectServiceStub.createProject(createProjectRequest);
    project3 = createProjectResponse.getProject();
    projectMap.put(project3.getId(), project3);
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected project name",
        createProjectRequest.getName(),
        project3.getName());
  }

  private static void createExperimentEntities() {

    // Create two experiment of above project
    CreateExperiment createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project.getId(), "Experiment-" + new Date().getTime());
    KeyValue attribute1 =
        KeyValue.newBuilder()
            .setKey("attribute_1")
            .setValue(Value.newBuilder().setNumberValue(0.012).build())
            .build();
    KeyValue attribute2 =
        KeyValue.newBuilder()
            .setKey("attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.99).build())
            .build();
    createExperimentRequest =
        createExperimentRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_2")
            .build();
    CreateExperiment.Response createExperimentResponse =
        experimentServiceStub.createExperiment(createExperimentRequest);
    experiment = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment.getName());

    // Create two experiment of above project
    createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project.getId(), "Experiment-" + new Date().getTime());
    createExperimentResponse = experimentServiceStub.createExperiment(createExperimentRequest);
    experiment2 = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment2.getName());
  }

  private static void createExperimentRunEntities() {

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequest(
            project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
    KeyValue metric1 =
        KeyValue.newBuilder()
            .setKey("loss")
            .setValue(Value.newBuilder().setNumberValue(0.31).build())
            .build();
    KeyValue metric2 =
        KeyValue.newBuilder()
            .setKey("accuracy")
            .setValue(Value.newBuilder().setNumberValue(0.31).build())
            .build();
    KeyValue hyperparameter1 =
        KeyValue.newBuilder()
            .setKey("tuning")
            .setValue(Value.newBuilder().setNumberValue(7).build())
            .build();
    createExperimentRunRequest =
        createExperimentRunRequest
            .toBuilder()
            .addMetrics(metric1)
            .addMetrics(metric2)
            .addHyperparameters(hyperparameter1)
            .build();
    CreateExperimentRun.Response createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    experimentRun = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun.getName());

    createExperimentRunRequest =
        getCreateExperimentRunRequest(
            project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
    metric1 =
        KeyValue.newBuilder()
            .setKey("loss")
            .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
            .build();
    metric2 =
        KeyValue.newBuilder()
            .setKey("accuracy")
            .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
            .build();
    hyperparameter1 =
        KeyValue.newBuilder()
            .setKey("tuning")
            .setValue(Value.newBuilder().setNumberValue(4.55).build())
            .build();
    createExperimentRunRequest =
        createExperimentRunRequest
            .toBuilder()
            .addMetrics(metric1)
            .addMetrics(metric2)
            .addHyperparameters(hyperparameter1)
            .build();
    createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    experimentRun2 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun2.getId(), experimentRun2);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun2.getName());
  }

  private void checkEqualsAssert(StatusRuntimeException e) {
    Status status = Status.fromThrowable(e);
    LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
    if (testConfig.hasAuth()) {
      assertTrue(
          Status.PERMISSION_DENIED.getCode() == status.getCode()
              || Status.NOT_FOUND.getCode()
                  == status.getCode()); // because of shadow delete the response could be 403 or 404
    } else {
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }
  }

  public CreateExperimentRun getCreateExperimentRunRequestSimple(
      String projectId, String experimentId, String experimentRunName) {
    return CreateExperimentRun.newBuilder()
        .setProjectId(projectId)
        .setExperimentId(experimentId)
        .setName(experimentRunName)
        .build();
  }

  public static CreateExperimentRun getCreateExperimentRunRequest(
      String projectId, String experimentId, String experimentRunName) {

    List<String> tags = new ArrayList<>();
    tags.add("Tag_x");
    tags.add("Tag_y");

    int rangeMax = 20;
    int rangeMin = 1;
    Random randomNum = new Random();

    List<KeyValue> attributeList = new ArrayList<>();
    Value intValue = Value.newBuilder().setNumberValue(1.1).build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_1_" + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build());
    Value stringValue =
        Value.newBuilder()
            .setStringValue("attributes_value_" + Calendar.getInstance().getTimeInMillis())
            .build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_2_" + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build());

    double randomValue = rangeMin + (rangeMax - rangeMin) * randomNum.nextDouble();
    List<KeyValue> hyperparameters = new ArrayList<>();
    intValue = Value.newBuilder().setNumberValue(randomValue).build();
    hyperparameters.add(
        KeyValue.newBuilder()
            .setKey("tuning_" + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build());
    stringValue =
        Value.newBuilder()
            .setStringValue("hyperparameters_value_" + Calendar.getInstance().getTimeInMillis())
            .build();
    hyperparameters.add(
        KeyValue.newBuilder()
            .setKey("hyperparameters_" + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build());

    List<Artifact> artifactList = new ArrayList<>();
    artifactList.add(
        Artifact.newBuilder()
            .setKey("Google developer Artifact")
            .setPath(
                "https://www.google.co.in/imgres?imgurl=https%3A%2F%2Flh3.googleusercontent.com%2FFyZA5SbKPJA7Y3XCeb9-uGwow8pugxj77Z1xvs8vFS6EI3FABZDCDtA9ScqzHKjhU8av_Ck95ET-P_rPJCbC2v_OswCN8A%3Ds688&imgrefurl=https%3A%2F%2Fdevelopers.google.com%2F&docid=1MVaWrOPIjYeJM&tbnid=I7xZkRN5m6_z-M%3A&vet=10ahUKEwjr1OiS0ufeAhWNbX0KHXpFAmQQMwhyKAMwAw..i&w=688&h=387&bih=657&biw=1366&q=google&ved=0ahUKEwjr1OiS0ufeAhWNbX0KHXpFAmQQMwhyKAMwAw&iact=mrc&uact=8")
            .setArtifactType(ArtifactType.BLOB)
            .setUploadCompleted(
                !testConfig.artifactStoreConfig.getArtifactStoreType().equals(ModelDBConstants.S3))
            .build());
    artifactList.add(
        Artifact.newBuilder()
            .setKey("Google Pay Artifact")
            .setPath(
                "https://www.google.co.in/imgres?imgurl=https%3A%2F%2Fpay.google.com%2Fabout%2Fstatic%2Fimages%2Fsocial%2Fknowledge_graph_logo.png&imgrefurl=https%3A%2F%2Fpay.google.com%2Fabout%2F&docid=zmoE9BrSKYr4xM&tbnid=eCL1Y6f9xrPtDM%3A&vet=10ahUKEwjr1OiS0ufeAhWNbX0KHXpFAmQQMwhwKAIwAg..i&w=1200&h=630&bih=657&biw=1366&q=google&ved=0ahUKEwjr1OiS0ufeAhWNbX0KHXpFAmQQMwhwKAIwAg&iact=mrc&uact=8")
            .setArtifactType(ArtifactType.IMAGE)
            .setUploadCompleted(
                !testConfig.artifactStoreConfig.getArtifactStoreType().equals(ModelDBConstants.S3))
            .build());

    List<Artifact> datasets = new ArrayList<>();
    datasets.add(
        Artifact.newBuilder()
            .setKey("Google developer datasets")
            .setPath("This is data artifact type in Google developer datasets")
            .setArtifactType(ArtifactType.MODEL)
            .setUploadCompleted(
                !testConfig.artifactStoreConfig.getArtifactStoreType().equals(ModelDBConstants.S3))
            .build());
    datasets.add(
        Artifact.newBuilder()
            .setKey("Google Pay datasets")
            .setPath("This is data artifact type in Google Pay datasets")
            .setArtifactType(ArtifactType.DATA)
            .setUploadCompleted(
                !testConfig.artifactStoreConfig.getArtifactStoreType().equals(ModelDBConstants.S3))
            .build());

    List<KeyValue> metrics = new ArrayList<>();
    randomValue = rangeMin + (rangeMax - rangeMin) * randomNum.nextDouble();
    intValue = Value.newBuilder().setNumberValue(randomValue).build();
    metrics.add(
        KeyValue.newBuilder()
            .setKey("accuracy_" + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build());
    randomValue = rangeMin + (rangeMax - rangeMin) * randomNum.nextDouble();
    intValue = Value.newBuilder().setNumberValue(randomValue).build();
    metrics.add(
        KeyValue.newBuilder()
            .setKey("loss_" + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build());
    randomValue = rangeMin + (rangeMax - rangeMin) * randomNum.nextDouble();
    Value listValue =
        Value.newBuilder()
            .setListValue(
                ListValue.newBuilder()
                    .addValues(intValue)
                    .addValues(Value.newBuilder().setNumberValue(randomValue).build()))
            .build();
    metrics.add(
        KeyValue.newBuilder()
            .setKey("profit_" + Calendar.getInstance().getTimeInMillis())
            .setValue(listValue)
            .setValueType(ValueType.LIST)
            .build());

    List<Observation> observations = new ArrayList<>();
    // TODO: uncomment after supporting artifact on observation
    /*observations.add(
    Observation.newBuilder()
        .setArtifact(
            Artifact.newBuilder()
                .setKey("Google developer Observation artifact")
                .setPath("This is data artifact type in Google developer Observation artifact")
                .setArtifactType(ArtifactType.DATA))
        .setTimestamp(Calendar.getInstance().getTimeInMillis() + 1)
        .setEpochNumber(Value.newBuilder().setNumberValue(1))
        .build());*/
    stringValue =
        Value.newBuilder()
            .setStringValue("Observation_value_" + Calendar.getInstance().getTimeInMillis())
            .build();
    observations.add(
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("Observation Key " + Calendar.getInstance().getTimeInMillis())
                    .setValue(stringValue)
                    .setValueType(ValueType.STRING))
            .setTimestamp(Calendar.getInstance().getTimeInMillis() + 2)
            .setEpochNumber(Value.newBuilder().setNumberValue(123))
            .build());

    List<Feature> features = new ArrayList<>();
    features.add(Feature.newBuilder().setName("ExperimentRun Test case feature 1").build());
    features.add(Feature.newBuilder().setName("ExperimentRun Test case feature 2").build());

    return CreateExperimentRun.newBuilder()
        .setProjectId(projectId)
        .setExperimentId(experimentId)
        .setName(experimentRunName)
        .setDescription("this is a ExperimentRun description")
        .setDateCreated(Calendar.getInstance().getTimeInMillis())
        .setDateUpdated(Calendar.getInstance().getTimeInMillis())
        .setStartTime(Calendar.getInstance().getTime().getTime())
        .setEndTime(Calendar.getInstance().getTime().getTime())
        .setCodeVersion("1.0")
        .addAllTags(tags)
        .addAllAttributes(attributeList)
        .addAllHyperparameters(hyperparameters)
        .addAllArtifacts(artifactList)
        .addAllDatasets(datasets)
        .addAllMetrics(metrics)
        .addAllObservations(observations)
        .addAllFeatures(features)
        .build();
  }

  @Test
  public void a_experimentRunCreateTest() {
    LOGGER.info("Create ExperimentRun test start................................");

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequest(project.getId(), experiment.getId(), experimentRun.getName());

    try {
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    try {
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().setProjectId("xyz").build();
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException e) {
      checkEqualsAssert(e);
    }

    try {
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setProjectId(project.getId())
              .setExperimentId("xyz")
              .build();
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    try {
      String tag52 = "Human Activity Recognition using Smartphone Dataset";
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setProjectId(project.getId())
              .setExperimentId(experiment.getId())
              .addTags(tag52)
              .build();
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    try {
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().clearTags().addTags("").build();
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    try {
      String name =
          "Experiment of Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset";
      createExperimentRunRequest = createExperimentRunRequest.toBuilder().setName(name).build();
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Create ExperimentRun test stop................................");
  }

  @Test
  public void a_experimentRunCreateNegativeTest() {
    LOGGER.info("Create ExperimentRun Negative test start................................");

    CreateExperimentRun request = getCreateExperimentRunRequest("", "abcd", "ExperiemntRun_xyz");

    try {
      experimentRunServiceStub.createExperimentRun(request);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.info("CreateExperimentRun Response : \n" + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Create ExperimentRun Negative test stop................................");
  }

  @Test
  public void b_getExperimentRunFromProjectRunTest() {
    LOGGER.info("Get ExperimentRun from Project test start................................");

    GetExperimentRunsInProject getExperimentRunRequest =
        GetExperimentRunsInProject.newBuilder().setProjectId(project.getId()).build();

    GetExperimentRunsInProject.Response experimentRunResponse =
        experimentRunServiceStub.getExperimentRunsInProject(getExperimentRunRequest);
    assertEquals(
        "ExperimentRuns count not match with expected experimentRun count",
        experimentRunMap.size(),
        experimentRunResponse.getExperimentRunsList().size());

    if (experimentRunResponse.getExperimentRunsList() != null) {
      for (ExperimentRun experimentRun1 : experimentRunResponse.getExperimentRunsList()) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun",
            experimentRunMap.get(experimentRun1.getId()),
            experimentRun1);
      }
    } else {
      LOGGER.warn("More ExperimentRun not found in database");
      assertTrue(true);
    }

    LOGGER.info("Get ExperimentRun from Project test stop................................");
  }

  @Test
  public void b_getExperimentRunWithPaginationFromProjectRunTest() {
    LOGGER.info(
        "Get ExperimentRun using pagination from Project test start................................");

    int pageLimit = 2;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      GetExperimentRunsInProject getExperimentRunRequest =
          GetExperimentRunsInProject.newBuilder()
              .setProjectId(project.getId())
              .setPageNumber(pageNumber)
              .setPageLimit(pageLimit)
              .setAscending(true)
              .setSortKey(ModelDBConstants.NAME)
              .build();

      GetExperimentRunsInProject.Response experimentRunResponse =
          experimentRunServiceStub.getExperimentRunsInProject(getExperimentRunRequest);

      assertEquals(
          "Total records count not matched with expected records count",
          2,
          experimentRunResponse.getTotalRecords());

      if (experimentRunResponse.getExperimentRunsList() != null
          && experimentRunResponse.getExperimentRunsList().size() > 0) {
        isExpectedResultFound = true;
        for (ExperimentRun experimentRun : experimentRunResponse.getExperimentRunsList()) {
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRunMap.get(experimentRun.getId()),
              experimentRun);
        }
      } else {
        if (isExpectedResultFound) {
          LOGGER.warn("More ExperimentRun not found in database");
          assertTrue(true);
        } else {
          fail("Expected experimentRun not found in response");
        }
        break;
      }
    }

    GetExperimentRunsInProject getExperiment =
        GetExperimentRunsInProject.newBuilder()
            .setProjectId(project.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(false)
            .setSortKey("metrics.loss")
            .build();

    GetExperimentRunsInProject.Response experimentRunResponse =
        experimentRunServiceStub.getExperimentRunsInProject(getExperiment);
    assertEquals(
        "Total records count not matched with expected records count",
        2,
        experimentRunResponse.getTotalRecords());
    assertEquals(
        "ExperimentRuns count not match with expected experimentRuns count",
        1,
        experimentRunResponse.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun2,
        experimentRunResponse.getExperimentRuns(0));

    getExperiment =
        GetExperimentRunsInProject.newBuilder()
            .setProjectId(project.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(true)
            .setSortKey("")
            .build();

    experimentRunResponse = experimentRunServiceStub.getExperimentRunsInProject(getExperiment);
    assertEquals(
        "ExperimentRuns count not match with expected experimentRuns count",
        1,
        experimentRunResponse.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun,
        experimentRunResponse.getExperimentRuns(0));

    getExperiment =
        GetExperimentRunsInProject.newBuilder()
            .setProjectId(project.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(true)
            .setSortKey("observations.attribute.attr_1")
            .build();

    try {
      experimentRunServiceStub.getExperimentRunsInProject(getExperiment);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get ExperimentRun using pagination from Project test stop................................");
  }

  @Test
  public void b_getExperimentFromProjectRunNegativeTest() {
    LOGGER.info(
        "Get ExperimentRun from Project Negative test start................................");

    GetExperimentRunsInProject getExperiment = GetExperimentRunsInProject.newBuilder().build();
    try {
      experimentRunServiceStub.getExperimentRunsInProject(getExperiment);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getExperiment = GetExperimentRunsInProject.newBuilder().setProjectId("sdfdsfsd").build();
    GetExperimentRunsInProject.Response response =
        experimentRunServiceStub.getExperimentRunsInProject(getExperiment);
    assertEquals("Expected response not found", 0, response.getExperimentRunsCount());

    LOGGER.info(
        "Get ExperimentRun from Project Negative test stop................................");
  }

  @Test
  public void bb_getExperimentRunFromExperimentTest() {
    LOGGER.info("Get ExperimentRun from Experiment test start................................");

    GetExperimentRunsInExperiment getExperimentRunsInExperiment =
        GetExperimentRunsInExperiment.newBuilder().setExperimentId(experiment.getId()).build();

    GetExperimentRunsInExperiment.Response experimentRunResponse =
        experimentRunServiceStub.getExperimentRunsInExperiment(getExperimentRunsInExperiment);
    assertEquals(
        "ExperimentRuns count not match with expected experimentRun count",
        experimentRunMap.size(),
        experimentRunResponse.getExperimentRunsList().size());

    if (experimentRunResponse.getExperimentRunsList() != null) {
      for (ExperimentRun experimentRun1 : experimentRunResponse.getExperimentRunsList()) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun",
            experimentRunMap.get(experimentRun1.getId()),
            experimentRun1);
      }
    } else {
      LOGGER.warn("More ExperimentRun not found in database");
      assertTrue(true);
    }

    LOGGER.info("Get ExperimentRun from Experiment test stop................................");
  }

  @Test
  public void bb_getExperimentRunWithPaginationFromExperimentTest() {
    LOGGER.info(
        "Get ExperimentRun using pagination from Experiment test start................................");

    int pageLimit = 1;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      GetExperimentRunsInExperiment getExperimentRunsInExperiment =
          GetExperimentRunsInExperiment.newBuilder()
              .setExperimentId(experiment.getId())
              .setPageNumber(pageNumber)
              .setPageLimit(pageLimit)
              .setAscending(true)
              .setSortKey(ModelDBConstants.NAME)
              .build();

      GetExperimentRunsInExperiment.Response experimentRunResponse =
          experimentRunServiceStub.getExperimentRunsInExperiment(getExperimentRunsInExperiment);
      assertEquals(
          "Total records count not matched with expected records count",
          2,
          experimentRunResponse.getTotalRecords());

      if (!experimentRunResponse.getExperimentRunsList().isEmpty()) {
        isExpectedResultFound = true;
        for (ExperimentRun experimentRun : experimentRunResponse.getExperimentRunsList()) {
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRunMap.get(experimentRun.getId()),
              experimentRun);
        }
      } else {
        if (isExpectedResultFound) {
          LOGGER.warn("More ExperimentRun not found in database");
          assertTrue(true);
          break;
        } else {
          fail("Expected experimentRun not found in response");
        }
      }
    }

    GetExperimentRunsInExperiment getExperimentRunsInExperiment =
        GetExperimentRunsInExperiment.newBuilder()
            .setExperimentId(experiment.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(false)
            .setSortKey("metrics.loss")
            .build();

    GetExperimentRunsInExperiment.Response experimentRunResponse =
        experimentRunServiceStub.getExperimentRunsInExperiment(getExperimentRunsInExperiment);
    assertEquals(
        "Total records count not matched with expected records count",
        2,
        experimentRunResponse.getTotalRecords());
    assertEquals(
        "ExperimentRuns count not match with expected experimentRuns count",
        1,
        experimentRunResponse.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun2,
        experimentRunResponse.getExperimentRuns(0));

    getExperimentRunsInExperiment =
        GetExperimentRunsInExperiment.newBuilder()
            .setExperimentId(experiment.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(true)
            .setSortKey("")
            .build();

    experimentRunResponse =
        experimentRunServiceStub.getExperimentRunsInExperiment(getExperimentRunsInExperiment);
    assertEquals(
        "ExperimentRuns count not match with expected experimentRuns count",
        1,
        experimentRunResponse.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun,
        experimentRunResponse.getExperimentRuns(0));
    assertEquals(
        "Total records count not matched with expected records count",
        2,
        experimentRunResponse.getTotalRecords());

    getExperimentRunsInExperiment =
        GetExperimentRunsInExperiment.newBuilder()
            .setExperimentId(experiment.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(true)
            .setSortKey("observations.attribute.attr_1")
            .build();

    try {
      experimentRunServiceStub.getExperimentRunsInExperiment(getExperimentRunsInExperiment);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get ExperimentRun using pagination from Experiment test stop................................");
  }

  @Test
  public void bb_getExperimentFromExperimentNegativeTest() {
    LOGGER.info(
        "Get ExperimentRun from Experiment Negative test start................................");

    GetExperimentRunsInExperiment getExperiment =
        GetExperimentRunsInExperiment.newBuilder().build();
    try {
      experimentRunServiceStub.getExperimentRunsInExperiment(getExperiment);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getExperiment = GetExperimentRunsInExperiment.newBuilder().setExperimentId("sdfdsfsd").build();
    try {
      experimentRunServiceStub.getExperimentRunsInExperiment(getExperiment);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get ExperimentRun from Experiment Negative test stop................................");
  }

  @Test
  public void c_getExperimentRunByIdTest() {
    LOGGER.info("Get ExperimentRunById test start................................");

    GetExperimentRunById request =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();

    GetExperimentRunById.Response response = experimentRunServiceStub.getExperimentRunById(request);

    LOGGER.info("getExperimentRunById Response : \n" + response.getExperimentRun());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun,
        response.getExperimentRun());

    LOGGER.info("Get ExperimentRunById test stop................................");
  }

  @Test
  public void c_getExperimentRunByIdNegativeTest() {
    LOGGER.info("Get ExperimentRunById Negative test start................................");

    GetExperimentRunById request = GetExperimentRunById.newBuilder().build();

    try {
      experimentRunServiceStub.getExperimentRunById(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    request = GetExperimentRunById.newBuilder().setId("fdsfd").build();

    try {
      experimentRunServiceStub.getExperimentRunById(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info("Get ExperimentRunById Negative test stop................................");
  }

  @Test
  public void c_getExperimentRunByNameTest() {
    LOGGER.info("Get ExperimentRunByName test start................................");

    GetExperimentRunByName request =
        GetExperimentRunByName.newBuilder()
            .setName(experimentRun.getName())
            .setExperimentId(experimentRun.getExperimentId())
            .build();

    GetExperimentRunByName.Response response =
        experimentRunServiceStub.getExperimentRunByName(request);

    LOGGER.info("getExperimentRunByName Response : \n" + response.getExperimentRun());
    assertEquals(
        "ExperimentRun name not match with expected experimentRun name ",
        experimentRun.getName(),
        response.getExperimentRun().getName());

    LOGGER.info("Get ExperimentRunByName test stop................................");
  }

  @Test
  public void c_getExperimentRunByNameNegativeTest() {
    LOGGER.info("Get ExperimentRunByName Negative test start................................");

    GetExperimentRunByName request = GetExperimentRunByName.newBuilder().build();

    try {
      experimentRunServiceStub.getExperimentRunByName(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Get ExperimentRunByName Negative test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: updateExperimentRunName endpoint")
  public void d_updateExperimentRunNameOrDescription() {
    LOGGER.info(
        "Update ExperimentRun Name & Description test start................................");

    UpdateExperimentRunName request =
        UpdateExperimentRunName.newBuilder()
            .setId(experimentRun.getId())
            .setName("ExperimentRun Name updated " + Calendar.getInstance().getTimeInMillis())
            .build();

    experimentRunServiceStub.updateExperimentRunName(request);

    UpdateExperimentRunDescription request2 =
        UpdateExperimentRunDescription.newBuilder()
            .setId(experimentRun.getId())
            .setDescription(
                "this is a ExperimentRun description updated "
                    + Calendar.getInstance().getTimeInMillis())
            .build();

    UpdateExperimentRunDescription.Response response2 =
        experimentRunServiceStub.updateExperimentRunDescription(request2);
    assertEquals(
        "ExperimentRun Description do not match with expected experimentRun name",
        request2.getDescription(),
        response2.getExperimentRun().getDescription());

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response2.getExperimentRun().getDateUpdated());
    experimentRun = response2.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    try {
      String name =
          "Experiment of Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset";
      request = request.toBuilder().setName(name).build();
      experimentRunServiceStub.updateExperimentRunName(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "Update ExperimentRun Name & Description test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: updateExperimentRunName endpoint")
  public void d_updateExperimentRunNameOrDescriptionNegativeTest() {
    LOGGER.info("Update ExperimentRun Name & Description Negative test start.......");

    UpdateExperimentRunDescription request =
        UpdateExperimentRunDescription.newBuilder()
            .setDescription(
                "this is a ExperimentRun description updated "
                    + Calendar.getInstance().getTimeInMillis())
            .build();

    try {
      experimentRunServiceStub.updateExperimentRunDescription(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Update ExperimentRun Name & Description Negative test stop.......");
  }

  @Test
  public void e_addExperimentRunTags() {
    LOGGER.info("Add ExperimentRun tags test start................................");

    List<String> tags = new ArrayList<>();
    tags.add("Test Added tag");
    tags.add("Test Added tag 2");

    AddExperimentRunTags request =
        AddExperimentRunTags.newBuilder().setId(experimentRun.getId()).addAllTags(tags).build();

    AddExperimentRunTags.Response aertResponse =
        experimentRunServiceStub.addExperimentRunTags(request);
    LOGGER.info("AddExperimentRunTags Response : \n" + aertResponse.getExperimentRun());
    assertEquals(
        experimentRun.getTagsCount() + tags.size(), aertResponse.getExperimentRun().getTagsCount());

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        aertResponse.getExperimentRun().getDateUpdated());
    experimentRun = aertResponse.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    tags = new ArrayList<>();
    tags.add("Test Added tag 3");
    tags.add("Test Added tag 2");

    request =
        AddExperimentRunTags.newBuilder().setId(experimentRun.getId()).addAllTags(tags).build();

    aertResponse = experimentRunServiceStub.addExperimentRunTags(request);
    LOGGER.info("AddExperimentRunTags Response : \n" + aertResponse.getExperimentRun());
    assertEquals(experimentRun.getTagsCount() + 1, aertResponse.getExperimentRun().getTagsCount());
    experimentRun = aertResponse.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    try {
      String tag52 = "Human Activity Recognition using Smartphone Dataset";
      request = request.toBuilder().addTags(tag52).build();
      experimentRunServiceStub.addExperimentRunTags(request);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add ExperimentRun tags test stop................................");
  }

  @Test
  public void ea_addExperimentRunTagsNegativeTest() {
    LOGGER.info("Add ExperimentRun tags Negative test start................................");

    List<String> tags = new ArrayList<>();
    tags.add("Test Added tag " + Calendar.getInstance().getTimeInMillis());
    tags.add("Test Added tag 2 " + Calendar.getInstance().getTimeInMillis());

    AddExperimentRunTags request = AddExperimentRunTags.newBuilder().addAllTags(tags).build();

    try {
      experimentRunServiceStub.addExperimentRunTags(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add ExperimentRun tags Negative test stop................................");
  }

  @Test
  public void eb_addExperimentRunTag() {
    LOGGER.info("Add ExperimentRun tag test start................................");

    AddExperimentRunTag request =
        AddExperimentRunTag.newBuilder()
            .setId(experimentRun.getId())
            .setTag("Added new tag 1")
            .build();

    AddExperimentRunTag.Response aertResponse =
        experimentRunServiceStub.addExperimentRunTag(request);
    LOGGER.info("AddExperimentRunTag Response : \n" + aertResponse.getExperimentRun());
    assertEquals(
        "ExperimentRun tags not match with expected experimentRun tags",
        experimentRun.getTagsCount() + 1,
        aertResponse.getExperimentRun().getTagsCount());

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        aertResponse.getExperimentRun().getDateUpdated());
    experimentRun = aertResponse.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    try {
      String tag52 = "Human Activity Recognition using Smartphone Dataset";
      request = request.toBuilder().setTag(tag52).build();
      experimentRunServiceStub.addExperimentRunTag(request);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add ExperimentRun tags test stop................................");
  }

  @Test
  public void ec_addExperimentRunTagNegativeTest() {
    LOGGER.info("Add ExperimentRun tag Negative test start................................");

    AddExperimentRunTag request = AddExperimentRunTag.newBuilder().setTag("Tag_xyz").build();

    try {
      experimentRunServiceStub.addExperimentRunTag(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add ExperimentRun tag Negative test stop................................");
  }

  @Test
  public void ee_getExperimentRunTags() {
    LOGGER.info("Get ExperimentRun tags test start................................");

    GetTags request = GetTags.newBuilder().setId(experimentRun.getId()).build();

    GetTags.Response response = experimentRunServiceStub.getExperimentRunTags(request);
    LOGGER.info("GetExperimentRunTags Response : \n" + response.getTagsList());
    assertEquals(
        "ExperimentRun tags not match with expected experimentRun tags",
        experimentRun.getTagsList(),
        response.getTagsList());

    LOGGER.info("Get ExperimentRun tags test stop................................");
  }

  @Test
  public void eea_getExperimentRunTagsNegativeTest() {
    LOGGER.info("Get ExperimentRun tags Negative test start................................");

    GetTags request = GetTags.newBuilder().build();
    try {
      experimentRunServiceStub.getExperimentRunTags(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Get ExperimentRun tags Negative test stop................................");
  }

  @Test
  public void f_deleteExperimentRunTags() {
    LOGGER.info("Delete ExperimentRun tags test start................................");

    e_addExperimentRunTags();
    List<String> removableTagList = new ArrayList<>();
    if (experimentRun.getTagsList().size() > 1) {
      removableTagList =
          experimentRun.getTagsList().subList(0, experimentRun.getTagsList().size() - 1);
    }
    DeleteExperimentRunTags request =
        DeleteExperimentRunTags.newBuilder()
            .setId(experimentRun.getId())
            .addAllTags(removableTagList)
            .build();

    DeleteExperimentRunTags.Response response =
        experimentRunServiceStub.deleteExperimentRunTags(request);
    LOGGER.info(
        "DeleteExperimentRunTags Response : \n" + response.getExperimentRun().getTagsList());
    assertTrue(response.getExperimentRun().getTagsList().size() <= 1);

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    if (response.getExperimentRun().getTagsList().size() > 0) {
      request =
          DeleteExperimentRunTags.newBuilder()
              .setId(experimentRun.getId())
              .setDeleteAll(true)
              .build();

      response = experimentRunServiceStub.deleteExperimentRunTags(request);
      LOGGER.info(
          "DeleteExperimentRunTags Response : \n" + response.getExperimentRun().getTagsList());
      assertEquals(0, response.getExperimentRun().getTagsList().size());
      experimentRun = response.getExperimentRun();
      experimentRunMap.put(experimentRun.getId(), experimentRun);
    }

    LOGGER.info("Delete ExperimentRun tags test stop................................");
  }

  @Test
  public void fa_deleteExperimentRunTagsNegativeTest() {
    LOGGER.info("Delete ExperimentRun tags Negative test start................................");

    DeleteExperimentRunTags request = DeleteExperimentRunTags.newBuilder().build();

    try {
      experimentRunServiceStub.deleteExperimentRunTags(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Delete ExperimentRun tags Negative test stop................................");
  }

  @Test
  public void fb_deleteExperimentRunTag() {
    LOGGER.info("Delete ExperimentRun tag test start................................");

    e_addExperimentRunTags();
    DeleteExperimentRunTag request =
        DeleteExperimentRunTag.newBuilder()
            .setId(experimentRun.getId())
            .setTag(experimentRun.getTags(0))
            .build();

    DeleteExperimentRunTag.Response response =
        experimentRunServiceStub.deleteExperimentRunTag(request);
    LOGGER.info("DeleteExperimentRunTag Response : \n" + response.getExperimentRun().getTagsList());
    assertFalse(response.getExperimentRun().getTagsList().contains(experimentRun.getTags(0)));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);
    LOGGER.info("Delete ExperimentRun tags test stop................................");
  }

  /**
   * This test tests comparision predicates on numeric Key Values (Hyperparameters in this case) It
   * creates a project with two Experiments , each with two experiment runs. E1 ER1 hyperparameter.C
   * = 0.0001 E1 ER2 no hyperparameters E2 ER1 hyperparameter.C = 0.0001 E2 ER1 hyperparameter.C =
   * 1E-6
   *
   * <p>It then filters on C >= 0.0001 and expects 2 ExperimentRuns in results
   */
  @Test
  public void findExperimentRunsHyperparameter() {
    LOGGER.info("FindExperimentRuns test start................................");

    Map<String, ExperimentRun> experimentRunMap = new HashMap<>();

    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun_ferh_1");
      KeyValue hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun11 = createExperimentRunResponse.getExperimentRun();
      experimentRunMap.put(experimentRun11.getId(), experimentRun11);
      LOGGER.info("ExperimentRun created successfully");
      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun_ferh_2");
      createExperimentRunRequest = createExperimentRunRequest.toBuilder().build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun12 = createExperimentRunResponse.getExperimentRun();
      experimentRunMap.put(experimentRun12.getId(), experimentRun12);
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun12.getName());

      // experiment2 of above project
      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun_ferh_2");
      hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun21 = createExperimentRunResponse.getExperimentRun();
      experimentRunMap.put(experimentRun21.getId(), experimentRun21);
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun21.getName());

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun_ferh_1");
      hyperparameter1 = generateNumericKeyValue("C", 1e-6);
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun22 = createExperimentRunResponse.getExperimentRun();
      experimentRunMap.put(experimentRun22.getId(), experimentRun22);
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun22.getName());

      Value hyperparameterFilter = Value.newBuilder().setNumberValue(0.0001).build();
      KeyValueQuery CGTE0_0001 =
          KeyValueQuery.newBuilder()
              .setKey("hyperparameters.C")
              .setValue(hyperparameterFilter)
              .setOperator(Operator.GTE)
              .setValueType(ValueType.NUMBER)
              .build();

      FindExperimentRuns findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .addPredicates(CGTE0_0001)
              .setAscending(false)
              .setIdsOnly(false)
              .build();

      FindExperimentRuns.Response response =
          experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      assertEquals(
          "Total records count not matched with expected records count",
          2,
          response.getTotalRecords());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          2,
          response.getExperimentRunsCount());
      for (ExperimentRun exprRun : response.getExperimentRunsList()) {
        for (KeyValue kv : exprRun.getHyperparametersList()) {
          if (kv.getKey() == "C") {
            assertEquals(
                "Value should be GTE 0.0001 " + kv, true, kv.getValue().getNumberValue() > 0.0001);
          }
        }
      }
    } finally {
      for (String runId : experimentRunMap.keySet()) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("FindExperimentRuns test stop................................");
  }

  private KeyValue generateNumericKeyValue(String key, Double value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(Value.newBuilder().setNumberValue(value).build())
        .build();
  }

  @Test
  public void g_logObservationTest() {
    LOGGER.info(" Log Observation in ExperimentRun test start................................");

    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    Observation observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("New Added Key " + Calendar.getInstance().getTimeInMillis())
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .setEpochNumber(Value.newBuilder().setNumberValue(2L))
            .build();

    LogObservation logObservationRequest =
        LogObservation.newBuilder()
            .setId(experimentRun.getId())
            .setObservation(observation)
            .build();

    experimentRunServiceStub.logObservation(logObservationRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogObservation Response : \n" + response.getExperimentRun());
    assertTrue(response.getExperimentRun().getObservationsList().contains(observation));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Observation in ExperimentRun tags test stop................................");
  }

  @Test
  public void g_logObservationNegativeTest() {
    LOGGER.info(
        " Log Observation in ExperimentRun Negative test start................................");

    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    Observation observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("New Added Key " + Calendar.getInstance().getTimeInMillis())
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .build();

    LogObservation logObservationRequest =
        LogObservation.newBuilder().setObservation(observation).build();

    try {
      experimentRunServiceStub.logObservation(logObservationRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logObservationRequest =
        LogObservation.newBuilder()
            .setId("sdfsd")
            .setObservation(experimentRun.getObservations(0))
            .build();

    try {
      experimentRunServiceStub.logObservation(logObservationRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Observation in ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void g_logObservationsTest() {
    LOGGER.info(" Log Observations in ExperimentRun test start................................");

    List<Observation> observations = new ArrayList<>();
    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    Observation observation1 =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("New Added Key " + Calendar.getInstance().getTimeInMillis())
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .setEpochNumber(Value.newBuilder().setNumberValue(234L))
            .build();
    observations.add(observation1);

    Value stringValue =
        Value.newBuilder()
            .setStringValue("new Observation " + Calendar.getInstance().getTimeInMillis())
            .build();
    Observation observation2 =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("New Added Key " + Calendar.getInstance().getTimeInMillis())
                    .setValue(stringValue)
                    .setValueType(ValueType.STRING)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .setEpochNumber(Value.newBuilder().setNumberValue(2134L))
            .build();
    observations.add(observation2);

    LogObservations logObservationRequest =
        LogObservations.newBuilder()
            .setId(experimentRun.getId())
            .addAllObservations(observations)
            .build();

    experimentRunServiceStub.logObservations(logObservationRequest);
    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);

    LOGGER.info("LogObservation Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun observations not match with expected ExperimentRun observation",
        response.getExperimentRun().getObservationsList().containsAll(observations));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    logObservationRequest =
        LogObservations.newBuilder()
            .setId(experimentRun.getId())
            .addAllObservations(experimentRun.getObservationsList())
            .build();

    experimentRunServiceStub.logObservations(logObservationRequest);

    getExperimentRunById = GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info(
        "Duplicate LogObservation Response : \n"
            + response.getExperimentRun().getObservationsList());
    assertEquals(
        "Existing observations count not match with expected observations count",
        experimentRun.getObservationsList().size() + experimentRun.getObservationsList().size(),
        response.getExperimentRun().getObservationsList().size());

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Observations in ExperimentRun tags test stop................................");
  }

  @Test
  public void g_logObservationsNegativeTest() {
    LOGGER.info(
        " Log Observations in ExperimentRun Negative test start................................");

    List<Observation> observations = new ArrayList<>();
    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    Observation observation1 =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("New Added Key " + Calendar.getInstance().getTimeInMillis())
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .build();
    observations.add(observation1);

    Value stringValue =
        Value.newBuilder()
            .setStringValue("new Observation " + Calendar.getInstance().getTimeInMillis())
            .build();
    Observation observation2 =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("New Added Key " + Calendar.getInstance().getTimeInMillis())
                    .setValue(stringValue)
                    .setValueType(ValueType.STRING)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .build();
    observations.add(observation2);

    LogObservations logObservationRequest =
        LogObservations.newBuilder().addAllObservations(observations).build();

    try {
      experimentRunServiceStub.logObservations(logObservationRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logObservationRequest =
        LogObservations.newBuilder()
            .setId("sdfsd")
            .addAllObservations(experimentRun.getObservationsList())
            .build();

    try {
      experimentRunServiceStub.logObservations(logObservationRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Observations in ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void h_getObservationTest() {
    LOGGER.info("Get Observation from ExperimentRun test start................................");

    g_logObservationsTest();
    GetObservations getObservationRequest =
        GetObservations.newBuilder()
            .setId(experimentRun.getId())
            .setObservationKey("Google developer Observation artifact")
            .build();

    GetObservations.Response response =
        experimentRunServiceStub.getObservations(getObservationRequest);

    LOGGER.info("GetObservations Response : " + response.getObservationsCount());
    for (Observation observation : response.getObservationsList()) {
      if (observation.hasAttribute()) {
        assertEquals(
            "ExperimentRun observations not match with expected observations ",
            "Google developer Observation artifact",
            observation.getAttribute().getKey());
      } else if (observation.hasArtifact()) {
        assertEquals(
            "ExperimentRun observations not match with expected observations ",
            "Google developer Observation artifact",
            observation.getArtifact().getKey());
      }
    }

    LOGGER.info(
        "Get Observation from ExperimentRun tags test stop................................");
  }

  @Test
  public void h_getObservationNegativeTest() {
    LOGGER.info(
        "Get Observation from ExperimentRun Negative test start................................");

    GetObservations getObservationRequest =
        GetObservations.newBuilder()
            .setObservationKey("Google developer Observation artifact")
            .build();

    try {
      experimentRunServiceStub.getObservations(getObservationRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getObservationRequest =
        GetObservations.newBuilder()
            .setId("dfsdfs")
            .setObservationKey("Google developer Observation artifact")
            .build();

    try {
      experimentRunServiceStub.getObservations(getObservationRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get Observation from ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void h_LogObservationEpochTest() {
    LOGGER.info("Get Observation from ExperimentRun test start................................");

    DeleteObservations request =
        DeleteObservations.newBuilder().setId(experimentRun.getId()).setDeleteAll(true).build();
    experimentRunServiceStub.deleteObservations(request);

    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    // First Observation without epoch should get epoch number to zero
    Observation observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("key1")
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .build();

    LogObservation logObservationRequest =
        LogObservation.newBuilder()
            .setId(experimentRun.getId())
            .setObservation(observation)
            .build();

    experimentRunServiceStub.logObservation(logObservationRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.debug("observation epoch should be 0 Response: {}", response);
    assertEquals(
        "there should be exactly one observation",
        1,
        response.getExperimentRun().getObservationsCount());
    Observation responseObservation = response.getExperimentRun().getObservations(0);
    assertEquals(
        "observation epoch should be 0",
        0,
        responseObservation.getEpochNumber().getNumberValue(),
        0.0);

    // observation with epoch should set the value passed
    observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("key1")
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .setEpochNumber(Value.newBuilder().setNumberValue(123))
            .build();

    logObservationRequest =
        LogObservation.newBuilder()
            .setId(experimentRun.getId())
            .setObservation(observation)
            .build();

    experimentRunServiceStub.logObservation(logObservationRequest);

    getExperimentRunById = GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.debug("observation epoch should be 123 Response: {}", response);
    assertEquals(
        "there should be two observations", 2, response.getExperimentRun().getObservationsCount());
    // Observations are sorted by auto incr id so the observation of interest is on index 1
    responseObservation = response.getExperimentRun().getObservations(1);
    assertEquals(
        "observation epoch should be 123",
        123,
        (long) responseObservation.getEpochNumber().getNumberValue());

    // Subsequent call to log observation without epoch should set value to old max +1
    observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("key1")
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .build();

    logObservationRequest =
        LogObservation.newBuilder()
            .setId(experimentRun.getId())
            .setObservation(observation)
            .build();

    experimentRunServiceStub.logObservation(logObservationRequest);

    getExperimentRunById = GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.debug("observation epoch should be 123 + 1 Response: {}", response);
    assertEquals(
        "there should be three observations",
        3,
        response.getExperimentRun().getObservationsCount());
    // Observations are sorted by auto incr id so the observation of interest is on index 2
    responseObservation = response.getExperimentRun().getObservations(2);
    assertEquals(
        "observation epoch should be 123 + 1",
        124,
        (long) responseObservation.getEpochNumber().getNumberValue());

    // call to log observation with epoch but o not a different key should set value to 0
    observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("key2")
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .build();

    logObservationRequest =
        LogObservation.newBuilder()
            .setId(experimentRun.getId())
            .setObservation(observation)
            .build();

    experimentRunServiceStub.logObservation(logObservationRequest);

    getExperimentRunById = GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.debug("observation epoch should be 0 again Response: {}", response);
    assertEquals(
        "there should be four observations", 4, response.getExperimentRun().getObservationsCount());
    // Observations are sorted by auto incr id so the observation of interest is on index 3
    responseObservation = response.getExperimentRun().getObservations(3);
    assertEquals(
        "observation epoch should be 0",
        0,
        (long) responseObservation.getEpochNumber().getNumberValue());

    // same epoch_number, same key stores duplicate
    observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("key1")
                    .setValue(Value.newBuilder().setNumberValue(1))
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .setEpochNumber(Value.newBuilder().setNumberValue(123))
            .build();

    logObservationRequest =
        LogObservation.newBuilder()
            .setId(experimentRun.getId())
            .setObservation(observation)
            .build();

    experimentRunServiceStub.logObservation(logObservationRequest);

    getExperimentRunById = GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    assertEquals(
        "there should be five observations", 5, response.getExperimentRun().getObservationsCount());
    // Observations are sorted by auto incr id so the observation of interest is on index 3
    responseObservation = response.getExperimentRun().getObservations(1);
    Observation responseObservation2 = response.getExperimentRun().getObservations(3);
    assertEquals(
        "observations have same key",
        responseObservation.getAttribute().getKey(),
        responseObservation2.getAttribute().getKey());
    assertEquals(
        "observations have same epoch number",
        responseObservation.getEpochNumber(),
        responseObservation2.getEpochNumber());

    // call to log observation with non numeric epoch throws error
    observation =
        Observation.newBuilder()
            .setAttribute(
                KeyValue.newBuilder()
                    .setKey("key2")
                    .setValue(intValue)
                    .setValueType(ValueType.NUMBER)
                    .build())
            .setTimestamp(Calendar.getInstance().getTimeInMillis())
            .setEpochNumber(Value.newBuilder().setStringValue("125"))
            .build();

    logObservationRequest =
        LogObservation.newBuilder()
            .setId(experimentRun.getId())
            .setObservation(observation)
            .build();

    try {
      experimentRunServiceStub.logObservation(logObservationRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get Observation from ExperimentRun tags test stop................................");
  }

  @Test
  public void i_logMetricTest() {
    LOGGER.info(" Log Metric in ExperimentRun test start................................");

    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    KeyValue keyValue =
        KeyValue.newBuilder()
            .setKey("New Added Metric " + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build();

    LogMetric logMetricRequest =
        LogMetric.newBuilder().setId(experimentRun.getId()).setMetric(keyValue).build();

    experimentRunServiceStub.logMetric(logMetricRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogMetric Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun metric not match with expected experimentRun metric",
        response.getExperimentRun().getMetricsList().contains(keyValue));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Metric in ExperimentRun tags test stop................................");
  }

  @Test
  public void i_logMetricNegativeTest() {
    LOGGER.info(" Log Metric in ExperimentRun Negative test start................................");

    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    KeyValue keyValue =
        KeyValue.newBuilder()
            .setKey("New Added Metric " + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build();

    LogMetric logMetricRequest = LogMetric.newBuilder().setMetric(keyValue).build();

    try {
      experimentRunServiceStub.logMetric(logMetricRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logMetricRequest = LogMetric.newBuilder().setId("dfsdfsd").setMetric(keyValue).build();

    try {
      experimentRunServiceStub.logMetric(logMetricRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    logMetricRequest =
        LogMetric.newBuilder()
            .setId(experimentRun.getId())
            .setMetric(experimentRun.getMetricsList().get(0))
            .build();

    try {
      experimentRunServiceStub.logMetric(logMetricRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Metric in ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void i_logMetricsTest() {
    LOGGER.info(" Log Metrics in ExperimentRun test start................................");

    List<KeyValue> keyValues = new ArrayList<>();
    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    KeyValue keyValue1 =
        KeyValue.newBuilder()
            .setKey("New Added Metric 1 " + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build();
    keyValues.add(keyValue1);
    Value stringValue =
        Value.newBuilder()
            .setStringValue("New Added Metric " + Calendar.getInstance().getTimeInMillis())
            .build();
    KeyValue keyValue2 =
        KeyValue.newBuilder()
            .setKey("New Added Metric 2 " + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build();
    keyValues.add(keyValue2);

    LogMetrics logMetricRequest =
        LogMetrics.newBuilder().setId(experimentRun.getId()).addAllMetrics(keyValues).build();

    experimentRunServiceStub.logMetrics(logMetricRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogMetrics Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun metrics not match with expected experimentRun metrics",
        response.getExperimentRun().getMetricsList().containsAll(keyValues));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Metrics in ExperimentRun tags test stop................................");
  }

  @Test
  public void i_logMetricsNegativeTest() {
    LOGGER.info(
        " Log Metrics in ExperimentRun Negative test start................................");

    List<KeyValue> keyValues = new ArrayList<>();
    Value intValue =
        Value.newBuilder().setNumberValue(Calendar.getInstance().getTimeInMillis()).build();
    KeyValue keyValue1 =
        KeyValue.newBuilder()
            .setKey("New Added Metric " + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build();
    keyValues.add(keyValue1);
    Value stringValue =
        Value.newBuilder()
            .setStringValue("New Added Metric " + Calendar.getInstance().getTimeInMillis())
            .build();
    KeyValue keyValue2 =
        KeyValue.newBuilder()
            .setKey("New Added Metric " + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build();
    keyValues.add(keyValue2);

    LogMetrics logMetricRequest = LogMetrics.newBuilder().addAllMetrics(keyValues).build();

    try {
      experimentRunServiceStub.logMetrics(logMetricRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logMetricRequest = LogMetrics.newBuilder().setId("dfsdfsd").addAllMetrics(keyValues).build();

    try {
      experimentRunServiceStub.logMetrics(logMetricRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(Status.NOT_FOUND.getCode().equals(status.getCode()));
    }

    logMetricRequest =
        LogMetrics.newBuilder()
            .setId(experimentRun.getId())
            .addAllMetrics(experimentRun.getMetricsList())
            .build();

    try {
      experimentRunServiceStub.logMetrics(logMetricRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Metrics in ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void j_getMetricsTest() {
    LOGGER.info("Get Metrics from ExperimentRun test start................................");

    i_logMetricsTest();
    GetMetrics getMetricsRequest = GetMetrics.newBuilder().setId(experimentRun.getId()).build();

    GetMetrics.Response response = experimentRunServiceStub.getMetrics(getMetricsRequest);
    LOGGER.info("GetMetrics Response : " + response.getMetricsCount());
    assertEquals(
        "ExperimentRun metrics not match with expected experimentRun metrics",
        experimentRun.getMetricsList(),
        response.getMetricsList());

    LOGGER.info("Get Metrics from ExperimentRun tags test stop................................");
  }

  @Test
  public void j_getMetricsNegativeTest() {
    LOGGER.info(
        "Get Metrics from ExperimentRun Negative test start................................");

    GetMetrics getMetricsRequest = GetMetrics.newBuilder().build();

    try {
      experimentRunServiceStub.getMetrics(getMetricsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getMetricsRequest = GetMetrics.newBuilder().setId("sdfdsfsd").build();

    try {
      experimentRunServiceStub.getMetrics(getMetricsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(Status.NOT_FOUND.getCode().equals(status.getCode()));
    }

    LOGGER.info(
        "Get Metrics from ExperimentRun tags Negative test stop................................");
  }

  /* commented till dataset int code is in
    @Test
    public void k_logDatasetTest() {
      LOGGER.info(" Log Dataset in ExperimentRun test start................................");

      ProjectTest projectTest = new ProjectTest();
      ExperimentTest experimentTest = new ExperimentTest();

      ProjectServiceBlockingStub projectServiceStub = ProjectServiceGrpc.newBlockingStub(channel);
      ExperimentServiceBlockingStub experimentServiceStub =
          ExperimentServiceGrpc.newBlockingStub(channel);
      ExperimentRunServiceBlockingStub experimentRunServiceStub =
          ExperimentRunServiceGrpc.newBlockingStub(channel);

      // Create project
      CreateProject createProjectRequest =
          projectTest.getCreateProjectRequest("experimentRun_project_ypcdt1");
      CreateProject.Response createProjectResponse =
          projectServiceStub.createProject(createProjectRequest);
      Project project = createProjectResponse.getProject();
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected project name",
          createProjectRequest.getName(),
          project.getName());

      // Create two experiment of above project
      CreateExperiment createExperimentRequest =
          experimentTest.getCreateExperimentRequest(project.getId(), "Experiment_n_sprt_abc");
      CreateExperiment.Response createExperimentResponse =
          experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment = createExperimentResponse.getExperiment();
      LOGGER.info("Experiment created successfully");
      assertEquals(
          "Experiment name not match with expected Experiment name",
          createExperimentRequest.getName(),
          experiment.getName());

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(project.getId(), experiment.getId(), "ExperimentRun_n_sprt");
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun.getName());

      DatasetServiceGrpc.DatasetServiceBlockingStub datasetServiceStub =
          DatasetServiceGrpc.newBlockingStub(channel);

      DatasetTest datasetTest = new DatasetTest();
      CreateDataset createDatasetRequest =
          datasetTest.getDatasetRequest("rental_TEXT_train_data.csv");
      CreateDataset.Response createDatasetResponse =
          datasetServiceStub.createDataset(createDatasetRequest);
      LOGGER.info("CreateDataset Response : \n" + createDatasetResponse.getDataset());
      assertEquals(
          "Dataset name not match with expected dataset name",
          createDatasetRequest.getName(),
          createDatasetResponse.getDataset().getName());

      Artifact artifact =
          Artifact.newBuilder()
              .setKey("Google Pay datasets " + Calendar.getInstance().getTimeInMillis())
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.DATA)
              .setLinkedArtifactId(createDatasetResponse.getDataset().getId())
              .build();

      LogDataset logDatasetRequest =
          LogDataset.newBuilder().setId(experimentRun.getId()).setDataset(artifact).build();

      experimentRunServiceStub.logDataset(logDatasetRequest);

      GetExperimentRunById getExperimentRunById =
          GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
      GetExperimentRunById.Response response =
          experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info("LogDataset Response : \n" + response.getExperimentRun());
      assertTrue(
          "Experiment dataset not match with expected dataset",
          response.getExperimentRun().getDatasetsList().contains(artifact));

      assertNotEquals(
          "ExperimentRun date_updated field not update on database",
          experimentRun.getDateUpdated(),
          response.getExperimentRun().getDateUpdated());

      artifact =
          artifact
              .toBuilder()
              .setPath(
                  "Overwritten data, This is overwritten data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.DATA)
              .build();

      logDatasetRequest =
          LogDataset.newBuilder().setId(experimentRun.getId()).setDataset(artifact).build();

      try {
        experimentRunServiceStub.logDataset(logDatasetRequest);
        fail();
      } catch (StatusRuntimeException ex) {
        Status status = Status.fromThrowable(ex);
        LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
      }

      logDatasetRequest =
          LogDataset.newBuilder()
              .setId(experimentRun.getId())
              .setDataset(artifact)
              .setOverwrite(true)
              .build();
      experimentRunServiceStub.logDataset(logDatasetRequest);

      response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      assertTrue(
          "Experiment dataset not match with expected dataset",
          response.getExperimentRun().getDatasetsList().contains(artifact));

      artifact =
          Artifact.newBuilder()
              .setKey("Google Pay datasets " + Calendar.getInstance().getTimeInMillis())
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.MODEL)
              .setLinkedArtifactId(createDatasetResponse.getDataset().getId())
              .build();

      logDatasetRequest =
          LogDataset.newBuilder().setId(experimentRun.getId()).setDataset(artifact).build();

      try {
        experimentRunServiceStub.logDataset(logDatasetRequest);
        fail();
      } catch (StatusRuntimeException ex) {
        Status status = Status.fromThrowable(ex);
        LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      }

      DeleteDataset deleteDataset =
          DeleteDataset.newBuilder().setId(createDatasetResponse.getDataset().getId()).build();
      DeleteDataset.Response deleteDatasetResponse = datasetServiceStub.deleteDataset(deleteDataset);
      LOGGER.info("Dataset deleted successfully");
      LOGGER.info(deleteDatasetResponse.toString());
      assertTrue(deleteDatasetResponse.getStatus());

      DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
      DeleteProject.Response deleteProjectResponse = projectServiceStub.deleteProject(deleteProject);
      LOGGER.info("Project deleted successfully");
      LOGGER.info(deleteProjectResponse.toString());
      assertTrue(deleteProjectResponse.getStatus());

      LOGGER.info("Log Dataset in ExperimentRun tags test stop................................");
    }


    @Test
    public void k_logDatasetNegativeTest() {
      LOGGER.info(
          " Log Dataset in ExperimentRun Negative test start................................");

      ProjectTest projectTest = new ProjectTest();
      ExperimentTest experimentTest = new ExperimentTest();

      ProjectServiceBlockingStub projectServiceStub = ProjectServiceGrpc.newBlockingStub(channel);
      ExperimentServiceBlockingStub experimentServiceStub =
          ExperimentServiceGrpc.newBlockingStub(channel);
      ExperimentRunServiceBlockingStub experimentRunServiceStub =
          ExperimentRunServiceGrpc.newBlockingStub(channel);

      // Create project
      CreateProject createProjectRequest =
          projectTest.getCreateProjectRequest("experimentRun_project_ypcdt1");
      CreateProject.Response createProjectResponse =
          projectServiceStub.createProject(createProjectRequest);
      Project project = createProjectResponse.getProject();
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected project name",
          createProjectRequest.getName(),
          project.getName());

      // Create two experiment of above project
      CreateExperiment createExperimentRequest =
          experimentTest.getCreateExperimentRequest(project.getId(), "Experiment_n_sprt_abc");
      CreateExperiment.Response createExperimentResponse =
          experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment = createExperimentResponse.getExperiment();
      LOGGER.info("Experiment created successfully");
      assertEquals(
          "Experiment name not match with expected Experiment name",
          createExperimentRequest.getName(),
          experiment.getName());

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(project.getId(), experiment.getId(), "ExperimentRun_n_sprt");
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun.getName());

      Artifact artifact =
          Artifact.newBuilder()
              .setKey("Google Pay datasets")
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.MODEL)
              .setLinkedArtifactId("dadasdadaasd")
              .build();

      LogDataset logDatasetRequest = LogDataset.newBuilder().setDataset(artifact).build();

      try {
        experimentRunServiceStub.logDataset(logDatasetRequest);
        fail();
      } catch (StatusRuntimeException ex) {
        Status status = Status.fromThrowable(ex);
        LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      }

      artifact = artifact.toBuilder().setArtifactType(ArtifactType.DATA).build();
      logDatasetRequest = LogDataset.newBuilder().setId("sdsdsa").setDataset(artifact).build();

      try {
        experimentRunServiceStub.logDataset(logDatasetRequest);
        fail();
      } catch (StatusRuntimeException ex) {
        Status status = Status.fromThrowable(ex);
        LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
      }

      artifact =
          Artifact.newBuilder()
              .setKey("Google Pay datasets " + Calendar.getInstance().getTimeInMillis())
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.DATA)
              .setLinkedArtifactId("fsdfsdfsdfds")
              .build();

      logDatasetRequest =
          LogDataset.newBuilder().setId(experimentRun.getId()).setDataset(artifact).build();

      experimentRunServiceStub.logDataset(logDatasetRequest);

      GetExperimentRunById getExperimentRunById =
          GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
      GetExperimentRunById.Response response =
          experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info("LogDataset Response : \n" + response.getExperimentRun());
      assertTrue(
          "Experiment dataset not match with expected dataset",
          response.getExperimentRun().getDatasetsList().contains(artifact));

      logDatasetRequest =
          LogDataset.newBuilder().setId(experimentRun.getId()).setDataset(artifact).build();
      try {
        experimentRunServiceStub.logDataset(logDatasetRequest);
        fail();
      } catch (StatusRuntimeException ex) {
        Status status = Status.fromThrowable(ex);
        LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
      }

      DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
      DeleteProject.Response deleteProjectResponse = projectServiceStub.deleteProject(deleteProject);
      LOGGER.info("Project deleted successfully");
      LOGGER.info(deleteProjectResponse.toString());
      assertTrue(deleteProjectResponse.getStatus());

      LOGGER.info(
          "Log Dataset in ExperimentRun tags Negative test stop................................");
    }

    @Test
    public void k_logDatasetsTest() {
      LOGGER.info(" Log Datasets in ExperimentRun test start................................");

      ProjectTest projectTest = new ProjectTest();
      ExperimentTest experimentTest = new ExperimentTest();

      ProjectServiceBlockingStub projectServiceStub = ProjectServiceGrpc.newBlockingStub(channel);
      ExperimentServiceBlockingStub experimentServiceStub =
          ExperimentServiceGrpc.newBlockingStub(channel);
      ExperimentRunServiceBlockingStub experimentRunServiceStub =
          ExperimentRunServiceGrpc.newBlockingStub(channel);

      // Create project
      CreateProject createProjectRequest =
          projectTest.getCreateProjectRequest("experimentRun_project_ypcdt1");
      CreateProject.Response createProjectResponse =
          projectServiceStub.createProject(createProjectRequest);
      Project project = createProjectResponse.getProject();
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected project name",
          createProjectRequest.getName(),
          project.getName());

      // Create two experiment of above project
      CreateExperiment createExperimentRequest =
          experimentTest.getCreateExperimentRequest(project.getId(), "Experiment_n_sprt_abc");
      CreateExperiment.Response createExperimentResponse =
          experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment = createExperimentResponse.getExperiment();
      LOGGER.info("Experiment created successfully");
      assertEquals(
          "Experiment name not match with expected Experiment name",
          createExperimentRequest.getName(),
          experiment.getName());

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(project.getId(), experiment.getId(), "ExperimentRun_n_sprt");
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun.getName());

      DatasetServiceGrpc.DatasetServiceBlockingStub datasetServiceStub =
          DatasetServiceGrpc.newBlockingStub(channel);

      DatasetTest datasetTest = new DatasetTest();
      CreateDataset createDatasetRequest =
          datasetTest.getDatasetRequest("rental_TEXT_train_data.csv");
      CreateDataset.Response createDatasetResponse =
          datasetServiceStub.createDataset(createDatasetRequest);
      Dataset dataset = createDatasetResponse.getDataset();
      LOGGER.info("CreateDataset Response : \n" + createDatasetResponse.getDataset());
      assertEquals(
          "Dataset name not match with expected dataset name",
          createDatasetRequest.getName(),
          dataset.getName());

      Map<String, Artifact> artifactMap = new HashMap<>();
      for (Artifact existingDataset : experimentRun.getDatasetsList()) {
        artifactMap.put(existingDataset.getKey(), existingDataset);
      }
      List<Artifact> artifacts = new ArrayList<>();
      Artifact artifact1 =
          Artifact.newBuilder()
              .setKey("Google Pay datasets_1")
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.DATA)
              .setLinkedArtifactId(dataset.getId())
              .build();
      artifacts.add(artifact1);
      artifactMap.put(artifact1.getKey(), artifact1);
      Artifact artifact2 =
          Artifact.newBuilder()
              .setKey("Google Pay datasets_2")
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.DATA)
              .setLinkedArtifactId(dataset.getId())
              .build();
      artifacts.add(artifact2);
      artifactMap.put(artifact2.getKey(), artifact2);

      LogDatasets logDatasetRequest =
          LogDatasets.newBuilder().setId(experimentRun.getId()).addAllDatasets(artifacts).build();

      experimentRunServiceStub.logDatasets(logDatasetRequest);

      GetExperimentRunById getExperimentRunById =
          GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
      GetExperimentRunById.Response response =
          experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info("LogDataset Response : \n" + response.getExperimentRun());

      for (Artifact datasetArtifact : response.getExperimentRun().getDatasetsList()) {
        assertEquals(
            "Experiment datasets not match with expected datasets",
            artifactMap.get(datasetArtifact.getKey()),
            datasetArtifact);
      }

      logDatasetRequest =
          LogDatasets.newBuilder().setId(experimentRun.getId()).addAllDatasets(artifacts).build();

      try {
        experimentRunServiceStub.logDatasets(logDatasetRequest);
        fail();
      } catch (StatusRuntimeException ex) {
        Status status = Status.fromThrowable(ex);
        LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
      }

      artifact1 =
          artifact1
              .toBuilder()
              .setPath(
                  "Overwritten data 1, This is overwritten data artifact type in Google Pay datasets")
              .build();
      artifactMap.put(artifact1.getKey(), artifact1);
      artifact2 =
          artifact2
              .toBuilder()
              .setPath(
                  "Overwritten data 2, This is overwritten data artifact type in Google Pay datasets")
              .build();
      artifactMap.put(artifact2.getKey(), artifact2);

      logDatasetRequest =
          LogDatasets.newBuilder()
              .setId(experimentRun.getId())
              .setOverwrite(true)
              .addDatasets(artifact1)
              .addDatasets(artifact2)
              .build();
      experimentRunServiceStub.logDatasets(logDatasetRequest);

      response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info("LogDataset Response : \n" + response.getExperimentRun());

      for (Artifact datasetArtifact : response.getExperimentRun().getDatasetsList()) {
        assertEquals(
            "Experiment datasets not match with expected datasets",
            artifactMap.get(datasetArtifact.getKey()),
            datasetArtifact);
      }

      assertNotEquals(
          "ExperimentRun date_updated field not update on database",
          experimentRun.getDateUpdated(),
          response.getExperimentRun().getDateUpdated());

      DeleteDataset deleteDataset =
          DeleteDataset.newBuilder().setId(createDatasetResponse.getDataset().getId()).build();
      DeleteDataset.Response deleteDatasetResponse = datasetServiceStub.deleteDataset(deleteDataset);
      LOGGER.info("Dataset deleted successfully");
      LOGGER.info(deleteDatasetResponse.toString());
      assertTrue(deleteDatasetResponse.getStatus());

      DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
      DeleteProject.Response deleteProjectResponse = projectServiceStub.deleteProject(deleteProject);
      LOGGER.info("Project deleted successfully");
      LOGGER.info(deleteProjectResponse.toString());
      assertTrue(deleteProjectResponse.getStatus());

      LOGGER.info("Log Datasets in ExperimentRun tags test stop................................");
    }
  */
  @Test
  public void k_logDatasetsNegativeTest() {
    LOGGER.info(
        " Log Datasets in ExperimentRun Negative test start................................");

    List<Artifact> artifacts = new ArrayList<>();
    Artifact artifact1 =
        Artifact.newBuilder()
            .setKey("Google Pay datasets " + Calendar.getInstance().getTimeInMillis())
            .setPath("This is new added data artifact type in Google Pay datasets")
            .setArtifactType(ArtifactType.MODEL)
            .build();
    artifacts.add(artifact1);
    Artifact artifact2 =
        Artifact.newBuilder()
            .setKey("Google Pay datasets " + Calendar.getInstance().getTimeInMillis())
            .setPath("This is new added data artifact type in Google Pay datasets")
            .setArtifactType(ArtifactType.DATA)
            .build();
    artifacts.add(artifact2);

    LogDatasets logDatasetRequest = LogDatasets.newBuilder().addAllDatasets(artifacts).build();

    try {
      experimentRunServiceStub.logDatasets(logDatasetRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logDatasetRequest = LogDatasets.newBuilder().setId("sdsdsa").addAllDatasets(artifacts).build();

    try {
      experimentRunServiceStub.logDatasets(logDatasetRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    logDatasetRequest =
        LogDatasets.newBuilder()
            .setId(experimentRun.getId())
            .addAllDatasets(experimentRun.getDatasetsList())
            .build();

    try {
      experimentRunServiceStub.logDatasets(logDatasetRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Datasets in ExperimentRun tags Negative test stop................................");
  }

  @Test
  public void l_getDatasetsTest() {
    LOGGER.info("Get Datasets from ExperimentRun test start................................");

    GetDatasets getDatasetsRequest = GetDatasets.newBuilder().setId(experimentRun.getId()).build();

    GetDatasets.Response response = experimentRunServiceStub.getDatasets(getDatasetsRequest);
    LOGGER.info("GetDatasets Response : " + response.getDatasetsCount());
    assertEquals(
        "Experiment datasets not match with expected datasets",
        experimentRun.getDatasetsList(),
        response.getDatasetsList());

    LOGGER.info("Get Datasets from ExperimentRun tags test stop................................");
  }

  @Test
  public void l_getDatasetsNegativeTest() {
    LOGGER.info(
        "Get Datasets from ExperimentRun Negative test start................................");

    GetDatasets getDatasetsRequest = GetDatasets.newBuilder().build();

    try {
      experimentRunServiceStub.getDatasets(getDatasetsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getDatasetsRequest = GetDatasets.newBuilder().setId("sdfsdfdsf").build();

    try {
      experimentRunServiceStub.getDatasets(getDatasetsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(Status.NOT_FOUND.getCode().equals(status.getCode()));
    }

    LOGGER.info(
        "Get Datasets from ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void m_logArtifactTest() {
    LOGGER.info(" Log Artifact in ExperimentRun test start................................");

    Artifact artifact =
        Artifact.newBuilder()
            .setKey("Google Pay Artifact " + Calendar.getInstance().getTimeInMillis())
            .setPath("46513216546" + Calendar.getInstance().getTimeInMillis())
            .setArtifactType(ArtifactType.TENSORBOARD)
            .build();

    LogArtifact logArtifactRequest =
        LogArtifact.newBuilder().setId(experimentRun.getId()).setArtifact(artifact).build();

    experimentRunServiceStub.logArtifact(logArtifactRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogArtifact Response : " + response.getExperimentRun().getArtifactsCount());
    assertEquals(
        "Experiment artifact count not match with expected artifact count",
        experimentRun.getArtifactsCount() + 1,
        response.getExperimentRun().getArtifactsCount());

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Artifact in ExperimentRun tags test stop................................");
  }

  @Test
  public void m_logArtifactNegativeTest() {
    LOGGER.info(
        " Log Artifact in ExperimentRun Negative test start................................");

    Artifact artifact =
        Artifact.newBuilder()
            .setKey("Google Pay Artifact")
            .setPath("46513216546" + Calendar.getInstance().getTimeInMillis())
            .setArtifactType(ArtifactType.TENSORBOARD)
            .build();

    LogArtifact logArtifactRequest = LogArtifact.newBuilder().setArtifact(artifact).build();
    try {
      experimentRunServiceStub.logArtifact(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logArtifactRequest = LogArtifact.newBuilder().setId("asda").setArtifact(artifact).build();
    try {
      experimentRunServiceStub.logArtifact(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    logArtifactRequest =
        LogArtifact.newBuilder()
            .setId(experimentRun.getId())
            .setArtifact(experimentRun.getArtifactsList().get(0))
            .build();
    try {
      experimentRunServiceStub.logArtifact(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Artifact in ExperimentRun tags Negative test stop................................");
  }

  @Test
  public void m_logArtifactsTest() {
    LOGGER.info(" Log Artifacts in ExperimentRun test start................................");

    List<Artifact> artifacts = new ArrayList<>();
    Artifact artifact1 =
        Artifact.newBuilder()
            .setKey("Google Pay Artifact " + Calendar.getInstance().getTimeInMillis())
            .setPath("This is new added data artifact type in Google Pay artifact")
            .setArtifactType(ArtifactType.MODEL)
            .build();
    artifacts.add(artifact1);
    Artifact artifact2 =
        Artifact.newBuilder()
            .setKey("Google Pay Artifact " + Calendar.getInstance().getTimeInMillis())
            .setPath("This is new added data artifact type in Google Pay artifact")
            .setArtifactType(ArtifactType.DATA)
            .build();
    artifacts.add(artifact2);

    LogArtifacts logArtifactRequest =
        LogArtifacts.newBuilder().setId(experimentRun.getId()).addAllArtifacts(artifacts).build();

    experimentRunServiceStub.logArtifacts(logArtifactRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogArtifact Response : \n" + response.getExperimentRun());
    assertEquals(
        "ExperimentRun artifacts not match with expected artifacts",
        (experimentRun.getArtifactsCount() + artifacts.size()),
        response.getExperimentRun().getArtifactsList().size());

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Artifacts in ExperimentRun tags test stop................................");
  }

  @Test
  public void m_logArtifactsNegativeTest() {
    LOGGER.info(
        " Log Artifacts in ExperimentRun Negative test start................................");

    List<Artifact> artifacts = new ArrayList<>();
    Artifact artifact1 =
        Artifact.newBuilder()
            .setKey("Google Pay Artifact " + Calendar.getInstance().getTimeInMillis())
            .setPath("This is new added data artifact type in Google Pay artifact")
            .setArtifactType(ArtifactType.MODEL)
            .build();
    artifacts.add(artifact1);
    Artifact artifact2 =
        Artifact.newBuilder()
            .setKey("Google Pay Artifact " + Calendar.getInstance().getTimeInMillis())
            .setPath("This is new added data artifact type in Google Pay artifact")
            .setArtifactType(ArtifactType.DATA)
            .build();
    artifacts.add(artifact2);

    LogArtifacts logArtifactRequest = LogArtifacts.newBuilder().addAllArtifacts(artifacts).build();
    try {
      experimentRunServiceStub.logArtifacts(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logArtifactRequest = LogArtifacts.newBuilder().setId("asda").addAllArtifacts(artifacts).build();
    try {
      experimentRunServiceStub.logArtifacts(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    logArtifactRequest =
        LogArtifacts.newBuilder()
            .setId(experimentRun.getId())
            .addAllArtifacts(experimentRun.getArtifactsList())
            .build();
    try {
      experimentRunServiceStub.logArtifacts(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Artifacts in ExperimentRun tags Negative test stop................................");
  }

  @Test
  public void n_getArtifactsTest() {
    LOGGER.info("Get Artifacts from ExperimentRun test start................................");

    m_logArtifactsTest();
    GetArtifacts getArtifactsRequest =
        GetArtifacts.newBuilder().setId(experimentRun.getId()).build();

    GetArtifacts.Response response = experimentRunServiceStub.getArtifacts(getArtifactsRequest);

    LOGGER.info("GetArtifacts Response : " + response.getArtifactsCount());
    assertEquals(
        "ExperimentRun artifacts not matched with expected artifacts",
        experimentRun.getArtifactsList(),
        response.getArtifactsList());

    LOGGER.info("Get Artifacts from ExperimentRun tags test stop................................");
  }

  @Test
  public void n_getArtifactsNegativeTest() {
    LOGGER.info(
        "Get Artifacts from ExperimentRun Negative test start................................");

    GetArtifacts getArtifactsRequest = GetArtifacts.newBuilder().build();

    try {
      experimentRunServiceStub.getArtifacts(getArtifactsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getArtifactsRequest = GetArtifacts.newBuilder().setId("dssaa").build();

    try {
      experimentRunServiceStub.getArtifacts(getArtifactsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get Artifacts from ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void o_logHyperparameterTest() {
    LOGGER.info(" Log Hyperparameter in ExperimentRun test start................................");

    Value blobValue = Value.newBuilder().setStringValue("this is a blob data example").build();
    KeyValue hyperparameter =
        KeyValue.newBuilder()
            .setKey("Log new hyperparameter " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();

    LogHyperparameter logHyperparameterRequest =
        LogHyperparameter.newBuilder()
            .setId(experimentRun.getId())
            .setHyperparameter(hyperparameter)
            .build();

    experimentRunServiceStub.logHyperparameter(logHyperparameterRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogHyperparameter Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun hyperparameter not match with expected hyperparameter",
        response.getExperimentRun().getHyperparametersList().contains(hyperparameter));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info(
        "Log Hyperparameter in ExperimentRun tags test stop................................");
  }

  @Test
  public void o_logHyperparameterNegativeTest() {
    LOGGER.info(
        " Log Hyperparameter in ExperimentRun Negative test start................................");

    Value blobValue = Value.newBuilder().setStringValue("this is a blob data example").build();
    KeyValue hyperparameter =
        KeyValue.newBuilder()
            .setKey("Log new hyperparameter " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();

    LogHyperparameter logHyperparameterRequest =
        LogHyperparameter.newBuilder().setHyperparameter(hyperparameter).build();

    try {
      experimentRunServiceStub.logHyperparameter(logHyperparameterRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logHyperparameterRequest =
        LogHyperparameter.newBuilder().setId("dsdsfs").setHyperparameter(hyperparameter).build();

    try {
      experimentRunServiceStub.logHyperparameter(logHyperparameterRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(Status.NOT_FOUND.getCode().equals(status.getCode()));
    }

    logHyperparameterRequest =
        LogHyperparameter.newBuilder()
            .setId(experimentRun.getId())
            .setHyperparameter(experimentRun.getHyperparametersList().get(0))
            .build();

    try {
      experimentRunServiceStub.logHyperparameter(logHyperparameterRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Hyperparameter in ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void o_logHyperparametersTest() {
    LOGGER.info(" Log Hyperparameters in ExperimentRun test start................................");

    List<KeyValue> hyperparameters = new ArrayList<>();
    Value blobValue = Value.newBuilder().setStringValue("this is a blob data example").build();
    KeyValue hyperparameter1 =
        KeyValue.newBuilder()
            .setKey("Log new hyperparameter 1 " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();
    hyperparameters.add(hyperparameter1);

    Value numValue = Value.newBuilder().setNumberValue(12.02125212).build();
    KeyValue hyperparameter2 =
        KeyValue.newBuilder()
            .setKey("Log new hyperparameter 2 " + Calendar.getInstance().getTimeInMillis())
            .setValue(numValue)
            .setValueType(ValueType.NUMBER)
            .build();
    hyperparameters.add(hyperparameter2);

    LogHyperparameters logHyperparameterRequest =
        LogHyperparameters.newBuilder()
            .setId(experimentRun.getId())
            .addAllHyperparameters(hyperparameters)
            .build();

    experimentRunServiceStub.logHyperparameters(logHyperparameterRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogHyperparameters Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun hyperparameters not match with expected hyperparameters",
        response.getExperimentRun().getHyperparametersList().containsAll(hyperparameters));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info(
        "Log Hyperparameters in ExperimentRun tags test stop................................");
  }

  @Test
  public void o_logHyperparametersNegativeTest() {
    LOGGER.info(
        " Log Hyperparameters in ExperimentRun Negative test start................................");

    List<KeyValue> hyperparameters = new ArrayList<>();
    Value blobValue = Value.newBuilder().setStringValue("this is a blob data example").build();
    KeyValue hyperparameter1 =
        KeyValue.newBuilder()
            .setKey("Log new hyperparameter " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();
    hyperparameters.add(hyperparameter1);

    Value numValue = Value.newBuilder().setNumberValue(12.02125212).build();
    KeyValue hyperparameter2 =
        KeyValue.newBuilder()
            .setKey("Log new hyperparameter " + Calendar.getInstance().getTimeInMillis())
            .setValue(numValue)
            .setValueType(ValueType.NUMBER)
            .build();
    hyperparameters.add(hyperparameter2);

    LogHyperparameters logHyperparameterRequest =
        LogHyperparameters.newBuilder().addAllHyperparameters(hyperparameters).build();

    try {
      experimentRunServiceStub.logHyperparameters(logHyperparameterRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logHyperparameterRequest =
        LogHyperparameters.newBuilder()
            .setId("dsdsfs")
            .addAllHyperparameters(hyperparameters)
            .build();

    try {
      experimentRunServiceStub.logHyperparameters(logHyperparameterRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    logHyperparameterRequest =
        LogHyperparameters.newBuilder()
            .setId(experimentRun.getId())
            .addAllHyperparameters(experimentRun.getHyperparametersList())
            .build();

    try {
      experimentRunServiceStub.logHyperparameters(logHyperparameterRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info("Log Hyperparameters in ExperimentRun Negative tags test stop.....");
  }

  @Test
  public void p_getHyperparametersTest() {
    LOGGER.info("Get Hyperparameters from ExperimentRun test start........");

    o_logHyperparametersTest();
    GetHyperparameters getHyperparametersRequest =
        GetHyperparameters.newBuilder().setId(experimentRun.getId()).build();

    GetHyperparameters.Response response =
        experimentRunServiceStub.getHyperparameters(getHyperparametersRequest);

    LOGGER.info("GetHyperparameters Response : " + response.getHyperparametersCount());
    assertEquals(
        "ExperimentRun Hyperparameters not match with expected hyperparameters",
        experimentRun.getHyperparametersList(),
        response.getHyperparametersList());

    LOGGER.info("Get Hyperparameters from ExperimentRun tags test stop......");
  }

  @Test
  public void p_getHyperparametersNegativeTest() {
    LOGGER.info("Get Hyperparameters from ExperimentRun Negative test start..........");

    GetHyperparameters getHyperparametersRequest = GetHyperparameters.newBuilder().build();

    try {
      experimentRunServiceStub.getHyperparameters(getHyperparametersRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getHyperparametersRequest = GetHyperparameters.newBuilder().setId("sdsssd").build();

    try {
      experimentRunServiceStub.getHyperparameters(getHyperparametersRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(Status.NOT_FOUND.getCode().equals(status.getCode()));
    }

    LOGGER.info(
        "Get Hyperparameters from ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void q_logAttributeTest() {
    LOGGER.info(" Log Attribute in ExperimentRun test start................................");

    Value blobValue =
        Value.newBuilder().setStringValue("this is a blob data example of attribute").build();
    KeyValue attribute =
        KeyValue.newBuilder()
            .setKey("Log new attribute " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();

    LogAttribute logAttributeRequest =
        LogAttribute.newBuilder().setId(experimentRun.getId()).setAttribute(attribute).build();

    experimentRunServiceStub.logAttribute(logAttributeRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogAttribute Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun attribute not match with expected attribute",
        response.getExperimentRun().getAttributesList().contains(attribute));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Attribute in ExperimentRun tags test stop................................");
  }

  @Test
  public void q_logAttributeNegativeTest() {
    LOGGER.info(
        " Log Attribute in ExperimentRun Negative test start................................");

    Value blobValue =
        Value.newBuilder().setStringValue("this is a blob data example of attribute").build();
    KeyValue attribute =
        KeyValue.newBuilder()
            .setKey("Log new attribute " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();

    LogAttribute logAttributeRequest = LogAttribute.newBuilder().setAttribute(attribute).build();

    try {
      experimentRunServiceStub.logAttribute(logAttributeRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logAttributeRequest = LogAttribute.newBuilder().setId("sdsds").setAttribute(attribute).build();

    try {
      experimentRunServiceStub.logAttribute(logAttributeRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(Status.NOT_FOUND.getCode().equals(status.getCode()));
    }

    logAttributeRequest =
        LogAttribute.newBuilder()
            .setId(experimentRun.getId())
            .setAttribute(experimentRun.getAttributesList().get(0))
            .build();

    try {
      experimentRunServiceStub.logAttribute(logAttributeRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Attribute in ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void q_logAttributesTest() {
    LOGGER.info(" Log Attributes in ExperimentRun test start................................");

    List<KeyValue> attributes = new ArrayList<>();
    Value blobValue =
        Value.newBuilder().setStringValue("this is a blob data example of attribute").build();
    KeyValue attribute1 =
        KeyValue.newBuilder()
            .setKey("Log new attribute 1 " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();
    attributes.add(attribute1);
    Value stringValue =
        Value.newBuilder().setStringValue("this is a blob data example of attribute").build();
    KeyValue attribute2 =
        KeyValue.newBuilder()
            .setKey("Log new attribute 2 " + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build();
    attributes.add(attribute2);

    LogAttributes logAttributeRequest =
        LogAttributes.newBuilder()
            .setId(experimentRun.getId())
            .addAllAttributes(attributes)
            .build();

    experimentRunServiceStub.logAttributes(logAttributeRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogAttributes Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun attributes not match with expected attributes",
        response.getExperimentRun().getAttributesList().containsAll(attributes));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log Attributes in ExperimentRun tags test stop................................");
  }

  @Test
  public void q_logAttributesNegativeTest() {
    LOGGER.info(
        " Log Attributes in ExperimentRun Negative test start................................");

    List<KeyValue> attributes = new ArrayList<>();
    Value blobValue =
        Value.newBuilder().setStringValue("this is a blob data example of attribute").build();
    KeyValue attribute1 =
        KeyValue.newBuilder()
            .setKey("Log new attribute " + Calendar.getInstance().getTimeInMillis())
            .setValue(blobValue)
            .setValueType(ValueType.BLOB)
            .build();
    attributes.add(attribute1);
    Value stringValue =
        Value.newBuilder().setStringValue("this is a blob data example of attribute").build();
    KeyValue attribute2 =
        KeyValue.newBuilder()
            .setKey("Log new attribute " + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build();
    attributes.add(attribute2);

    LogAttributes logAttributeRequest =
        LogAttributes.newBuilder().addAllAttributes(attributes).build();

    try {
      experimentRunServiceStub.logAttributes(logAttributeRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logAttributeRequest =
        LogAttributes.newBuilder().setId("sdsds").addAllAttributes(attributes).build();

    try {
      experimentRunServiceStub.logAttributes(logAttributeRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    logAttributeRequest =
        LogAttributes.newBuilder()
            .setId(experimentRun.getId())
            .addAllAttributes(experimentRun.getAttributesList())
            .build();

    try {
      experimentRunServiceStub.logAttributes(logAttributeRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Attributes in ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void qq_addExperimentRunAttributes() {
    LOGGER.info("Add ExperimentRun Attributes test start................................");

    List<KeyValue> attributeList = new ArrayList<>();
    Value intValue = Value.newBuilder().setNumberValue(1.1).build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_1" + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build());
    Value stringValue =
        Value.newBuilder()
            .setStringValue("attributes_value_" + Calendar.getInstance().getTimeInMillis())
            .build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_2" + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.BLOB)
            .build());

    AddExperimentRunAttributes request =
        AddExperimentRunAttributes.newBuilder()
            .setId(experimentRun.getId())
            .addAllAttributes(attributeList)
            .build();

    experimentRunServiceStub.addExperimentRunAttributes(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("AddExperimentRunAttributes Response : \n" + response.getExperimentRun());
    assertTrue(
        "ExperimentRun attributes not match with expected attributes",
        response.getExperimentRun().getAttributesList().containsAll(attributeList));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Add ExperimentRun Attributes test stop................................");
  }

  @Test
  public void qq_addExperimentRunAttributesNegativeTest() {
    LOGGER.info("Add ExperimentRun attributes Negative test start................................");

    AddExperimentRunAttributes request =
        AddExperimentRunAttributes.newBuilder().setId("xyz").build();

    try {
      experimentRunServiceStub.addExperimentRunAttributes(request);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    AddExperimentRunAttributes addAttributesRequest =
        AddExperimentRunAttributes.newBuilder().build();
    try {
      experimentRunServiceStub.addExperimentRunAttributes(addAttributesRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add ExperimentRun attributes Negative test stop................................");
  }

  @Test
  public void r_getExperimentRunAttributesTest() {
    LOGGER.info("Get Attributes from ExperimentRun test start................................");

    q_logAttributesTest();
    List<KeyValue> attributes = experimentRun.getAttributesList();
    LOGGER.info("Attributes size : " + attributes.size());

    if (attributes.size() == 0) {
      LOGGER.warn("Experiment Attributes not found in database ");
      fail();
      return;
    }

    List<String> keys = new ArrayList<>();
    if (attributes.size() > 1) {
      for (int index = 0; index < attributes.size() - 1; index++) {
        KeyValue keyValue = attributes.get(index);
        keys.add(keyValue.getKey());
      }
    } else {
      keys.add(attributes.get(0).getKey());
    }
    LOGGER.info("Attributes key size : " + keys.size());

    GetAttributes getAttributesRequest =
        GetAttributes.newBuilder().setId(experimentRun.getId()).addAllAttributeKeys(keys).build();

    GetAttributes.Response response =
        experimentRunServiceStub.getExperimentRunAttributes(getAttributesRequest);

    LOGGER.info("GetAttributes Response : " + response.getAttributesCount());
    for (KeyValue attributeKeyValue : response.getAttributesList()) {
      assertTrue(
          "Experiment attribute not match with expected attribute",
          keys.contains(attributeKeyValue.getKey()));
    }

    getAttributesRequest =
        GetAttributes.newBuilder().setId(experimentRun.getId()).setGetAll(true).build();

    response = experimentRunServiceStub.getExperimentRunAttributes(getAttributesRequest);

    LOGGER.info("GetAttributes Response : " + response.getAttributesCount());
    assertEquals(attributes.size(), response.getAttributesList().size());

    LOGGER.info("Get Attributes from ExperimentRun tags test stop................................");
  }

  @Test
  public void r_getExperimentRunAttributesNegativeTest() {
    LOGGER.info(
        "Get Attributes from ExperimentRun Negative test start................................");

    GetAttributes getAttributesRequest = GetAttributes.newBuilder().build();

    try {
      experimentRunServiceStub.getExperimentRunAttributes(getAttributesRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getAttributesRequest = GetAttributes.newBuilder().setId("dssdds").setGetAll(true).build();

    try {
      experimentRunServiceStub.getExperimentRunAttributes(getAttributesRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get Attributes from ExperimentRun Negative tags test stop................................");
  }

  @Test
  public void rrr_deleteExperimentRunAttributes() {
    LOGGER.info("Delete ExperimentRun Attributes test start................................");

    q_logAttributesTest();
    List<KeyValue> attributes = experimentRun.getAttributesList();
    LOGGER.info("Attributes size : " + attributes.size());
    List<String> keys = new ArrayList<>();
    for (int index = 0; index < attributes.size() - 1; index++) {
      KeyValue keyValue = attributes.get(index);
      keys.add(keyValue.getKey());
    }
    LOGGER.info("Attributes key size : " + keys.size());

    DeleteExperimentRunAttributes request =
        DeleteExperimentRunAttributes.newBuilder()
            .setId(experimentRun.getId())
            .addAllAttributeKeys(keys)
            .build();

    experimentRunServiceStub.deleteExperimentRunAttributes(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info(
        "DeleteExperimentRunAttributes Response : \n"
            + response.getExperimentRun().getAttributesList());
    assertTrue(response.getExperimentRun().getAttributesList().size() <= 1);

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    if (response.getExperimentRun().getAttributesList().size() != 0) {
      request =
          DeleteExperimentRunAttributes.newBuilder()
              .setId(experimentRun.getId())
              .setDeleteAll(true)
              .build();

      experimentRunServiceStub.deleteExperimentRunAttributes(request);

      response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info(
          "DeleteExperimentRunAttributes Response : \n"
              + response.getExperimentRun().getAttributesList());
      assertEquals(0, response.getExperimentRun().getAttributesList().size());

      assertNotEquals(
          "ExperimentRun date_updated field not update on database",
          experimentRun.getDateUpdated(),
          response.getExperimentRun().getDateUpdated());
      experimentRun = response.getExperimentRun();
      experimentRunMap.put(experimentRun.getId(), experimentRun);
    }

    LOGGER.info("Delete ExperimentRun Attributes test stop................................");
  }

  @Test
  public void rrr_deleteExperimentRunAttributesNegativeTest() {
    LOGGER.info(
        "Delete ExperimentRun Attributes Negative test start................................");

    DeleteExperimentRunAttributes request = DeleteExperimentRunAttributes.newBuilder().build();

    try {
      experimentRunServiceStub.deleteExperimentRunAttributes(request);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    request =
        DeleteExperimentRunAttributes.newBuilder()
            .setId(experimentRun.getId())
            .setDeleteAll(true)
            .build();

    experimentRunServiceStub.deleteExperimentRunAttributes(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info(
        "DeleteExperimentRunAttributes Response : \n"
            + response.getExperimentRun().getAttributesList());
    assertEquals(0, response.getExperimentRun().getAttributesList().size());

    LOGGER.info(
        "Delete ExperimentRun Attributes Negative test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: sortExperimentRuns endpoint")
  public void t_sortExperimentRunsTest() {
    LOGGER.info("SortExperimentRuns test start................................");

    List<String> experimentRunIds = new ArrayList<>();
    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      KeyValue metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.012).build())
              .build();
      KeyValue metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.99).build())
              .build();
      KeyValue hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(9).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun11 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun11.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun11.getName());

      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.31).build())
              .build();
      metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.31).build())
              .build();
      hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(7).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun12 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun12.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun12.getName());

      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
              .build();
      metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
              .build();
      hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(4.55).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun21 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun21.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun21.getName());

      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(1.00).build())
              .build();
      metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.001212).build())
              .build();
      hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(2.545).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun22 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun22.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun22.getName());

      SortExperimentRuns sortExperimentRuns =
          SortExperimentRuns.newBuilder()
              .addAllExperimentRunIds(experimentRunIds)
              .setSortKey("metrics.accuracy")
              .setAscending(true)
              .build();

      SortExperimentRuns.Response response =
          experimentRunServiceStub.sortExperimentRuns(sortExperimentRuns);
      LOGGER.info("SortExperimentRuns Response : " + response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          4,
          response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun22,
          response.getExperimentRunsList().get(0));
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun11,
          response.getExperimentRunsList().get(3));

      try {
        sortExperimentRuns =
            SortExperimentRuns.newBuilder()
                .addAllExperimentRunIds(experimentRunIds)
                .setSortKey("observations.attribute.attr_1")
                .setAscending(true)
                .build();

        experimentRunServiceStub.sortExperimentRuns(sortExperimentRuns);
        fail();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        LOGGER.warn(
            "Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      }

      sortExperimentRuns =
          SortExperimentRuns.newBuilder()
              .addAllExperimentRunIds(experimentRunIds)
              .setSortKey("metrics.accuracy")
              .setAscending(true)
              .setIdsOnly(true)
              .build();

      response = experimentRunServiceStub.sortExperimentRuns(sortExperimentRuns);
      LOGGER.info("SortExperimentRuns Response : " + response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          4,
          response.getExperimentRunsCount());

      for (int index = 0; index < response.getExperimentRunsCount(); index++) {
        ExperimentRun experimentRun = response.getExperimentRunsList().get(index);
        if (index == 0) {
          assertNotEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRun22,
              experimentRun);
          assertEquals(
              "ExperimentRun Id not match with expected experimentRun Id",
              experimentRun22.getId(),
              experimentRun.getId());
        } else if (index == 1) {
          assertNotEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRun12,
              experimentRun);
          assertEquals(
              "ExperimentRun Id not match with expected experimentRun Id",
              experimentRun12.getId(),
              experimentRun.getId());
        } else if (index == 2) {
          assertNotEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRun21,
              experimentRun);
          assertEquals(
              "ExperimentRun Id not match with expected experimentRun Id",
              experimentRun21.getId(),
              experimentRun.getId());
        } else if (index == 3) {
          assertNotEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRun11,
              experimentRun);
          assertEquals(
              "ExperimentRun Id not match with expected experimentRun Id",
              experimentRun11.getId(),
              experimentRun.getId());
        }
      }
    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("SortExperimentRuns test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: sortExperimentRuns endpoint")
  public void t_sortExperimentRunsNegativeTest() {
    LOGGER.info("SortExperimentRuns Negative test start................................");

    SortExperimentRuns sortExperimentRuns =
        SortExperimentRuns.newBuilder().setSortKey("end_time").setIdsOnly(true).build();

    try {
      experimentRunServiceStub.sortExperimentRuns(sortExperimentRuns);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    try {
      List<String> experimentRunIds = new ArrayList<>();
      experimentRunIds.add("abc");
      experimentRunIds.add("xyz");
      sortExperimentRuns =
          SortExperimentRuns.newBuilder()
              .addAllExperimentRunIds(experimentRunIds)
              .setSortKey("end_time")
              .build();

      experimentRunServiceStub.sortExperimentRuns(sortExperimentRuns);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    LOGGER.info("SortExperimentRuns Negative test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: getTopExperimentRuns endpoint")
  public void u_getTopExperimentRunsTest() {
    LOGGER.info("TopExperimentRuns test start................................");

    List<String> experimentRunIds = new ArrayList<>();
    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      KeyValue metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.012).build())
              .build();
      KeyValue metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.99).build())
              .build();
      KeyValue hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(9).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setCodeVersion("1")
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun11 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun11.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun11.getName());

      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.31).build())
              .build();
      metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.31).build())
              .build();
      hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(7).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setCodeVersion("2")
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun12 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun12.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun12.getName());

      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
              .build();
      metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
              .build();
      hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(4.55).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setCodeVersion("3")
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun21 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun21.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun21.getName());

      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(1.00).build())
              .build();
      metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.001212).build())
              .build();
      hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(2.545).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setCodeVersion("4")
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun22 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun22.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun22.getName());

      TopExperimentRunsSelector topExperimentRunsSelector =
          TopExperimentRunsSelector.newBuilder()
              .addAllExperimentRunIds(experimentRunIds)
              .setSortKey("metrics.accuracy")
              .setTopK(3)
              .setAscending(true)
              .build();

      TopExperimentRunsSelector.Response response =
          experimentRunServiceStub.getTopExperimentRuns(topExperimentRunsSelector);
      LOGGER.info("TopExperimentRunsSelector Response : " + response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          3,
          response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun22,
          response.getExperimentRunsList().get(0));
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun21,
          response.getExperimentRunsList().get(2));

      topExperimentRunsSelector =
          TopExperimentRunsSelector.newBuilder()
              .setExperimentId(experiment.getId())
              .setSortKey("hyperparameters.tuning")
              .setTopK(20000)
              .setAscending(true)
              .setIdsOnly(true)
              .build();

      response = experimentRunServiceStub.getTopExperimentRuns(topExperimentRunsSelector);
      LOGGER.info("TopExperimentRunsSelector Response : " + response.getExperimentRunsCount());

      List<ExperimentRun> responseExperimentRuns = new ArrayList<>();
      for (ExperimentRun resExperimentRun : response.getExperimentRunsList()) {
        if (experimentRunIds.contains(resExperimentRun.getId())) {
          responseExperimentRuns.add(resExperimentRun);
        }
      }

      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          2,
          responseExperimentRuns.size());
      assertNotEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun12,
          responseExperimentRuns.get(0));
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun12.getId(),
          responseExperimentRuns.get(0).getId());
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun11.getId(),
          responseExperimentRuns.get(1).getId());

      topExperimentRunsSelector =
          TopExperimentRunsSelector.newBuilder()
              .addAllExperimentRunIds(experimentRunIds)
              .setSortKey("code_version")
              .setTopK(3)
              .setIdsOnly(true)
              .build();

      response = experimentRunServiceStub.getTopExperimentRuns(topExperimentRunsSelector);
      responseExperimentRuns = new ArrayList<>();
      for (ExperimentRun resExperimentRun : response.getExperimentRunsList()) {
        if (experimentRunIds.contains(resExperimentRun.getId())) {
          responseExperimentRuns.add(resExperimentRun);
        }
      }
      LOGGER.info("TopExperimentRunsSelector Response : " + response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          3,
          responseExperimentRuns.size());
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun22.getId(),
          responseExperimentRuns.get(0).getId());
      assertNotEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun12,
          responseExperimentRuns.get(2));
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun12.getId(),
          responseExperimentRuns.get(2).getId());
    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("TopExperimentRuns test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: getTopExperimentRuns endpoint")
  public void u_getTopExperimentRunsNegativeTest() {
    LOGGER.info("TopExperimentRuns Negative test start................................");

    TopExperimentRunsSelector topExperimentRunsSelector =
        TopExperimentRunsSelector.newBuilder().setTopK(4).setAscending(true).build();

    try {
      topExperimentRunsSelector =
          TopExperimentRunsSelector.newBuilder()
              .setProjectId("12321")
              .setSortKey("endTime")
              .build();
      experimentRunServiceStub.getTopExperimentRuns(topExperimentRunsSelector);
      fail();
    } catch (StatusRuntimeException ex) {
      checkEqualsAssert(ex);
    }

    try {
      List<String> experimentRunIds = new ArrayList<>();
      experimentRunIds.add("abc");
      experimentRunIds.add("xyz");
      topExperimentRunsSelector =
          TopExperimentRunsSelector.newBuilder()
              .addAllExperimentRunIds(experimentRunIds)
              .setSortKey("end_time")
              .setTopK(3)
              .build();

      experimentRunServiceStub.getTopExperimentRuns(topExperimentRunsSelector);
      fail();
    } catch (StatusRuntimeException exce) {
      Status status = Status.fromThrowable(exce);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    LOGGER.info("TopExperimentRuns Negative test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: logJobId endpoint")
  public void v_logJobIdTest() {
    LOGGER.info(" Log Job Id in ExperimentRun test start................................");

    String jobId = "xyz";
    LogJobId logJobIdRequest =
        LogJobId.newBuilder().setId(experimentRun.getId()).setJobId(jobId).build();

    experimentRunServiceStub.logJobId(logJobIdRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogJobId Response : \n" + response.getExperimentRun());
    assertEquals(
        "Job Id not match with expected job Id", jobId, response.getExperimentRun().getJobId());

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    LOGGER.info("Log Job Id in ExperimentRun test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: logJobId endpoint")
  public void v_logJobIdNegativeTest() {
    LOGGER.info(" Log Job Id in ExperimentRun Negative test start................................");

    String jobId = "xyz";
    LogJobId logJobIdRequest = LogJobId.newBuilder().setJobId(jobId).build();

    try {

      experimentRunServiceStub.logJobId(logJobIdRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    try {
      logJobIdRequest = LogJobId.newBuilder().setId("abc").build();
      experimentRunServiceStub.logJobId(logJobIdRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Log Job Id in ExperimentRun Negative test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: logJobId endpoint")
  public void w_getJobIdTest() {
    LOGGER.info(" Get Job Id in ExperimentRun test start................................");

    String jobId = "xyz";
    LogJobId logJobIdRequest =
        LogJobId.newBuilder().setId(experimentRun.getId()).setJobId(jobId).build();

    experimentRunServiceStub.logJobId(logJobIdRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response getExpRunResponse =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info("LogJobId Response : \n" + getExpRunResponse.getExperimentRun());
    assertEquals(
        "Job Id not match with expected job Id",
        jobId,
        getExpRunResponse.getExperimentRun().getJobId());

    GetJobId getJobIdRequest = GetJobId.newBuilder().setId(experimentRun.getId()).build();

    GetJobId.Response response = experimentRunServiceStub.getJobId(getJobIdRequest);

    LOGGER.info("GetJobId Response : \n" + response.getJobId());
    assertEquals("Job Id not match with expected job Id", "xyz", response.getJobId());

    LOGGER.info("Get Job Id in ExperimentRun test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: logJobId endpoint")
  public void w_getJobIdNegativeTest() {
    LOGGER.info(" Get Job Id in ExperimentRun Negative test start................................");

    GetJobId getJobIdRequest = GetJobId.newBuilder().build();

    try {
      experimentRunServiceStub.getJobId(getJobIdRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getJobIdRequest = GetJobId.newBuilder().setId("dssdds").build();

    try {
      experimentRunServiceStub.getJobId(getJobIdRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(
          Status.NOT_FOUND.getCode().equals(status.getCode())
              || Status.UNAUTHENTICATED.getCode().equals(status.getCode()));
    }

    LOGGER.info("Get Job Id in ExperimentRun Negative test stop................................");
  }

  /*@Test
  public void x_getURLForArtifact() {
    LOGGER.info("Get Url for Artifact test start................................");
    ProjectTest projectTest = new ProjectTest();
    ExperimentTest experimentTest = new ExperimentTest();

    ProjectServiceBlockingStub projectServiceStub = ProjectServiceGrpc.newBlockingStub(channel);
    ExperimentServiceBlockingStub experimentServiceStub =
        ExperimentServiceGrpc.newBlockingStub(channel);
    ExperimentRunServiceBlockingStub experimentRunServiceStub =
        ExperimentRunServiceGrpc.newBlockingStub(channel);

    // Create project
    CreateProject createProjectRequest =
        projectTest.getCreateProjectRequest("experimentRun_project_ypcdt11");
    CreateProject.Response createProjectResponse =
        projectServiceStub.createProject(createProjectRequest);
    Project project = createProjectResponse.getProject();
    LOGGER.info("Project created successfully");

    // Create two experiment of above project
    CreateExperiment createExperimentRequest =
        experimentTest.getCreateExperimentRequest(project.getId(), "Experiment_zys");
    CreateExperiment.Response createExperimentResponse =
        experimentServiceStub.createExperiment(createExperimentRequest);
    Experiment experiment = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequest(project.getId(), experiment.getId(), "ExperimentRun_zys");
    CreateExperimentRun.Response createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
    Artifact artifact = experimentRun.getArtifacts(0);

    GetUrlForArtifact getUrlForArtifact =
        GetUrlForArtifact.newBuilder()
            .setId(experiment.getId())
            .setKey(artifact.getKey())
            .setMethod("put")
            .build();
    GetUrlForArtifact.Response getUrlForArtifactResponse =
        experimentRunServiceStub.getUrlForArtifact(getUrlForArtifact);
    String url = getUrlForArtifactResponse.getUrl();
    assertEquals("Artifact Url not match with expected artifact Url", url, url);

    DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
    DeleteProject.Response deleteProjectResponse = projectServiceStub.deleteProject(deleteProject);
    LOGGER.info(deleteProjectResponse.toString());
    assertTrue(deleteProjectResponse.getStatus());

    LOGGER.info("Get Url for Artifact test stop................................");
  }*/

  @Test
  public void z_deleteExperimentRunTest() {
    LOGGER.info("Delete ExperimentRun test start................................");

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequest(project.getId(), experiment.getId(), "ExperimentRun_n_sprt");
    CreateExperimentRun.Response createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun.getName());

    DeleteExperimentRun deleteExperimentRun =
        DeleteExperimentRun.newBuilder().setId(experimentRun.getId()).build();
    DeleteExperimentRun.Response deleteExperimentRunResponse =
        experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
    assertTrue(deleteExperimentRunResponse.getStatus());

    LOGGER.info("Delete ExperimentRun test stop................................");
  }

  @Test
  public void z_deleteExperimentRunNegativeTest() {
    LOGGER.info("Delete ExperimentRun Negative test start................................");

    DeleteExperimentRun request = DeleteExperimentRun.newBuilder().build();

    try {
      experimentRunServiceStub.deleteExperimentRun(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    request = DeleteExperimentRun.newBuilder().setId("ddsdfds").build();

    try {
      experimentRunServiceStub.deleteExperimentRun(request);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertTrue(Status.NOT_FOUND.getCode().equals(status.getCode()));
    }

    LOGGER.info("Delete ExperimentRun Negative test stop................................");
  }

  @Test
  public void createParentChildExperimentRunTest() {
    LOGGER.info("Create Parent Children ExperimentRun test start................................");

    List<String> experimentRunIds = new ArrayList<>();
    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-parent-1-" + new Date().getTime());
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun.getName());

      // Children experimentRun 1
      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-children-1-" + new Date().getTime());
      // Add Parent Id to children
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().setParentId(experimentRun.getId()).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun childrenExperimentRun1 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(childrenExperimentRun1.getId());
      LOGGER.info("ExperimentRun1 created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          childrenExperimentRun1.getName());

      // Children experimentRun 2
      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-children-2-" + new Date().getTime());
      // Add Parent Id to children
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().setParentId(experimentRun.getId()).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun childrenExperimentRun2 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(childrenExperimentRun2.getId());
      LOGGER.info("ExperimentRun1 created successfully");
      assertEquals(
          "ExperimentRun2 name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          childrenExperimentRun2.getName());

    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("Create Parent Children ExperimentRun test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: getChildrenExperimentRuns endpoint")
  public void getChildExperimentRunWithPaginationTest() {
    LOGGER.info(
        "Get Children ExperimentRun using pagination test start................................");

    List<String> experimentRunIds = new ArrayList<>();
    try {
      // Create parent experimentRun
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-parent-1-" + new Date().getTime());
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun parentExperimentRun = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(parentExperimentRun.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          parentExperimentRun.getName());

      Map<String, ExperimentRun> childrenExperimentRunMap = new HashMap<>();

      // Children experimentRun 1
      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-children-1-" + new Date().getTime());
      // Add Parent Id to children
      KeyValue metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.31).build())
              .build();
      KeyValue metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.31).build())
              .build();
      KeyValue hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(7).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setParentId(parentExperimentRun.getId())
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .setDateCreated(123456)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun childrenExperimentRun1 = createExperimentRunResponse.getExperimentRun();
      childrenExperimentRunMap.put(childrenExperimentRun1.getId(), childrenExperimentRun1);
      experimentRunIds.add(childrenExperimentRun1.getId());
      LOGGER.info("ExperimentRun1 created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          childrenExperimentRun1.getName());

      // Children experimentRun 2
      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-children-2-" + new Date().getTime());
      // Add Parent Id to children
      metric1 =
          KeyValue.newBuilder()
              .setKey("loss")
              .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
              .build();
      metric2 =
          KeyValue.newBuilder()
              .setKey("accuracy")
              .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
              .build();
      hyperparameter1 =
          KeyValue.newBuilder()
              .setKey("tuning")
              .setValue(Value.newBuilder().setNumberValue(4.55).build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setParentId(parentExperimentRun.getId())
              .addMetrics(metric1)
              .addMetrics(metric2)
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun childrenExperimentRun2 = createExperimentRunResponse.getExperimentRun();
      childrenExperimentRunMap.put(childrenExperimentRun2.getId(), childrenExperimentRun2);
      experimentRunIds.add(childrenExperimentRun2.getId());
      LOGGER.info("ExperimentRun1 created successfully");
      assertEquals(
          "ExperimentRun2 name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          childrenExperimentRun2.getName());

      int pageLimit = 1;
      boolean isExpectedResultFound = false;
      for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
        GetChildrenExperimentRuns getChildrenExperimentRunsRequest =
            GetChildrenExperimentRuns.newBuilder()
                .setExperimentRunId(parentExperimentRun.getId())
                .setPageNumber(pageNumber)
                .setPageLimit(pageLimit)
                .setAscending(true)
                .setSortKey(ModelDBConstants.NAME)
                .build();

        GetChildrenExperimentRuns.Response experimentRunResponse =
            experimentRunServiceStub.getChildrenExperimentRuns(getChildrenExperimentRunsRequest);
        assertEquals(
            "Total records count not matched with expected records count",
            2,
            experimentRunResponse.getTotalRecords());

        if (experimentRunResponse.getExperimentRunsList() != null) {
          isExpectedResultFound = true;
          for (ExperimentRun experimentRun : experimentRunResponse.getExperimentRunsList()) {
            assertEquals(
                "ExperimentRun not match with expected experimentRun",
                childrenExperimentRunMap.get(experimentRun.getId()),
                experimentRun);
          }
        } else {
          if (isExpectedResultFound) {
            LOGGER.warn("More ExperimentRun not found in database");
            assertTrue(true);
          } else {
            fail("Expected experimentRun not found in response");
          }
        }
      }

      GetChildrenExperimentRuns getChildrenExperimentRunRequest =
          GetChildrenExperimentRuns.newBuilder()
              .setExperimentRunId(parentExperimentRun.getId())
              .setPageNumber(1)
              .setPageLimit(1)
              .setAscending(false)
              .setSortKey("metrics.loss")
              .build();

      GetChildrenExperimentRuns.Response experimentRunResponse =
          experimentRunServiceStub.getChildrenExperimentRuns(getChildrenExperimentRunRequest);
      assertEquals(
          "Total records count not matched with expected records count",
          2,
          experimentRunResponse.getTotalRecords());
      assertEquals(
          "ExperimentRuns count not match with expected experimentRuns count",
          1,
          experimentRunResponse.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          childrenExperimentRun2,
          experimentRunResponse.getExperimentRuns(0));

      getChildrenExperimentRunRequest =
          GetChildrenExperimentRuns.newBuilder()
              .setExperimentRunId(parentExperimentRun.getId())
              .setPageNumber(1)
              .setPageLimit(1)
              .setAscending(true)
              .setSortKey("")
              .build();

      experimentRunResponse =
          experimentRunServiceStub.getChildrenExperimentRuns(getChildrenExperimentRunRequest);
      assertEquals(
          "ExperimentRuns count not match with expected experimentRuns count",
          1,
          experimentRunResponse.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          childrenExperimentRun1,
          experimentRunResponse.getExperimentRuns(0));
      assertEquals(
          "Total records count not matched with expected records count",
          2,
          experimentRunResponse.getTotalRecords());

      getChildrenExperimentRunRequest =
          GetChildrenExperimentRuns.newBuilder()
              .setExperimentRunId(parentExperimentRun.getId())
              .setPageNumber(1)
              .setPageLimit(1)
              .setAscending(true)
              .setSortKey("observations.attribute.attr_1")
              .build();

      try {
        experimentRunServiceStub.getChildrenExperimentRuns(getChildrenExperimentRunRequest);
        fail();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        LOGGER.warn(
            "Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      }
    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info(
        "Get Children ExperimentRun using pagination test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: setParentExperimentRunId endpoint")
  public void setParentIdOnChildExperimentRunTest() {
    LOGGER.info(
        "Set Parent ID on Children ExperimentRun test start................................");

    List<String> experimentRunIds = new ArrayList<>();
    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-parent-1-" + new Date().getTime());
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun.getName());

      // Children experimentRun 1
      createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-children-1-" + new Date().getTime());
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun childrenExperimentRun1 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(childrenExperimentRun1.getId());
      LOGGER.info("ExperimentRun1 created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          childrenExperimentRun1.getName());

      SetParentExperimentRunId setParentExperimentRunIdRequest =
          SetParentExperimentRunId.newBuilder()
              .setExperimentRunId(childrenExperimentRun1.getId())
              .setParentId(experimentRun.getId())
              .build();
      experimentRunServiceStub.setParentExperimentRunId(setParentExperimentRunIdRequest);

      GetExperimentRunById getExperimentRunById =
          GetExperimentRunById.newBuilder().setId(childrenExperimentRun1.getId()).build();
      GetExperimentRunById.Response response =
          experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          childrenExperimentRun1.getName(),
          response.getExperimentRun().getName());
      assertEquals(
          "ExperimentRun parent ID not match with expected ExperimentRun parent ID",
          experimentRun.getId(),
          response.getExperimentRun().getParentId());
    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }
    LOGGER.info(
        "Set Parent ID on Children ExperimentRun test stop................................");
  }

  @Test
  public void logExperimentRunCodeVersionTest() {
    LOGGER.info("Log ExperimentRun code version test start................................");

    LogExperimentRunCodeVersion logExperimentRunCodeVersionRequest =
        LogExperimentRunCodeVersion.newBuilder()
            .setId(experimentRun.getId())
            .setCodeVersion(
                CodeVersion.newBuilder()
                    .setCodeArchive(
                        Artifact.newBuilder()
                            .setKey("code_version_image")
                            .setPath("https://xyz_path_string.com/image.png")
                            .setArtifactType(ArtifactType.CODE)
                            .setUploadCompleted(
                                !testConfig
                                    .artifactStoreConfig
                                    .getArtifactStoreType()
                                    .equals(ModelDBConstants.S3))
                            .build())
                    .build())
            .build();
    experimentRunServiceStub.logExperimentRunCodeVersion(logExperimentRunCodeVersionRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    CodeVersion codeVersion = response.getExperimentRun().getCodeVersionSnapshot();
    assertEquals(
        "ExperimentRun codeVersion not match with expected ExperimentRun codeVersion",
        logExperimentRunCodeVersionRequest.getCodeVersion(),
        codeVersion);

    try {
      experimentRunServiceStub.logExperimentRunCodeVersion(logExperimentRunCodeVersionRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    logExperimentRunCodeVersionRequest =
        logExperimentRunCodeVersionRequest
            .toBuilder()
            .setCodeVersion(
                CodeVersion.newBuilder()
                    .setCodeArchive(
                        Artifact.newBuilder()
                            .setKey("code_version_image_1")
                            .setPath("https://xyz_path_string.com/image.png")
                            .setArtifactType(ArtifactType.IMAGE)
                            .setUploadCompleted(
                                !testConfig
                                    .artifactStoreConfig
                                    .getArtifactStoreType()
                                    .equals(ModelDBConstants.S3))
                            .build())
                    .build())
            .setOverwrite(true)
            .build();
    experimentRunServiceStub.logExperimentRunCodeVersion(logExperimentRunCodeVersionRequest);

    response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    codeVersion = response.getExperimentRun().getCodeVersionSnapshot();
    assertEquals(
        "ExperimentRun codeVersion not match with expected ExperimentRun codeVersion",
        logExperimentRunCodeVersionRequest.getCodeVersion(),
        codeVersion);

    logExperimentRunCodeVersionRequest =
        LogExperimentRunCodeVersion.newBuilder()
            .setId(experimentRun.getId())
            .setCodeVersion(
                CodeVersion.newBuilder()
                    .setGitSnapshot(
                        GitSnapshot.newBuilder()
                            .setHash("code_version_image_1_hash")
                            .setRepo("code_version_image_1_repo")
                            .addFilepaths("https://xyz_path_string.com/image.png")
                            .setIsDirty(Ternary.TRUE)
                            .build())
                    .build())
            .setOverwrite(true)
            .build();
    experimentRunServiceStub.logExperimentRunCodeVersion(logExperimentRunCodeVersionRequest);

    response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    codeVersion = response.getExperimentRun().getCodeVersionSnapshot();
    assertEquals(
        "ExperimentRun codeVersion not match with expected ExperimentRun codeVersion",
        logExperimentRunCodeVersionRequest.getCodeVersion(),
        codeVersion);

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Log ExperimentRun code version test stop................................");
  }

  @Test
  public void getExperimentRunCodeVersionTest() {
    LOGGER.info("Get ExperimentRun code version test start................................");

    LogExperimentRunCodeVersion logExperimentRunCodeVersionRequest =
        LogExperimentRunCodeVersion.newBuilder()
            .setId(experimentRun.getId())
            .setCodeVersion(
                CodeVersion.newBuilder()
                    .setCodeArchive(
                        Artifact.newBuilder()
                            .setKey("code_version_image")
                            .setPath("https://xyz_path_string.com/image.png")
                            .setArtifactType(ArtifactType.CODE)
                            .setUploadCompleted(
                                !testConfig
                                    .artifactStoreConfig
                                    .getArtifactStoreType()
                                    .equals(ModelDBConstants.S3))
                            .build())
                    .build())
            .build();
    experimentRunServiceStub.logExperimentRunCodeVersion(logExperimentRunCodeVersionRequest);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    CodeVersion codeVersion = response.getExperimentRun().getCodeVersionSnapshot();
    assertEquals(
        "ExperimentRun codeVersion not match with expected ExperimentRun codeVersion",
        logExperimentRunCodeVersionRequest.getCodeVersion(),
        codeVersion);

    GetExperimentRunCodeVersion getExperimentRunCodeVersionRequest =
        GetExperimentRunCodeVersion.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunCodeVersion.Response getExperimentRunCodeVersionResponse =
        experimentRunServiceStub.getExperimentRunCodeVersion(getExperimentRunCodeVersionRequest);
    assertEquals(
        "ExperimentRun codeVersion not match with expected experimentRun codeVersion",
        codeVersion,
        getExperimentRunCodeVersionResponse.getCodeVersion());

    LOGGER.info("Get ExperimentRun code version test stop................................");
  }

  @Test
  public void deleteExperimentRunArtifacts() {
    LOGGER.info("Delete ExperimentRun Artifacts test start................................");

    m_logArtifactsTest();
    List<Artifact> artifacts = experimentRun.getArtifactsList();
    LOGGER.info("Artifacts size : " + artifacts.size());
    if (artifacts.isEmpty()) {
      fail("Artifacts not found");
    }

    DeleteArtifact request =
        DeleteArtifact.newBuilder()
            .setId(experimentRun.getId())
            .setKey(artifacts.get(0).getKey())
            .build();

    experimentRunServiceStub.deleteArtifact(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info(
        "DeleteExperimentRunArtifacts Response : \n"
            + response.getExperimentRun().getArtifactsList());
    assertFalse(response.getExperimentRun().getArtifactsList().contains(artifacts.get(0)));

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());

    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    LOGGER.info("Delete ExperimentRun Artifacts test stop................................");
  }

  @Test
  public void batchDeleteExperimentRunTest() {
    LOGGER.info("Batch Delete ExperimentRun test start................................");

    // Create two experiment of above project
    CreateExperiment createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project.getId(), "Experiment-" + new Date().getTime());
    CreateExperiment.Response createExperimentResponse =
        experimentServiceStub.createExperiment(createExperimentRequest);
    Experiment experiment = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment.getName());

    List<String> experimentRunIds = new ArrayList<>();
    for (int count = 0; count < 5; count++) {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-" + count + "-" + new Date().getTime());
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun.getName());
    }

    DeleteExperimentRuns deleteExperimentRuns =
        DeleteExperimentRuns.newBuilder().addAllIds(experimentRunIds).build();
    DeleteExperimentRuns.Response deleteExperimentRunsResponse =
        experimentRunServiceStub.deleteExperimentRuns(deleteExperimentRuns);
    assertTrue(deleteExperimentRunsResponse.getStatus());

    FindExperimentRuns getExperimentRunsInExperiment =
        FindExperimentRuns.newBuilder().setExperimentId(experiment.getId()).build();

    FindExperimentRuns.Response experimentRunResponse =
        experimentRunServiceStub.findExperimentRuns(getExperimentRunsInExperiment);
    assertEquals(
        "ExperimentRuns count not match with expected experimentRun count",
        0,
        experimentRunResponse.getExperimentRunsCount());

    LOGGER.info("Batch Delete ExperimentRun test stop................................");
  }

  @Test
  public void deleteExperimentRunByParentEntitiesOwnerTest() {
    LOGGER.info(
        "Delete ExperimentRun by parent entities owner test start.........................");

    if (testConfig.hasAuth()) {
      AddCollaboratorRequest addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project, CollaboratorType.READ_ONLY, authClientInterceptor);

      AddCollaboratorRequest.Response addCollaboratorResponse =
          collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator added in server : " + addCollaboratorResponse.getStatus());
      assertTrue(addCollaboratorResponse.getStatus());
    }

    // Create two experiment of above project
    CreateExperiment createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project.getId(), "Experiment-" + new Date().getTime());
    CreateExperiment.Response createExperimentResponse =
        experimentServiceStub.createExperiment(createExperimentRequest);
    Experiment experiment = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");

    List<String> experimentRunIds = new ArrayList<>();
    for (int count = 0; count < 5; count++) {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-" + count + "-" + new Date().getTime());
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun.getId());
      LOGGER.info("ExperimentRun created successfully");
    }

    DeleteExperimentRuns deleteExperimentRuns =
        DeleteExperimentRuns.newBuilder()
            .addAllIds(experimentRunIds.subList(0, experimentRunIds.size() - 1))
            .build();

    if (testConfig.hasAuth()) {
      try {
        experimentRunServiceStubClient2.deleteExperimentRuns(deleteExperimentRuns);
      } catch (StatusRuntimeException e) {
        checkEqualsAssert(e);
      }

      AddCollaboratorRequest addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project, CollaboratorType.READ_WRITE, authClientInterceptor);

      AddCollaboratorRequest.Response addCollaboratorResponse =
          collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator updated in server : " + addCollaboratorResponse.getStatus());
      assertTrue(addCollaboratorResponse.getStatus());

      DeleteExperimentRuns.Response deleteExperimentRunsResponse =
          experimentRunServiceStubClient2.deleteExperimentRuns(deleteExperimentRuns);
      assertTrue(deleteExperimentRunsResponse.getStatus());

      DeleteExperimentRun deleteExperimentRun =
          DeleteExperimentRun.newBuilder().setId(experimentRunIds.get(4)).build();

      DeleteExperimentRun.Response deleteExperimentRunResponse =
          experimentRunServiceStubClient2.deleteExperimentRun(deleteExperimentRun);
      assertTrue(deleteExperimentRunResponse.getStatus());

    } else {
      deleteExperimentRuns = DeleteExperimentRuns.newBuilder().addAllIds(experimentRunIds).build();

      DeleteExperimentRuns.Response deleteExperimentRunResponse =
          experimentRunServiceStub.deleteExperimentRuns(deleteExperimentRuns);
      assertTrue(deleteExperimentRunResponse.getStatus());
    }

    FindExperimentRuns getExperimentRunsInExperiment =
        FindExperimentRuns.newBuilder().setExperimentId(experiment.getId()).build();

    FindExperimentRuns.Response experimentRunResponse =
        experimentRunServiceStub.findExperimentRuns(getExperimentRunsInExperiment);
    assertEquals(
        "ExperimentRuns count not match with expected experimentRun count",
        0,
        experimentRunResponse.getExperimentRunsCount());

    LOGGER.info(
        "Delete ExperimentRun by parent entities owner test stop................................");
  }

  @Test
  public void versioningAtExperimentRunCreateTest()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("Versioning ExperimentRun test start................................");

    long repoId = createRepository(versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);
    Commit commit =
        Commit.newBuilder()
            .setMessage("this is the test commit message")
            .setDateCreated(111)
            .addParentShas(getBranchResponse.getCommit().getCommitSha())
            .build();
    CreateCommitRequest createCommitRequest =
        CreateCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommit(commit)
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addLocation("dataset")
                    .addLocation("train")
                    .build())
            .build();
    CreateCommitRequest.Response commitResponse =
        versioningServiceBlockingStub.createCommit(createCommitRequest);

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequest(
            project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
    Map<String, Location> locationMap = new HashMap<>();
    locationMap.put(
        "location-2", Location.newBuilder().addLocation("dataset").addLocation("train").build());
    createExperimentRunRequest =
        createExperimentRunRequest
            .toBuilder()
            .setVersionedInputs(
                VersioningEntry.newBuilder()
                    .setRepositoryId(repoId)
                    .setCommit(commitResponse.getCommit().getCommitSha())
                    .putAllKeyLocationMap(locationMap)
                    .build())
            .build();
    CreateExperimentRun.Response createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun.getName());
    assertEquals(
        "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
        createExperimentRunRequest.getVersionedInputs(),
        experimentRun.getVersionedInputs());

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response getExperimentRunByIdRes =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    assertEquals(
        "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
        createExperimentRunRequest.getVersionedInputs(),
        getExperimentRunByIdRes.getExperimentRun().getVersionedInputs());

    DeleteCommitRequest deleteCommitRequest =
        DeleteCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommitSha(commitResponse.getCommit().getCommitSha())
            .build();
    versioningServiceBlockingStub.deleteCommit(deleteCommitRequest);

    DeleteRepositoryRequest deleteRepository =
        DeleteRepositoryRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
            .build();
    DeleteRepositoryRequest.Response deleteResult =
        versioningServiceBlockingStub.deleteRepository(deleteRepository);
    Assert.assertTrue(deleteResult.getStatus());

    LOGGER.info("Versioning ExperimentRun test stop................................");
  }

  @Test
  public void versioningAtExperimentRunCreateNegativeTest() throws ModelDBException {
    LOGGER.info("Versioning ExperimentRun negative test start................................");

    long repoId = createRepository(versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequest(
            project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
    Map<String, Location> locationMap = new HashMap<>();
    locationMap.put(
        "location-2", Location.newBuilder().addLocation("dataset").addLocation("train").build());
    createExperimentRunRequest =
        createExperimentRunRequest
            .toBuilder()
            .setVersionedInputs(
                VersioningEntry.newBuilder()
                    .setRepositoryId(123456)
                    .setCommit("xyz")
                    .putAllKeyLocationMap(locationMap)
                    .build())
            .build();
    try {
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    } catch (StatusRuntimeException e) {
      Assert.assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    try {
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setName("test-" + Calendar.getInstance().getTimeInMillis())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit("xyz")
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Assert.assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    try {
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setName("test-" + Calendar.getInstance().getTimeInMillis())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(getBranchResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Assert.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    DeleteRepositoryRequest deleteRepository =
        DeleteRepositoryRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
            .build();
    DeleteRepositoryRequest.Response deleteResult =
        versioningServiceBlockingStub.deleteRepository(deleteRepository);
    Assert.assertTrue(deleteResult.getStatus());

    LOGGER.info("Versioning ExperimentRun negative test stop................................");
  }

  @Test
  public void logGetVersioningExperimentRunCreateTest()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("Log and Get Versioning ExperimentRun test start................................");

    long repoId = createRepository(versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    try {
      GetBranchRequest getBranchRequest =
          GetBranchRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setBranch(ModelDBConstants.MASTER_BRANCH)
              .build();
      GetBranchRequest.Response getBranchResponse =
          versioningServiceBlockingStub.getBranch(getBranchRequest);
      Commit commit =
          Commit.newBuilder()
              .setMessage("this is the test commit message")
              .setDateCreated(111)
              .addParentShas(getBranchResponse.getCommit().getCommitSha())
              .build();
      CreateCommitRequest createCommitRequest =
          CreateCommitRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommit(commit)
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                      .addLocation("dataset")
                      .addLocation("train")
                      .build())
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                      .addLocation("dataset_1")
                      .addLocation("train")
                      .build())
              .build();
      CreateCommitRequest.Response commitResponse =
          versioningServiceBlockingStub.createCommit(createCommitRequest);

      Map<String, Location> locationMap = new HashMap<>();
      locationMap.put(
          "location-2", Location.newBuilder().addLocation("dataset").addLocation("train").build());

      LogVersionedInput logVersionedInput =
          LogVersionedInput.newBuilder()
              .setId(experimentRun.getId())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      experimentRunServiceStub.logVersionedInput(logVersionedInput);

      LogVersionedInput logVersionedInputFail =
          LogVersionedInput.newBuilder()
              .setId(experimentRun.getId())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getParentShasList().get(0))
                      .build())
              .build();
      try {
        experimentRunServiceStub.logVersionedInput(logVersionedInputFail);
        fail();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        LOGGER.warn(
            "Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
      }

      GetExperimentRunById getExperimentRunById =
          GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
      Response response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      assertEquals(
          "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
          logVersionedInput.getVersionedInputs(),
          response.getExperimentRun().getVersionedInputs());

      locationMap.put(
          "location-1",
          Location.newBuilder().addLocation("dataset_1").addLocation("train").build());
      logVersionedInput =
          LogVersionedInput.newBuilder()
              .setId(experimentRun.getId())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      experimentRunServiceStub.logVersionedInput(logVersionedInput);

      response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      assertEquals(
          "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
          logVersionedInput.getVersionedInputs(),
          response.getExperimentRun().getVersionedInputs());

      GetVersionedInput getVersionedInput =
          GetVersionedInput.newBuilder().setId(experimentRun.getId()).build();
      GetVersionedInput.Response getVersionedInputResponse =
          experimentRunServiceStub.getVersionedInputs(getVersionedInput);
      assertEquals(
          "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
          logVersionedInput.getVersionedInputs(),
          getVersionedInputResponse.getVersionedInputs());

      if (testConfig.hasAuth()) {
        getVersionedInput = GetVersionedInput.newBuilder().setId(experimentRun.getId()).build();
        getVersionedInputResponse =
            experimentRunServiceStubClient2.getVersionedInputs(getVersionedInput);
        if (testConfig.isPopulateConnectionsBasedOnPrivileges()) {
          assertTrue(
              "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
              getVersionedInputResponse.getVersionedInputs().getKeyLocationMapMap().isEmpty());
        } else {
          assertFalse(
              "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
              getVersionedInputResponse.getVersionedInputs().getKeyLocationMapMap().isEmpty());
        }
      }
    } finally {
      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());
    }

    LOGGER.info("Log and Get Versioning ExperimentRun test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: listCommitExperimentRuns endpoint")
  public void listCommitExperimentRunsTest() throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("Fetch ExperimentRun for commit test start................................");

    String repoName = "Repo-" + new Date().getTime();
    long repoId = RepositoryTest.createRepository(versioningServiceBlockingStub, repoName);

    String testUser1UserName = null;
    if (testConfig.hasAuth()) {
      GetUser getUserRequest =
          GetUser.newBuilder().setEmail(authClientInterceptor.getClient1Email()).build();
      // Get the user info by vertaId form the AuthService
      UserInfo testUser1 = uacServiceStub.getUser(getUserRequest);
      testUser1UserName = testUser1.getVertaInfo().getUsername();
    }
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);
    Commit commit =
        Commit.newBuilder()
            .setMessage("this is the test commit message")
            .setDateCreated(111)
            .addParentShas(getBranchResponse.getCommit().getCommitSha())
            .build();
    Location location1 = Location.newBuilder().addLocation("dataset").addLocation("train").build();
    Location location2 =
        Location.newBuilder().addLocation("test-1").addLocation("test1.json").build();
    Location location3 =
        Location.newBuilder().addLocation("test-2").addLocation("test2.json").build();
    CreateCommitRequest createCommitRequest =
        CreateCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommit(commit)
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location1.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.CONFIG))
                    .addAllLocation(location2.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location3.getLocationList())
                    .build())
            .build();
    CreateCommitRequest.Response commitResponse =
        versioningServiceBlockingStub.createCommit(createCommitRequest);
    commit = commitResponse.getCommit();

    if (testConfig.hasAuth()) {
      AddCollaboratorRequest addCollaboratorRequest =
          CollaboratorUtils.addCollaboratorRequestProject(
              project, authClientInterceptor.getClient2Email(), CollaboratorType.READ_WRITE);
      collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("\n Collaborator1 added in project successfully \n");

      addCollaboratorRequest =
          CollaboratorUtils.addCollaboratorRequestUser(
              String.valueOf(repoId),
              authClientInterceptor.getClient2Email(),
              CollaboratorType.READ_WRITE,
              "This is a repo collaborator description");
      collaboratorServiceStubClient1.addOrUpdateRepositoryCollaborator(addCollaboratorRequest);
      LOGGER.info("\n Collaborator1 added in repository successfully \n");
    }

    List<String> experimentRunIds = new ArrayList<>();
    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun-1-" + new Date().getTime());
      Map<String, Location> locationMap = new HashMap<>();
      locationMap.put("location-1", location1);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setDateCreated(Calendar.getInstance().getTimeInMillis())
              .setDateUpdated(Calendar.getInstance().getTimeInMillis())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun1 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun1.getId());
      LOGGER.info("ExperimentRun1 created successfully");

      locationMap.put("location-2", location2);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setDateCreated(Calendar.getInstance().getTimeInMillis())
              .setDateUpdated(Calendar.getInstance().getTimeInMillis())
              .setName("ExperimentRun-2-" + new Date().getTime())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun2 created successfully");

      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setDateCreated(Calendar.getInstance().getTimeInMillis())
              .setDateUpdated(Calendar.getInstance().getTimeInMillis())
              .setName("ExperimentRun-3-" + new Date().getTime())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun3 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun3.getId());
      LOGGER.info("ExperimentRun3 created successfully");

      ListCommitExperimentRunsRequest listCommitExperimentRunsRequest =
          ListCommitExperimentRunsRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommitSha(commit.getCommitSha())
              .build();
      ListCommitExperimentRunsRequest.Response listCommitExperimentRunsResponse =
          experimentRunServiceStub.listCommitExperimentRuns(listCommitExperimentRunsRequest);
      assertEquals(
          "ExperimentRun total records not match with expected ExperimentRun total records",
          3,
          listCommitExperimentRunsResponse.getTotalRecords());
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun1,
          listCommitExperimentRunsResponse.getRuns(0));
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun3.getId(),
          listCommitExperimentRunsResponse.getRuns(2).getId());

      listCommitExperimentRunsRequest =
          ListCommitExperimentRunsRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommitSha(commit.getCommitSha())
              .setPagination(Pagination.newBuilder().setPageNumber(1).setPageLimit(1).build())
              .build();
      listCommitExperimentRunsResponse =
          experimentRunServiceStub.listCommitExperimentRuns(listCommitExperimentRunsRequest);
      assertEquals(
          "ExperimentRun total records not match with expected ExperimentRun total records",
          3,
          listCommitExperimentRunsResponse.getTotalRecords());
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun1,
          listCommitExperimentRunsResponse.getRuns(0));

      listCommitExperimentRunsRequest =
          ListCommitExperimentRunsRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommitSha(commit.getCommitSha())
              .setPagination(Pagination.newBuilder().setPageNumber(3).setPageLimit(1).build())
              .build();
      listCommitExperimentRunsResponse =
          experimentRunServiceStub.listCommitExperimentRuns(listCommitExperimentRunsRequest);
      assertEquals(
          "ExperimentRun total records not match with expected ExperimentRun total records",
          3,
          listCommitExperimentRunsResponse.getTotalRecords());
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun3.getId(),
          listCommitExperimentRunsResponse.getRuns(0).getId());

      RepositoryIdentification repositoryIdentification;
      if (testUser1UserName != null) {
        repositoryIdentification =
            RepositoryIdentification.newBuilder()
                .setNamedId(
                    RepositoryNamedIdentification.newBuilder()
                        .setName(repoName)
                        .setWorkspaceName(testUser1UserName)
                        .build())
                .build();
      } else {
        repositoryIdentification = RepositoryIdentification.newBuilder().setRepoId(repoId).build();
      }
      listCommitExperimentRunsRequest =
          ListCommitExperimentRunsRequest.newBuilder()
              .setRepositoryId(repositoryIdentification)
              .setCommitSha(commit.getCommitSha())
              .build();
      listCommitExperimentRunsResponse =
          experimentRunServiceStub.listCommitExperimentRuns(listCommitExperimentRunsRequest);
      assertEquals(
          "ExperimentRun total records not match with expected ExperimentRun total records",
          3,
          listCommitExperimentRunsResponse.getTotalRecords());
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun1,
          listCommitExperimentRunsResponse.getRuns(0));
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun3.getId(),
          listCommitExperimentRunsResponse.getRuns(2).getId());

      DeleteCommitRequest deleteCommitRequest =
          DeleteCommitRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommitSha(commitResponse.getCommit().getCommitSha())
              .build();
      versioningServiceBlockingStub.deleteCommit(deleteCommitRequest);
    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }

      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());
    }

    LOGGER.info("Fetch ExperimentRun for commit test stop................................");
  }

  @Test
  @Ignore("UNIMPLEMENTED: listBlobExperimentRuns endpoint")
  public void ListBlobExperimentRunsRequestTest()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("Fetch ExperimentRun blobs for commit test start................................");

    long repoId =
        RepositoryTest.createRepository(
            versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    List<String> experimentRunIds = new ArrayList<>();
    try {
      GetBranchRequest getBranchRequest =
          GetBranchRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setBranch(ModelDBConstants.MASTER_BRANCH)
              .build();
      GetBranchRequest.Response getBranchResponse =
          versioningServiceBlockingStub.getBranch(getBranchRequest);
      Commit commit =
          Commit.newBuilder()
              .setMessage("this is the test commit message")
              .setDateCreated(111)
              .addParentShas(getBranchResponse.getCommit().getCommitSha())
              .build();

      Location location1 =
          Location.newBuilder().addLocation("dataset").addLocation("train").build();
      Location location2 =
          Location.newBuilder().addLocation("test-1").addLocation("test1.json").build();
      Location location3 =
          Location.newBuilder().addLocation("test-2").addLocation("test2.json").build();

      CreateCommitRequest createCommitRequest =
          CreateCommitRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommit(commit)
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                      .addAllLocation(location1.getLocationList())
                      .build())
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.CONFIG))
                      .addAllLocation(location2.getLocationList())
                      .build())
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                      .addAllLocation(location3.getLocationList())
                      .build())
              .build();
      CreateCommitRequest.Response commitResponse =
          versioningServiceBlockingStub.createCommit(createCommitRequest);
      commit = commitResponse.getCommit();

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun-1-" + new Date().getTime());
      Map<String, Location> locationMap = new HashMap<>();
      locationMap.put("location-2", location1);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun1 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun1.getId());
      LOGGER.info("ExperimentRun1 created successfully");

      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setDateCreated(Calendar.getInstance().getTimeInMillis())
              .setDateUpdated(Calendar.getInstance().getTimeInMillis())
              .setName("ExperimentRun-2")
              .setVersionedInputs(
                  createExperimentRunRequest
                      .getVersionedInputs()
                      .toBuilder()
                      .putKeyLocationMap("XYZ", location2)
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun2 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun2.getId());
      LOGGER.info("ExperimentRun2 created successfully");

      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setDateCreated(Calendar.getInstance().getTimeInMillis())
              .setDateUpdated(Calendar.getInstance().getTimeInMillis())
              .setName("ExperimentRun-3")
              .setVersionedInputs(
                  createExperimentRunRequest
                      .getVersionedInputs()
                      .toBuilder()
                      .putKeyLocationMap("PQR", location3)
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun3 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun3.getId());
      LOGGER.info("ExperimentRun3 created successfully");

      ListBlobExperimentRunsRequest listBlobExperimentRunsRequest =
          ListBlobExperimentRunsRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommitSha(commit.getCommitSha())
              .addAllLocation(location1.getLocationList())
              .build();
      ListBlobExperimentRunsRequest.Response listBlobExperimentRunsResponse =
          experimentRunServiceStub.listBlobExperimentRuns(listBlobExperimentRunsRequest);
      assertEquals(
          "ExperimentRun total records not match with expected ExperimentRun total records",
          3,
          listBlobExperimentRunsResponse.getTotalRecords());
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun1,
          listBlobExperimentRunsResponse.getRuns(0));
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun3.getId(),
          listBlobExperimentRunsResponse.getRuns(2).getId());

      listBlobExperimentRunsRequest =
          ListBlobExperimentRunsRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommitSha(commit.getCommitSha())
              .addAllLocation(location2.getLocationList())
              .build();
      listBlobExperimentRunsResponse =
          experimentRunServiceStub.listBlobExperimentRuns(listBlobExperimentRunsRequest);
      assertEquals(
          "ExperimentRun total records not match with expected ExperimentRun total records",
          2,
          listBlobExperimentRunsResponse.getTotalRecords());
      assertEquals(
          "ExperimentRun not match with expected ExperimentRun",
          experimentRun3.getId(),
          listBlobExperimentRunsResponse.getRuns(1).getId());

      Location location4 = Location.newBuilder().addLocation("test-2").build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setName("ExperimentRun-4")
              .setVersionedInputs(
                  createExperimentRunRequest
                      .getVersionedInputs()
                      .toBuilder()
                      .putKeyLocationMap("PQR", location4)
                      .build())
              .build();
      try {
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        LOGGER.warn(
            "Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      }

      DeleteCommitRequest deleteCommitRequest =
          DeleteCommitRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommitSha(commitResponse.getCommit().getCommitSha())
              .build();
      versioningServiceBlockingStub.deleteCommit(deleteCommitRequest);
    } finally {
      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());

      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("Fetch ExperimentRun blob for commit test stop................................");
  }

  @Test
  public void versioningWithoutLocationAtExperimentRunCreateTest()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info(
        "Versioning without Locations ExperimentRun test start................................");

    long repoId = createRepository(versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);
    Commit commit =
        Commit.newBuilder()
            .setMessage("this is the test commit message")
            .setDateCreated(111)
            .addParentShas(getBranchResponse.getCommit().getCommitSha())
            .build();
    CreateCommitRequest createCommitRequest =
        CreateCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommit(commit)
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addLocation("dataset")
                    .addLocation("train")
                    .build())
            .build();
    CreateCommitRequest.Response commitResponse =
        versioningServiceBlockingStub.createCommit(createCommitRequest);

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequest(
            project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
    createExperimentRunRequest =
        createExperimentRunRequest
            .toBuilder()
            .setVersionedInputs(
                VersioningEntry.newBuilder()
                    .setRepositoryId(repoId)
                    .setCommit(commitResponse.getCommit().getCommitSha())
                    .build())
            .build();
    CreateExperimentRun.Response createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun.getName());
    assertEquals(
        "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
        createExperimentRunRequest.getVersionedInputs(),
        experimentRun.getVersionedInputs());

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response getExperimentRunByIdRes =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    assertEquals(
        "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
        createExperimentRunRequest.getVersionedInputs(),
        getExperimentRunByIdRes.getExperimentRun().getVersionedInputs());
    assertEquals(
        "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
        0,
        getExperimentRunByIdRes.getExperimentRun().getVersionedInputs().getKeyLocationMapCount());

    DeleteCommitRequest deleteCommitRequest =
        DeleteCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommitSha(commitResponse.getCommit().getCommitSha())
            .build();
    versioningServiceBlockingStub.deleteCommit(deleteCommitRequest);

    DeleteRepositoryRequest deleteRepository =
        DeleteRepositoryRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
            .build();
    DeleteRepositoryRequest.Response deleteResult =
        versioningServiceBlockingStub.deleteRepository(deleteRepository);
    Assert.assertTrue(deleteResult.getStatus());

    LOGGER.info(
        "Versioning without Locations ExperimentRun test stop................................");
  }

  @Test
  public void logGetVersioningWithoutLocationsExperimentRunCreateTest()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info(
        "Log and Get Versioning without locations ExperimentRun test start................................");

    List<String> experimentRunIds = new ArrayList<>();
    long repoId = createRepository(versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun.getId());
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun.getName());
      assertFalse(
          "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
          createExperimentRunRequest.hasVersionedInputs());

      GetBranchRequest getBranchRequest =
          GetBranchRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setBranch(ModelDBConstants.MASTER_BRANCH)
              .build();
      GetBranchRequest.Response getBranchResponse =
          versioningServiceBlockingStub.getBranch(getBranchRequest);
      Commit commit =
          Commit.newBuilder()
              .setMessage("this is the test commit message")
              .setDateCreated(111)
              .addParentShas(getBranchResponse.getCommit().getCommitSha())
              .build();
      CreateCommitRequest createCommitRequest =
          CreateCommitRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommit(commit)
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                      .addLocation("dataset")
                      .addLocation("train")
                      .build())
              .build();
      CreateCommitRequest.Response commitResponse =
          versioningServiceBlockingStub.createCommit(createCommitRequest);

      LogVersionedInput logVersionedInput =
          LogVersionedInput.newBuilder()
              .setId(experimentRun.getId())
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .build())
              .build();
      experimentRunServiceStub.logVersionedInput(logVersionedInput);

      GetExperimentRunById getExperimentRunById =
          GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
      GetExperimentRunById.Response response =
          experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      assertEquals(
          "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
          logVersionedInput.getVersionedInputs(),
          response.getExperimentRun().getVersionedInputs());

      GetVersionedInput getVersionedInput =
          GetVersionedInput.newBuilder().setId(experimentRun.getId()).build();
      GetVersionedInput.Response getVersionedInputResponse =
          experimentRunServiceStub.getVersionedInputs(getVersionedInput);
      assertEquals(
          "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
          logVersionedInput.getVersionedInputs(),
          getVersionedInputResponse.getVersionedInputs());
      assertEquals(
          "ExperimentRun versioningInput not match with expected ExperimentRun versioningInput",
          0,
          getVersionedInputResponse.getVersionedInputs().getKeyLocationMapCount());
    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }

      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());
    }

    LOGGER.info(
        "Log and Get Versioning without locations ExperimentRun test stop................................");
  }

  @Test
  public void findExperimentRunsHyperparameterWithRepository()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("FindExperimentRuns test start................................");

    long repoId =
        RepositoryTest.createRepository(
            versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);
    Commit commit =
        Commit.newBuilder()
            .setMessage("this is the test commit message")
            .setDateCreated(111)
            .addParentShas(getBranchResponse.getCommit().getCommitSha())
            .build();
    Location location1 = Location.newBuilder().addLocation("dataset").addLocation("train").build();
    Location location2 =
        Location.newBuilder().addLocation("test-1").addLocation("test1.json").build();
    Location location3 =
        Location.newBuilder().addLocation("test-2").addLocation("test2.json").build();
    Location location4 =
        Location.newBuilder().addLocation("test-location-4").addLocation("test4.json").build();

    CreateCommitRequest createCommitRequest =
        CreateCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommit(commit)
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location1.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.CONFIG))
                    .addAllLocation(location2.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location3.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getHyperparameterConfigBlob(0.14F, 0.10F))
                    .addAllLocation(location4.getLocationList())
                    .build())
            .build();
    CreateCommitRequest.Response commitResponse =
        versioningServiceBlockingStub.createCommit(createCommitRequest);
    commit = commitResponse.getCommit();

    List<String> experimentRunIds = new ArrayList<>();
    try {
      Map<String, Location> locationMap = new HashMap<>();
      locationMap.put("location-1", location1);

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-1-" + new Date().getTime());
      KeyValue hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .addHyperparameters(hyperparameter1)
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      locationMap.put("location-2", location2);

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-2-" + new Date().getTime());
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      LOGGER.info("ExperimentRun created successfully");
      ExperimentRun experimentRunConfig1 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRunConfig1.getId());

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      Map<String, Location> locationMap2 = new HashMap<>();
      locationMap2.put("location-4", location4);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap2)
                      .build())
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      LOGGER.info("ExperimentRun created successfully");
      ExperimentRun experimentRunConfig2 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRunConfig2.getId());

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 1e-6);
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      Value hyperparameterFilter = Value.newBuilder().setNumberValue(0.0001).build();
      KeyValueQuery keyValueQuery =
          KeyValueQuery.newBuilder()
              .setKey("hyperparameters.train")
              .setValue(hyperparameterFilter)
              .setOperator(Operator.GTE)
              .setValueType(ValueType.NUMBER)
              .build();

      FindExperimentRuns findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .addPredicates(keyValueQuery)
              .setAscending(false)
              .setIdsOnly(false)
              .build();

      FindExperimentRuns.Response response =
          experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      assertEquals(
          "Total records count not matched with expected records count",
          2,
          response.getTotalRecords());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          2,
          response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          experimentRunConfig2.getId(),
          response.getExperimentRuns(0).getId());
      for (ExperimentRun exprRun : response.getExperimentRunsList()) {
        for (KeyValue kv : exprRun.getHyperparametersList()) {
          if (kv.getKey().equals("train")) {
            assertTrue("Value should be GTE 0.0001 " + kv, kv.getValue().getNumberValue() > 0.0001);
          }
        }
      }

      if (testConfig.hasAuth()) {
        AddCollaboratorRequest addCollaboratorRequest =
            AddCollaboratorRequest.newBuilder()
                .setShareWith(authClientInterceptor.getClient2Email())
                .setPermission(
                    CollaboratorPermissions.newBuilder()
                        .setCollaboratorType(CollaboratorTypeEnum.CollaboratorType.READ_ONLY)
                        .build())
                .setAuthzEntityType(EntitiesEnum.EntitiesTypes.USER)
                .addEntityIds(project.getId())
                .build();
        AddCollaboratorRequest.Response addCollaboratorResponse =
            collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
        LOGGER.info(
            "Project Collaborator added in server : " + addCollaboratorResponse.getStatus());
        assertTrue(addCollaboratorResponse.getStatus());

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(keyValueQuery)
                .setAscending(false)
                .setIdsOnly(false)
                .build();

        response = experimentRunServiceStubClient2.findExperimentRuns(findExperimentRuns);
        assertEquals(
            "Total records count not matched with expected records count",
            2,
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            2,
            response.getExperimentRunsCount());
        if (testConfig.isPopulateConnectionsBasedOnPrivileges()) {
          assertEquals(
              "ExperimentRun hyperparameters count not match with expected experimentRun hyperparameters count",
              1,
              response.getExperimentRuns(0).getHyperparametersCount());
        } else {
          assertEquals(
              "ExperimentRun hyperparameters count not match with expected experimentRun hyperparameters count",
              3,
              response.getExperimentRuns(0).getHyperparametersCount());
        }

        addCollaboratorRequest =
            AddCollaboratorRequest.newBuilder()
                .setShareWith(authClientInterceptor.getClient2Email())
                .setPermission(
                    CollaboratorPermissions.newBuilder()
                        .setCollaboratorType(CollaboratorTypeEnum.CollaboratorType.READ_ONLY)
                        .build())
                .setAuthzEntityType(EntitiesEnum.EntitiesTypes.USER)
                .addEntityIds(String.valueOf(repoId))
                .build();
        addCollaboratorResponse =
            collaboratorServiceStubClient1.addOrUpdateRepositoryCollaborator(
                addCollaboratorRequest);
        LOGGER.info("Collaborator added in server : " + addCollaboratorResponse.getStatus());
        assertTrue(addCollaboratorResponse.getStatus());

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(keyValueQuery)
                .setAscending(false)
                .setIdsOnly(false)
                .build();

        response = experimentRunServiceStubClient2.findExperimentRuns(findExperimentRuns);
        assertEquals(
            "Total records count not matched with expected records count",
            2,
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            2,
            response.getExperimentRunsCount());
        if (testConfig.isPopulateConnectionsBasedOnPrivileges()) {
          assertEquals(
              "ExperimentRun hyperparameters count not match with expected experimentRun hyperparameters count",
              3,
              response.getExperimentRuns(0).getHyperparametersCount());
        } else {
          assertEquals(
              "ExperimentRun hyperparameters count not match with expected experimentRun hyperparameters count",
              3,
              response.getExperimentRuns(0).getHyperparametersCount());
        }
      }
    } finally {
      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());

      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("FindExperimentRuns test stop................................");
  }

  @Test
  public void findExperimentRunsHyperparameterWithSortKeyRepository()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("FindExperimentRuns test start................................");

    long repoId =
        RepositoryTest.createRepository(
            versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);
    Commit commit =
        Commit.newBuilder()
            .setMessage("this is the test commit message")
            .setDateCreated(111)
            .addParentShas(getBranchResponse.getCommit().getCommitSha())
            .build();
    Location location1 = Location.newBuilder().addLocation("dataset").addLocation("train").build();
    Location location2 =
        Location.newBuilder().addLocation("test-1").addLocation("test1.json").build();
    Location location3 =
        Location.newBuilder().addLocation("test-2").addLocation("test2.json").build();
    Location location4 =
        Location.newBuilder().addLocation("test-location-4").addLocation("test4.json").build();

    CreateCommitRequest createCommitRequest =
        CreateCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommit(commit)
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location1.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getHyperparameterConfigBlob(3F, 3F))
                    .addAllLocation(location2.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location3.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getHyperparameterConfigBlob(5F, 2F))
                    .addAllLocation(location4.getLocationList())
                    .build())
            .build();
    CreateCommitRequest.Response commitResponse =
        versioningServiceBlockingStub.createCommit(createCommitRequest);
    commit = commitResponse.getCommit();

    Map<String, Location> locationMap = new HashMap<>();
    locationMap.put("location-1", location1);

    List<String> experimentRunIds = new ArrayList<>();

    try {
      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      KeyValue hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .addHyperparameters(hyperparameter1)
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      locationMap.put("location-2", location2);

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      LOGGER.info("ExperimentRun created successfully");
      ExperimentRun experimentRunConfig1 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRunConfig1.getId());

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 0.0002);
      Map<String, Location> locationMap2 = new HashMap<>();
      locationMap2.put("location-4", location4);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap2)
                      .build())
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      LOGGER.info("ExperimentRun created successfully");
      ExperimentRun experimentRunConfig2 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRunConfig2.getId());

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 0.0003);
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      try {
        Value hyperparameterFilter = Value.newBuilder().setNumberValue(1).build();
        KeyValueQuery keyValueQuery =
            KeyValueQuery.newBuilder()
                .setKey("hyperparameters.train")
                .setValue(hyperparameterFilter)
                .setOperator(Operator.GTE)
                .setValueType(ValueType.NUMBER)
                .build();

        FindExperimentRuns findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(keyValueQuery)
                .setAscending(false)
                .setIdsOnly(false)
                .setSortKey("hyperparameters.train")
                .build();

        FindExperimentRuns.Response response =
            experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

        assertEquals(
            "Total records count not matched with expected records count",
            2,
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            2,
            response.getExperimentRunsCount());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            experimentRunConfig2.getId(),
            response.getExperimentRuns(0).getId());

        for (int index = 0; index < response.getExperimentRunsCount(); index++) {
          ExperimentRun exprRun = response.getExperimentRuns(index);
          for (KeyValue kv : exprRun.getHyperparametersList()) {
            if (kv.getKey().equals("C")) {
              assertTrue(
                  "Value should be GTE 0.0001 " + kv, kv.getValue().getNumberValue() >= 0.0001);
            } else if (kv.getKey().equals("train")) {
              if (index == 0) {
                assertEquals(
                    "Value should be GTE 1 " + kv, 5.0F, kv.getValue().getNumberValue(), 0.0);
              } else {
                assertEquals(
                    "Value should be GTE 1 " + kv, 3.0F, kv.getValue().getNumberValue(), 0.0);
              }
            }
          }
        }

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(keyValueQuery)
                .setAscending(true)
                .setIdsOnly(false)
                .setSortKey("hyperparameters.train")
                .build();

        response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

        assertEquals(
            "Total records count not matched with expected records count",
            2,
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            2,
            response.getExperimentRunsCount());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            experimentRunConfig1.getId(),
            response.getExperimentRuns(0).getId());

        for (int index = 0; index < response.getExperimentRunsCount(); index++) {
          ExperimentRun exprRun = response.getExperimentRuns(index);
          for (KeyValue kv : exprRun.getHyperparametersList()) {
            if (kv.getKey().equals("C")) {
              assertTrue(
                  "Value should be GTE 0.0001 " + kv, kv.getValue().getNumberValue() >= 0.0001);
            } else if (kv.getKey().equals("train")) {
              if (index == 0) {
                assertEquals(
                    "Value should be GTE 1 " + kv, 3.0F, kv.getValue().getNumberValue(), 0.0);
              } else {
                assertEquals(
                    "Value should be GTE 1 " + kv, 5.0F, kv.getValue().getNumberValue(), 0.0);
              }
            }
          }
        }

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(keyValueQuery)
                .setAscending(false)
                .setIdsOnly(false)
                .setPageLimit(1)
                .setPageNumber(1)
                .setSortKey("hyperparameters.train")
                .build();

        response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

        assertEquals(
            "Total records count not matched with expected records count",
            2,
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            1,
            response.getExperimentRunsCount());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            experimentRunConfig2.getId(),
            response.getExperimentRuns(0).getId());
        for (ExperimentRun exprRun : response.getExperimentRunsList()) {
          for (KeyValue kv : exprRun.getHyperparametersList()) {
            if (kv.getKey().equals("train")) {
              assertTrue("Value should be GTE 1 " + kv, kv.getValue().getNumberValue() > 1);
            }
          }
        }

        Value oldHyperparameterFilter = Value.newBuilder().setNumberValue(0.0002).build();
        KeyValueQuery oldKeyValueQuery =
            KeyValueQuery.newBuilder()
                .setKey("hyperparameters.C")
                .setValue(oldHyperparameterFilter)
                .setOperator(Operator.GTE)
                .setValueType(ValueType.NUMBER)
                .build();

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(oldKeyValueQuery)
                .setAscending(true)
                .setIdsOnly(false)
                .setSortKey("hyperparameters.C")
                .build();

        response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

        assertEquals(
            "Total records count not matched with expected records count",
            2,
            response.getTotalRecords());

        for (int index = 0; index < response.getExperimentRunsCount(); index++) {
          ExperimentRun exprRun = response.getExperimentRuns(index);
          for (KeyValue kv : exprRun.getHyperparametersList()) {
            if (kv.getKey().equals("C")) {
              assertTrue(
                  "Value should be GTE 0.0001 " + kv, kv.getValue().getNumberValue() > 0.0001);
            }
          }
        }

        oldHyperparameterFilter = Value.newBuilder().setNumberValue(1).build();
        oldKeyValueQuery =
            KeyValueQuery.newBuilder()
                .setKey("hyperparameters.train")
                .setValue(oldHyperparameterFilter)
                .setOperator(Operator.GTE)
                .setValueType(ValueType.NUMBER)
                .build();

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(oldKeyValueQuery)
                .setAscending(false)
                .setIdsOnly(false)
                .setSortKey("hyperparameters.C")
                .build();

        response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

        assertEquals(
            "Total records count not matched with expected records count",
            2,
            response.getTotalRecords());

        for (int index = 0; index < response.getExperimentRunsCount(); index++) {
          ExperimentRun exprRun = response.getExperimentRuns(index);
          for (KeyValue kv : exprRun.getHyperparametersList()) {
            if (kv.getKey().equals("C")) {
              assertTrue(
                  "Value should be GTE 0.0001 " + kv, kv.getValue().getNumberValue() > 0.0001);
            }
          }
        }

        hyperparameterFilter = Value.newBuilder().setNumberValue(5).build();
        keyValueQuery =
            KeyValueQuery.newBuilder()
                .setKey("hyperparameters.train")
                .setValue(hyperparameterFilter)
                .setOperator(Operator.NE)
                .setValueType(ValueType.NUMBER)
                .build();

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(keyValueQuery)
                .setAscending(false)
                .setIdsOnly(false)
                .setSortKey("hyperparameters.train")
                .build();

        response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

        assertEquals(
            "Total records count not matched with expected records count",
            3 + experimentRunMap.size(),
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            3 + experimentRunMap.size(),
            response.getExperimentRunsCount());

        // FIX ME: Fix findExperimentRun with string value predicate
        /*hyperparameterFilter = Value.newBuilder().setStringValue("abc").build();
        keyValueQuery =
            KeyValueQuery.newBuilder()
                .setKey("hyperparameters.C")
                .setValue(hyperparameterFilter)
                .setOperator(Operator.CONTAIN)
                .setValueType(ValueType.STRING)
                .build();

        findExperimentRuns =
            FindExperimentRuns.newBuilder()
                .setProjectId(project.getId())
                .addPredicates(keyValueQuery)
                .setAscending(false)
                .setIdsOnly(false)
                .setSortKey("hyperparameters.train")
                .build();

        response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

        assertEquals(
            "Total records count not matched with expected records count",
            1,
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            1,
            response.getExperimentRunsCount());*/

      } finally {
        DeleteRepositoryRequest deleteRepository =
            DeleteRepositoryRequest.newBuilder()
                .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
                .build();
        DeleteRepositoryRequest.Response deleteResult =
            versioningServiceBlockingStub.deleteRepository(deleteRepository);
        Assert.assertTrue(deleteResult.getStatus());
      }
    } finally {
      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("FindExperimentRuns test stop................................");
  }

  @Test
  public void findExperimentRunsCodeConfigWithRepository()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("FindExperimentRuns test start................................");

    long repoId =
        RepositoryTest.createRepository(
            versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);
    Commit commit =
        Commit.newBuilder()
            .setMessage("this is the test commit message")
            .setDateCreated(111)
            .addParentShas(getBranchResponse.getCommit().getCommitSha())
            .build();
    Location datasetLocation =
        Location.newBuilder().addLocation("dataset").addLocation("train").build();
    Location test1Location =
        Location.newBuilder().addLocation("test-1").addLocation("test1.json").build();
    Location test2Location =
        Location.newBuilder().addLocation("test-2").addLocation("test2.json").build();

    CreateCommitRequest createCommitRequest =
        CreateCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommit(commit)
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(datasetLocation.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.CODE))
                    .addAllLocation(test1Location.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(
                        Blob.newBuilder()
                            .setCode(
                                CodeBlob.newBuilder()
                                    .setGit(
                                        GitCodeBlob.newBuilder()
                                            .setBranch("abcd")
                                            .setRepo("Repo-" + new Date().getTime())
                                            .setHash(FileHasher.getSha(""))
                                            .setIsDirty(false)
                                            .setTag(
                                                "Tag-" + Calendar.getInstance().getTimeInMillis())
                                            .build())))
                    .addAllLocation(test2Location.getLocationList())
                    .build())
            .build();
    CreateCommitRequest.Response commitResponse =
        versioningServiceBlockingStub.createCommit(createCommitRequest);
    commit = commitResponse.getCommit();

    List<String> experimentRunIds = new ArrayList<>();
    try {
      Map<String, Location> locationMap = new HashMap<>();
      locationMap.put("location-1", datasetLocation);

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      KeyValue hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .addHyperparameters(hyperparameter1)
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      locationMap.put("location-2", test1Location);

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun2 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun2.getId());
      LOGGER.info("ExperimentRun created successfully");

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      Map<String, Location> locationMap2 = new HashMap<>();
      locationMap2.put("location-111", test2Location);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap2)
                      .build())
              .addHyperparameters(hyperparameter1)
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun3 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRun3.getId());
      LOGGER.info("ExperimentRun created successfully");

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 1e-6);
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      FindExperimentRuns findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .setAscending(false)
              .setIdsOnly(false)
              .build();

      FindExperimentRuns.Response response =
          experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      assertEquals(
          "Total records count not matched with expected records count",
          4 + experimentRunMap.size(),
          response.getTotalRecords());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          4 + experimentRunMap.size(),
          response.getExperimentRunsCount());
      for (ExperimentRun exprRun : response.getExperimentRunsList()) {
        if (exprRun.getId().equals(experimentRun2.getId())) {
          String locationKey =
              ModelDBUtils.getLocationWithSlashOperator(test1Location.getLocationList());
          if (testConfig.isPopulateConnectionsBasedOnPrivileges()) {
            assertFalse(
                "Code blob should not empty", exprRun.containsCodeVersionFromBlob(locationKey));
          } else {
            assertTrue(
                "Code blob should not empty", exprRun.containsCodeVersionFromBlob(locationKey));
            assertFalse(
                "Expected code config not found in map",
                exprRun
                    .getCodeVersionFromBlobOrThrow(locationKey)
                    .getGitSnapshot()
                    .getFilepathsList()
                    .isEmpty());
          }
        } else if (exprRun.getId().equals(experimentRun3.getId())) {
          String locationKey =
              ModelDBUtils.getLocationWithSlashOperator(test2Location.getLocationList());
          if (testConfig.isPopulateConnectionsBasedOnPrivileges()) {
            assertFalse(
                "Code blob should not empty", exprRun.containsCodeVersionFromBlob(locationKey));
          } else {
            assertTrue(
                "Code blob should not empty", exprRun.containsCodeVersionFromBlob(locationKey));
            assertTrue(
                "Expected code config not found in map",
                exprRun
                    .getCodeVersionFromBlobOrThrow(locationKey)
                    .getGitSnapshot()
                    .getFilepathsList()
                    .isEmpty());
          }
        }
      }

      GetExperimentRunById.Response getHydratedExperimentRunsResponse =
          experimentRunServiceStub.getExperimentRunById(
              GetExperimentRunById.newBuilder().setId(experimentRun2.getId()).build());
      ExperimentRun exprRun = getHydratedExperimentRunsResponse.getExperimentRun();
      String locationKey =
          ModelDBUtils.getLocationWithSlashOperator(test1Location.getLocationList());
      if (testConfig.isPopulateConnectionsBasedOnPrivileges()) {
        assertFalse("Code blob should not empty", exprRun.containsCodeVersionFromBlob(locationKey));
      } else {
        assertTrue("Code blob should not empty", exprRun.containsCodeVersionFromBlob(locationKey));
        assertFalse(
            "Expected code config not found in map",
            exprRun
                .getCodeVersionFromBlobOrThrow(locationKey)
                .getGitSnapshot()
                .getFilepathsList()
                .isEmpty());
      }
    } finally {

      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());

      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("FindExperimentRuns test stop................................");
  }

  @Test
  public void findExperimentRunsHyperparameterWithStringNumberFloat()
      throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("FindExperimentRuns test start................................");

    long repoId =
        RepositoryTest.createRepository(
            versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    List<String> experimentRunIds = new ArrayList<>();
    try {
      GetBranchRequest getBranchRequest =
          GetBranchRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setBranch(ModelDBConstants.MASTER_BRANCH)
              .build();
      GetBranchRequest.Response getBranchResponse =
          versioningServiceBlockingStub.getBranch(getBranchRequest);
      Commit commit =
          Commit.newBuilder()
              .setMessage("this is the test commit message")
              .setDateCreated(111)
              .addParentShas(getBranchResponse.getCommit().getCommitSha())
              .build();
      Location location1 =
          Location.newBuilder().addLocation("dataset").addLocation("train").build();
      Location location2 =
          Location.newBuilder().addLocation("test-1").addLocation("test1.json").build();
      Location location3 =
          Location.newBuilder().addLocation("test-2").addLocation("test2.json").build();
      Location location4 =
          Location.newBuilder().addLocation("test-location-4").addLocation("test4.json").build();

      CreateCommitRequest createCommitRequest =
          CreateCommitRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
              .setCommit(commit)
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                      .addAllLocation(location1.getLocationList())
                      .build())
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getHyperparameterConfigBlob(3F, 3F))
                      .addAllLocation(location2.getLocationList())
                      .build())
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                      .addAllLocation(location3.getLocationList())
                      .build())
              .addBlobs(
                  BlobExpanded.newBuilder()
                      .setBlob(CommitTest.getHyperparameterConfigBlob(5.1F, 2.5F))
                      .addAllLocation(location4.getLocationList())
                      .build())
              .build();
      CreateCommitRequest.Response commitResponse =
          versioningServiceBlockingStub.createCommit(createCommitRequest);
      commit = commitResponse.getCommit();

      Map<String, Location> locationMap = new HashMap<>();
      locationMap.put("location-1", location1);

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      KeyValue hyperparameter1 = generateNumericKeyValue("C1", 0.0001);
      KeyValue hyperparameter2 =
          KeyValue.newBuilder()
              .setKey("C")
              .setValue(Value.newBuilder().setStringValue("2.5").build())
              .build();
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .addHyperparameters(hyperparameter1)
              .addHyperparameters(hyperparameter2)
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      locationMap.put("location-2", location2);

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .addHyperparameters(
                  KeyValue.newBuilder()
                      .setKey("C")
                      .setValue(Value.newBuilder().setStringValue("3").build())
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 0.0002);
      Map<String, Location> locationMap2 = new HashMap<>();
      locationMap2.put("location-4", location4);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap2)
                      .build())
              .addHyperparameters(hyperparameter1)
              .addHyperparameters(
                  KeyValue.newBuilder()
                      .setKey("D")
                      .setValue(Value.newBuilder().setStringValue("test_hyper").build())
                      .build())
              .build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      LOGGER.info("ExperimentRun created successfully");
      ExperimentRun experimentRunConfig2 = createExperimentRunResponse.getExperimentRun();
      experimentRunIds.add(experimentRunConfig2.getId());

      createExperimentRunRequest =
          getCreateExperimentRunRequestSimple(
              project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
      hyperparameter1 = generateNumericKeyValue("C", 0.0003);
      createExperimentRunRequest =
          createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      experimentRunIds.add(createExperimentRunResponse.getExperimentRun().getId());
      LOGGER.info("ExperimentRun created successfully");

      Value hyperparameterFilter = Value.newBuilder().setNumberValue(1).build();
      KeyValueQuery keyValueQuery =
          KeyValueQuery.newBuilder()
              .setKey("hyperparameters.c")
              .setValue(hyperparameterFilter)
              .setOperator(Operator.GTE)
              .setValueType(ValueType.NUMBER)
              .build();

      FindExperimentRuns findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .addPredicates(keyValueQuery)
              .setAscending(false)
              .setIdsOnly(false)
              .setSortKey("hyperparameters.train")
              .build();

      FindExperimentRuns.Response response =
          experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      keyValueQuery =
          KeyValueQuery.newBuilder()
              .setKey("hyperparameters.d")
              .setValue(Value.newBuilder().setStringValue("test_hyper").build())
              .setOperator(Operator.EQ)
              .setValueType(ValueType.STRING)
              .build();
      findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .addPredicates(keyValueQuery)
              .setAscending(true)
              .setIdsOnly(false)
              .setSortKey("hyperparameters.train")
              .build();

      response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      keyValueQuery =
          KeyValueQuery.newBuilder()
              .setKey("hyperparameters.c")
              .setValue(Value.newBuilder().setNumberValue(2.5).build())
              .setOperator(Operator.EQ)
              .setValueType(ValueType.STRING)
              .build();
      findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .addPredicates(keyValueQuery)
              .setAscending(false)
              .setIdsOnly(false)
              .setSortKey("hyperparameters.train")
              .build();

      response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      KeyValueQuery oldKeyValueQuery =
          KeyValueQuery.newBuilder()
              .setKey("hyperparameters.C")
              .setValue(Value.newBuilder().setNumberValue(0.0002).build())
              .setOperator(Operator.GTE)
              .setValueType(ValueType.NUMBER)
              .build();

      findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .addPredicates(oldKeyValueQuery)
              .setAscending(true)
              .setIdsOnly(false)
              .setSortKey("hyperparameters.C")
              .build();

      response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      Value oldHyperparameterFilter = Value.newBuilder().setNumberValue(1).build();
      oldKeyValueQuery =
          KeyValueQuery.newBuilder()
              .setKey("hyperparameters.train")
              .setValue(oldHyperparameterFilter)
              .setOperator(Operator.GTE)
              .setValueType(ValueType.NUMBER)
              .build();

      findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project.getId())
              .addPredicates(oldKeyValueQuery)
              .setAscending(false)
              .setIdsOnly(false)
              .setSortKey("hyperparameters.C")
              .build();

      response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      assertEquals(
          "Total records count not matched with expected records count",
          2,
          response.getTotalRecords());

      for (int index = 0; index < response.getExperimentRunsCount(); index++) {
        ExperimentRun exprRun = response.getExperimentRuns(index);
        for (KeyValue kv : exprRun.getHyperparametersList()) {
          if (kv.getKey().equals("C")) {
            assertTrue(
                "Value should be GTE 0.0001 " + kv,
                (kv.getValue().getKindCase() == KindCase.STRING_VALUE
                        ? Double.parseDouble(kv.getValue().getStringValue())
                        : kv.getValue().getNumberValue())
                    > 0.0001);
          }
        }
      }
    } finally {
      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());

      for (String runId : experimentRunIds) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("FindExperimentRuns test stop................................");
  }

  @Test
  public void findExperimentRunsByDatasetVersionId() {
    LOGGER.info("FindExperimentRuns test start................................");

    DatasetTest datasetTest = new DatasetTest();
    DatasetVersionTest datasetVersionTest = new DatasetVersionTest();
    Map<String, ExperimentRun> experimentRunMap = new HashMap<>();

    CreateExperimentRun createExperimentRunRequest =
        getCreateExperimentRunRequestSimple(
            project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
    KeyValue hyperparameter1 = generateNumericKeyValue("C", 0.0001);
    createExperimentRunRequest =
        createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
    CreateExperimentRun.Response createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun11 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun11.getId(), experimentRun11);
    LOGGER.info("ExperimentRun created successfully");
    createExperimentRunRequest =
        getCreateExperimentRunRequestSimple(
            project.getId(), experiment.getId(), "ExperimentRun-" + new Date().getTime());
    createExperimentRunRequest = createExperimentRunRequest.toBuilder().build();
    createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun12 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun12.getId(), experimentRun12);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun12.getName());

    createExperimentRunRequest =
        getCreateExperimentRunRequestSimple(
            project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
    hyperparameter1 = generateNumericKeyValue("C", 0.0001);
    createExperimentRunRequest =
        createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
    createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun21 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun21.getId(), experimentRun21);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun21.getName());

    createExperimentRunRequest =
        getCreateExperimentRunRequestSimple(
            project.getId(), experiment2.getId(), "ExperimentRun-" + new Date().getTime());
    hyperparameter1 = generateNumericKeyValue("C", 1e-6);
    createExperimentRunRequest =
        createExperimentRunRequest.toBuilder().addHyperparameters(hyperparameter1).build();
    createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    ExperimentRun experimentRun22 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun22.getId(), experimentRun22);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun22.getName());

    List<Dataset> datasetList = new ArrayList<>();
    CreateDataset createDatasetRequest =
        datasetTest.getDatasetRequest("Dataset-" + new Date().getTime());
    CreateDataset.Response createDatasetResponse =
        datasetServiceStub.createDataset(createDatasetRequest);
    Dataset dataset1 = createDatasetResponse.getDataset();
    datasetList.add(dataset1);
    LOGGER.info("CreateDataset Response : \n" + createDatasetResponse.getDataset());
    assertEquals(
        "Dataset name not match with expected dataset name",
        createDatasetRequest.getName(),
        dataset1.getName());

    createDatasetRequest = datasetTest.getDatasetRequest("rental_TEXT_train_data_1.csv");
    createDatasetResponse = datasetServiceStub.createDataset(createDatasetRequest);
    Dataset dataset2 = createDatasetResponse.getDataset();
    datasetList.add(dataset2);
    LOGGER.info("CreateDataset Response : \n" + createDatasetResponse.getDataset());
    assertEquals(
        "Dataset name not match with expected dataset name",
        createDatasetRequest.getName(),
        dataset2.getName());

    List<String> datasetVersionIds = new ArrayList<>();
    // Create two datasetVersion of above datasetVersion
    CreateDatasetVersion createDatasetVersionRequest =
        datasetVersionTest.getDatasetVersionRequest(dataset1.getId());
    KeyValue attribute1 =
        KeyValue.newBuilder()
            .setKey("attribute_1")
            .setValue(Value.newBuilder().setNumberValue(0.012).build())
            .build();
    KeyValue attribute2 =
        KeyValue.newBuilder()
            .setKey("attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.99).build())
            .build();
    createDatasetVersionRequest =
        createDatasetVersionRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_2")
            .build();
    CreateDatasetVersion.Response createDatasetVersionResponse =
        datasetVersionServiceStub.createDatasetVersion(createDatasetVersionRequest);
    DatasetVersion datasetVersion1 = createDatasetVersionResponse.getDatasetVersion();
    datasetVersionIds.add(datasetVersion1.getId());
    LOGGER.info("DatasetVersion created successfully");

    // datasetVersion2 of above datasetVersion
    createDatasetVersionRequest = datasetVersionTest.getDatasetVersionRequest(dataset2.getId());
    attribute1 =
        KeyValue.newBuilder()
            .setKey("attribute_1")
            .setValue(Value.newBuilder().setNumberValue(0.31).build())
            .build();
    attribute2 =
        KeyValue.newBuilder()
            .setKey("attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.31).build())
            .build();
    createDatasetVersionRequest =
        createDatasetVersionRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_3")
            .addTags("Tag_4")
            .build();
    createDatasetVersionResponse =
        datasetVersionServiceStub.createDatasetVersion(createDatasetVersionRequest);
    DatasetVersion datasetVersion2 = createDatasetVersionResponse.getDatasetVersion();
    datasetVersionIds.add(datasetVersion2.getId());
    LOGGER.info("DatasetVersion created successfully");

    try {

      Map<String, Artifact> artifactMap = new HashMap<>();
      for (Artifact existingDataset : experimentRun11.getDatasetsList()) {
        artifactMap.put(existingDataset.getKey(), existingDataset);
      }
      List<Artifact> artifacts = new ArrayList<>();
      Artifact artifact1 =
          Artifact.newBuilder()
              .setKey("Google Pay datasets_1")
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.DATA)
              .setLinkedArtifactId(datasetVersion1.getId())
              .setUploadCompleted(
                  !testConfig
                      .artifactStoreConfig
                      .getArtifactStoreType()
                      .equals(ModelDBConstants.S3))
              .build();
      artifacts.add(artifact1);
      artifactMap.put(artifact1.getKey(), artifact1);
      Artifact artifact2 =
          Artifact.newBuilder()
              .setKey("Google Pay datasets_2")
              .setPath("This is new added data artifact type in Google Pay datasets")
              .setArtifactType(ArtifactType.DATA)
              .setLinkedArtifactId(datasetVersion2.getId())
              .setUploadCompleted(
                  !testConfig
                      .artifactStoreConfig
                      .getArtifactStoreType()
                      .equals(ModelDBConstants.S3))
              .build();
      artifacts.add(artifact2);
      artifactMap.put(artifact2.getKey(), artifact2);

      LogDatasets logDatasetRequest =
          LogDatasets.newBuilder().setId(experimentRun11.getId()).addAllDatasets(artifacts).build();

      experimentRunServiceStub.logDatasets(logDatasetRequest);

      GetExperimentRunById getExperimentRunById =
          GetExperimentRunById.newBuilder().setId(experimentRun11.getId()).build();
      GetExperimentRunById.Response getExperimentRunByIdResponse =
          experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info("LogDataset Response : \n" + getExperimentRunByIdResponse.getExperimentRun());

      for (Artifact datasetArtifact :
          getExperimentRunByIdResponse.getExperimentRun().getDatasetsList()) {
        assertEquals(
            "Experiment datasets not match with expected datasets",
            artifactMap.get(datasetArtifact.getKey()),
            datasetArtifact);
      }
      experimentRun11 = getExperimentRunByIdResponse.getExperimentRun();

      GetExperimentRunsByDatasetVersionId getExperimentRunsByDatasetVersionId =
          GetExperimentRunsByDatasetVersionId.newBuilder()
              .setDatasetVersionId(datasetVersion1.getId())
              .build();

      GetExperimentRunsByDatasetVersionId.Response response =
          experimentRunServiceStub.getExperimentRunsByDatasetVersionId(
              getExperimentRunsByDatasetVersionId);

      assertEquals(
          "Total records count not matched with expected records count",
          1,
          response.getTotalRecords());
      assertEquals(
          "ExperimentRun count not match with expected experimentRun count",
          1,
          response.getExperimentRunsCount());
      assertEquals(
          "ExperimentRun not match with expected experimentRun",
          experimentRun11,
          response.getExperimentRuns(0));

      if (testConfig.hasAuth()) {
        AddCollaboratorRequest addCollaboratorRequest =
            addCollaboratorRequestProjectInterceptor(
                project, CollaboratorType.READ_ONLY, authClientInterceptor);

        AddCollaboratorRequest.Response addCollaboratorResponse =
            collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
        LOGGER.info("Collaborator updated in server : " + addCollaboratorResponse.getStatus());
        assertTrue(addCollaboratorResponse.getStatus());

        response =
            experimentRunServiceStubClient2.getExperimentRunsByDatasetVersionId(
                getExperimentRunsByDatasetVersionId);

        assertEquals(
            "Total records count not matched with expected records count",
            1,
            response.getTotalRecords());
        assertEquals(
            "ExperimentRun count not match with expected experimentRun count",
            1,
            response.getExperimentRunsCount());
        if (testConfig.isPopulateConnectionsBasedOnPrivileges()) {
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              0,
              response.getExperimentRuns(0).getDatasetsCount());
        } else {
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              2,
              response.getExperimentRuns(0).getDatasetsCount());
        }
      }
    } finally {
      for (String datasetVersionId : datasetVersionIds) {
        DeleteDatasetVersion deleteDatasetVersion =
            DeleteDatasetVersion.newBuilder().setId(datasetVersionId).build();
        DeleteDatasetVersion.Response deleteDatasetVersionResponse =
            datasetVersionServiceStub.deleteDatasetVersion(deleteDatasetVersion);
        LOGGER.info("DatasetVersion deleted successfully");
        LOGGER.info(deleteDatasetVersionResponse.toString());
      }

      for (Dataset dataset : datasetList) {
        DeleteDataset deleteDataset = DeleteDataset.newBuilder().setId(dataset.getId()).build();
        DeleteDataset.Response deleteDatasetResponse =
            datasetServiceStub.deleteDataset(deleteDataset);
        LOGGER.info("Dataset deleted successfully");
        LOGGER.info(deleteDatasetResponse.toString());
        assertTrue(deleteDatasetResponse.getStatus());
      }
      for (String runId : experimentRunMap.keySet()) {
        DeleteExperimentRun deleteExperimentRun =
            DeleteExperimentRun.newBuilder().setId(runId).build();
        DeleteExperimentRun.Response deleteExperimentRunResponse =
            experimentRunServiceStub.deleteExperimentRun(deleteExperimentRun);
        assertTrue(deleteExperimentRunResponse.getStatus());
      }
    }

    LOGGER.info("FindExperimentRuns test stop................................");
  }

  @Test
  public void deleteExperimentRunHyperparameters() {
    LOGGER.info("Delete ExperimentRun Hyperparameters test start................................");

    o_logHyperparametersTest();
    List<KeyValue> hyperparameters = experimentRun.getHyperparametersList();
    LOGGER.info("Hyperparameters size : " + hyperparameters.size());
    List<String> keys = new ArrayList<>();
    for (int index = 0; index < hyperparameters.size() - 1; index++) {
      KeyValue keyValue = hyperparameters.get(index);
      keys.add(keyValue.getKey());
    }
    LOGGER.info("Hyperparameters key size : " + keys.size());

    DeleteHyperparameters request =
        DeleteHyperparameters.newBuilder()
            .setId(experimentRun.getId())
            .addAllHyperparameterKeys(keys)
            .build();

    experimentRunServiceStub.deleteHyperparameters(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info(
        "DeleteExperimentRunHyperparameters Response : \n"
            + response.getExperimentRun().getHyperparametersList());
    assertTrue(response.getExperimentRun().getHyperparametersList().size() <= 1);

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    if (response.getExperimentRun().getHyperparametersList().size() != 0) {
      request =
          DeleteHyperparameters.newBuilder()
              .setId(experimentRun.getId())
              .setDeleteAll(true)
              .build();

      experimentRunServiceStub.deleteHyperparameters(request);

      response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info(
          "DeleteExperimentRunHyperparameters Response : \n"
              + response.getExperimentRun().getHyperparametersList());
      assertEquals(0, response.getExperimentRun().getHyperparametersList().size());

      assertNotEquals(
          "ExperimentRun date_updated field not update on database",
          experimentRun.getDateUpdated(),
          response.getExperimentRun().getDateUpdated());
      experimentRun = response.getExperimentRun();
      experimentRunMap.put(experimentRun.getId(), experimentRun);
    }

    LOGGER.info("Delete ExperimentRun Hyperparameters test stop................................");
  }

  @Test
  public void deleteExperimentRunMetrics() {
    LOGGER.info("Delete ExperimentRun Metrics test start................................");

    i_logMetricsTest();
    List<KeyValue> metrics = experimentRun.getMetricsList();
    LOGGER.info("Metrics size : " + metrics.size());
    List<String> keys = new ArrayList<>();
    for (int index = 0; index < metrics.size() - 1; index++) {
      KeyValue keyValue = metrics.get(index);
      keys.add(keyValue.getKey());
    }
    LOGGER.info("Metrics key size : " + keys.size());

    DeleteMetrics request =
        DeleteMetrics.newBuilder().setId(experimentRun.getId()).addAllMetricKeys(keys).build();

    experimentRunServiceStub.deleteMetrics(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info(
        "DeleteExperimentRunMetrics Response : \n" + response.getExperimentRun().getMetricsList());
    assertTrue(response.getExperimentRun().getMetricsList().size() <= 1);

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    if (response.getExperimentRun().getMetricsList().size() != 0) {
      request = DeleteMetrics.newBuilder().setId(experimentRun.getId()).setDeleteAll(true).build();

      experimentRunServiceStub.deleteMetrics(request);

      response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info(
          "DeleteExperimentRunMetrics Response : \n"
              + response.getExperimentRun().getMetricsList());
      assertEquals(0, response.getExperimentRun().getMetricsList().size());

      assertNotEquals(
          "ExperimentRun date_updated field not update on database",
          experimentRun.getDateUpdated(),
          response.getExperimentRun().getDateUpdated());
      experimentRun = response.getExperimentRun();
      experimentRunMap.put(experimentRun.getId(), experimentRun);
    }

    LOGGER.info("Delete ExperimentRun Metrics test stop................................");
  }

  @Test
  public void deleteExperimentRunObservations() {
    LOGGER.info("Delete ExperimentRun Observations test start................................");

    g_logObservationsTest();
    List<Observation> observations = experimentRun.getObservationsList();
    LOGGER.info("Observations size : " + observations.size());
    List<String> keys = new ArrayList<>();
    for (int index = 0; index < observations.size() - 1; index++) {
      Observation observation = observations.get(index);
      if (observation.getOneOfCase().equals(Observation.OneOfCase.ATTRIBUTE)) {
        keys.add(observation.getAttribute().getKey());
      } else {
        keys.add(observation.getArtifact().getKey());
      }
    }
    LOGGER.info("Observations key size : " + keys.size());

    DeleteObservations request =
        DeleteObservations.newBuilder()
            .setId(experimentRun.getId())
            .addAllObservationKeys(keys)
            .build();

    experimentRunServiceStub.deleteObservations(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();
    GetExperimentRunById.Response response =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    LOGGER.info(
        "DeleteExperimentRunObservations Response : \n"
            + response.getExperimentRun().getObservationsList());
    assertTrue(response.getExperimentRun().getObservationsList().size() <= 1);

    assertNotEquals(
        "ExperimentRun date_updated field not update on database",
        experimentRun.getDateUpdated(),
        response.getExperimentRun().getDateUpdated());
    experimentRun = response.getExperimentRun();
    experimentRunMap.put(experimentRun.getId(), experimentRun);

    if (response.getExperimentRun().getObservationsList().size() != 0) {
      request =
          DeleteObservations.newBuilder().setId(experimentRun.getId()).setDeleteAll(true).build();

      experimentRunServiceStub.deleteObservations(request);

      response = experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
      LOGGER.info(
          "DeleteExperimentRunObservations Response : \n"
              + response.getExperimentRun().getObservationsList());
      assertEquals(0, response.getExperimentRun().getObservationsList().size());

      assertNotEquals(
          "ExperimentRun date_updated field not update on database",
          experimentRun.getDateUpdated(),
          response.getExperimentRun().getDateUpdated());
      experimentRun = response.getExperimentRun();
      experimentRunMap.put(experimentRun.getId(), experimentRun);
    }

    LOGGER.info("Delete ExperimentRun Observations test stop................................");
  }

  @Test
  public void cloneExperimentRun() throws ModelDBException, NoSuchAlgorithmException {
    LOGGER.info("Clone experimentRun test start................................");

    ProjectTest projectTest = new ProjectTest();
    long repoId =
        RepositoryTest.createRepository(
            versioningServiceBlockingStub, "Repo-" + new Date().getTime());
    GetBranchRequest getBranchRequest =
        GetBranchRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setBranch(ModelDBConstants.MASTER_BRANCH)
            .build();
    GetBranchRequest.Response getBranchResponse =
        versioningServiceBlockingStub.getBranch(getBranchRequest);
    Commit commit =
        Commit.newBuilder()
            .setMessage("this is the test commit message")
            .setDateCreated(111)
            .addParentShas(getBranchResponse.getCommit().getCommitSha())
            .build();
    Location location1 = Location.newBuilder().addLocation("dataset").addLocation("train").build();
    Location location2 =
        Location.newBuilder().addLocation("test-1").addLocation("test1.json").build();
    Location location3 =
        Location.newBuilder().addLocation("test-2").addLocation("test2.json").build();
    Location location4 =
        Location.newBuilder().addLocation("test-location-4").addLocation("test4.json").build();

    CreateCommitRequest createCommitRequest =
        CreateCommitRequest.newBuilder()
            .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId).build())
            .setCommit(commit)
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location1.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.CONFIG))
                    .addAllLocation(location2.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getBlob(Blob.ContentCase.DATASET))
                    .addAllLocation(location3.getLocationList())
                    .build())
            .addBlobs(
                BlobExpanded.newBuilder()
                    .setBlob(CommitTest.getHyperparameterConfigBlob(0.14F, 0.10F))
                    .addAllLocation(location4.getLocationList())
                    .build())
            .build();
    CreateCommitRequest.Response commitResponse =
        versioningServiceBlockingStub.createCommit(createCommitRequest);
    commit = commitResponse.getCommit();

    // Create project
    CreateProject createProjectRequest =
        projectTest.getCreateProjectRequest("experimentRun-project-" + new Date().getTime());
    CreateProject.Response createProjectResponse =
        projectServiceStub.createProject(createProjectRequest);
    Project project1 = createProjectResponse.getProject();
    LOGGER.info("Project1 created successfully");

    createProjectRequest =
        projectTest.getCreateProjectRequest("experimentRun-project-" + new Date().getTime());
    createProjectResponse = projectServiceStub.createProject(createProjectRequest);
    Project project2 = createProjectResponse.getProject();
    LOGGER.info("Project2 created successfully");

    try {
      // Create two experiment of above project
      CreateExperiment createExperimentRequest =
          ExperimentTest.getCreateExperimentRequest(
              project1.getId(), "Experiment-1-" + new Date().getTime());
      CreateExperiment.Response createExperimentResponse =
          experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment1 = createExperimentResponse.getExperiment();
      LOGGER.info("Experiment1 created successfully");

      createExperimentRequest =
          ExperimentTest.getCreateExperimentRequest(
              project2.getId(), "Experiment-2-" + new Date().getTime());
      createExperimentResponse = experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment2 = createExperimentResponse.getExperiment();
      LOGGER.info("Experiment2 created successfully");

      Map<String, Location> locationMap = new HashMap<>();
      locationMap.put("location-1", location1);

      CreateExperimentRun createExperimentRunRequest =
          getCreateExperimentRunRequest(
              project1.getId(), experiment1.getId(), "ExperimentRun-" + new Date().getTime());
      KeyValue hyperparameter1 = generateNumericKeyValue("C", 0.0001);
      createExperimentRunRequest =
          createExperimentRunRequest
              .toBuilder()
              .setVersionedInputs(
                  VersioningEntry.newBuilder()
                      .setRepositoryId(repoId)
                      .setCommit(commitResponse.getCommit().getCommitSha())
                      .putAllKeyLocationMap(locationMap)
                      .build())
              .addHyperparameters(hyperparameter1)
              .build();
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      LOGGER.info("ExperimentRun created successfully");
      ExperimentRun srcExperimentRun = createExperimentRunResponse.getExperimentRun();

      CloneExperimentRun cloneExperimentRun =
          CloneExperimentRun.newBuilder().setSrcExperimentRunId(srcExperimentRun.getId()).build();
      CloneExperimentRun.Response cloneResponse =
          experimentRunServiceStub.cloneExperimentRun(cloneExperimentRun);
      assertNotEquals(
          "Clone run id should not be match with source run id",
          srcExperimentRun.getId(),
          cloneResponse.getRun().getId());
      srcExperimentRun =
          srcExperimentRun
              .toBuilder()
              .setId(cloneResponse.getRun().getId())
              .setName(cloneResponse.getRun().getName())
              .setDateCreated(cloneResponse.getRun().getDateCreated())
              .setDateUpdated(cloneResponse.getRun().getDateUpdated())
              .setStartTime(cloneResponse.getRun().getStartTime())
              .setEndTime(cloneResponse.getRun().getEndTime())
              .build();
      assertEquals(
          "Clone experimentRun can not match with expected experimentRun",
          srcExperimentRun,
          cloneResponse.getRun());

      cloneExperimentRun =
          CloneExperimentRun.newBuilder()
              .setSrcExperimentRunId(srcExperimentRun.getId())
              .setDestExperimentRunName("Test - " + Calendar.getInstance().getTimeInMillis())
              .setDestExperimentId(experiment2.getId())
              .build();
      cloneResponse = experimentRunServiceStub.cloneExperimentRun(cloneExperimentRun);
      assertNotEquals(
          "Clone run id should not be match with source run id",
          srcExperimentRun.getId(),
          cloneResponse.getRun().getId());
      srcExperimentRun =
          srcExperimentRun
              .toBuilder()
              .setId(cloneResponse.getRun().getId())
              .setName(cloneExperimentRun.getDestExperimentRunName())
              .setProjectId(cloneResponse.getRun().getProjectId())
              .setExperimentId(cloneExperimentRun.getDestExperimentId())
              .setDateCreated(cloneResponse.getRun().getDateCreated())
              .setDateUpdated(cloneResponse.getRun().getDateUpdated())
              .setStartTime(cloneResponse.getRun().getStartTime())
              .setEndTime(cloneResponse.getRun().getEndTime())
              .build();
      assertEquals(
          "Clone experimentRun can not match with expected experimentRun",
          srcExperimentRun,
          cloneResponse.getRun());

      try {
        cloneExperimentRun =
            CloneExperimentRun.newBuilder()
                .setSrcExperimentRunId(srcExperimentRun.getId())
                .setDestExperimentId("XYZ")
                .build();
        experimentRunServiceStub.cloneExperimentRun(cloneExperimentRun);
        fail();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        LOGGER.warn(
            "Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
      }
    } finally {
      DeleteRepositoryRequest deleteRepository =
          DeleteRepositoryRequest.newBuilder()
              .setRepositoryId(RepositoryIdentification.newBuilder().setRepoId(repoId))
              .build();
      DeleteRepositoryRequest.Response deleteResult =
          versioningServiceBlockingStub.deleteRepository(deleteRepository);
      Assert.assertTrue(deleteResult.getStatus());

      for (Project project : new Project[] {project1, project2}) {
        DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
        DeleteProject.Response deleteProjectResponse =
            projectServiceStub.deleteProject(deleteProject);
        LOGGER.info("Project deleted successfully");
        LOGGER.info(deleteProjectResponse.toString());
        assertTrue(deleteProjectResponse.getStatus());
      }
    }

    LOGGER.info("Clone experimentRun test stop................................");
  }

  @Test
  public void randomNameGeneration() {
    LOGGER.info("Random name generation test start................................");
    GenerateRandomNameRequest.Response response =
        metadataServiceBlockingStub.generateRandomName(
            GenerateRandomNameRequest.newBuilder().build());
    LOGGER.info("Random name: {}", response.getName());
    assertFalse("Random name should not be empty", response.getName().isEmpty());
    LOGGER.info("Random name generation test stop................................");
  }

  @Test
  public void logEnvironmentTest() {
    LOGGER.info("logEnvironment test start................................");
    EnvironmentBlob environmentBlob =
        EnvironmentBlob.newBuilder()
            .setPython(
                PythonEnvironmentBlob.newBuilder()
                    .setVersion(
                        VersionEnvironmentBlob.newBuilder().setMajor(3).setMinor(7).setPatch(5))
                    .addRequirements(
                        PythonRequirementEnvironmentBlob.newBuilder()
                            .setLibrary("pytest")
                            .setConstraint("==")
                            .setVersion(VersionEnvironmentBlob.newBuilder().setMajor(1))
                            .build())
                    .addRequirements(
                        PythonRequirementEnvironmentBlob.newBuilder()
                            .setLibrary("verta")
                            .setConstraint("==")
                            .setVersion(
                                VersionEnvironmentBlob.newBuilder().setMajor(14).setMinor(9))))
            .build();
    LogEnvironment request =
        LogEnvironment.newBuilder()
            .setId(experimentRun.getId())
            .setEnvironment(environmentBlob)
            .build();

    experimentRunServiceStub.logEnvironment(request);

    GetExperimentRunById getExperimentRunById =
        GetExperimentRunById.newBuilder().setId(experimentRun.getId()).build();

    GetExperimentRunById.Response getExperimentRunByIdResponse =
        experimentRunServiceStub.getExperimentRunById(getExperimentRunById);
    assertEquals(
        "environment not match with expected environment",
        request.getEnvironment(),
        getExperimentRunByIdResponse.getExperimentRun().getEnvironment());

    try {
      experimentRunServiceStub.logEnvironment(request.toBuilder().clearId().build());
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("logEnvironment test stop................................");
  }
}
