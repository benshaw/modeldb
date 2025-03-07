package ai.verta.modeldb;

import static org.junit.Assert.*;

import ai.verta.common.Artifact;
import ai.verta.common.ArtifactTypeEnum.ArtifactType;
import ai.verta.common.CodeVersion;
import ai.verta.common.KeyValue;
import ai.verta.common.ValueTypeEnum.ValueType;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExperimentTest extends TestsInit {

  public static final Logger LOGGER = LogManager.getLogger(ExperimentTest.class);

  // Project Entities
  private static Project project;

  // Experiment Entities
  private static Experiment experiment;

  @Before
  public void createEntities() {
    // Create all entities
    createProjectEntities();
    createExperimentEntities();
  }

  @After
  public void removeEntities() {
    DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
    DeleteProject.Response deleteProjectResponse = projectServiceStub.deleteProject(deleteProject);
    LOGGER.info("Project deleted successfully");
    LOGGER.info(deleteProjectResponse.toString());
    assertTrue(deleteProjectResponse.getStatus());

    project = null;

    // Experiment Entities
    experiment = null;
  }

  private static void createProjectEntities() {
    ProjectTest projectTest = new ProjectTest();

    // Create two project of above project
    CreateProject createProjectRequest =
        projectTest.getCreateProjectRequest("project-" + new Date().getTime());
    CreateProject.Response createProjectResponse =
        projectServiceStub.createProject(createProjectRequest);
    project = createProjectResponse.getProject();
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected Project name",
        createProjectRequest.getName(),
        project.getName());
  }

  private static void createExperimentEntities() {
    CreateExperiment createExperimentRequest =
        getCreateExperimentRequest(project.getId(), "Experiment-" + new Date().getTime());
    CreateExperiment.Response createExperimentResponse =
        experimentServiceStub.createExperiment(createExperimentRequest);
    experiment = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment.getName());
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

  public static CreateExperiment getCreateExperimentRequest(
      String projectId, String experimentName) {
    List<KeyValue> attributeList = new ArrayList<>();
    Value stringValue =
        Value.newBuilder()
            .setStringValue("attribute_" + Calendar.getInstance().getTimeInMillis() + "_value")
            .build();
    KeyValue keyValue =
        KeyValue.newBuilder()
            .setKey("attribute_1_" + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .build();
    attributeList.add(keyValue);

    Value intValue = Value.newBuilder().setNumberValue(12345).build();
    keyValue =
        KeyValue.newBuilder()
            .setKey("attribute_2_" + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build();
    attributeList.add(keyValue);

    Value listValue =
        Value.newBuilder()
            .setListValue(ListValue.newBuilder().addValues(intValue).addValues(stringValue).build())
            .build();
    keyValue =
        KeyValue.newBuilder()
            .setKey("attribute_3_" + Calendar.getInstance().getTimeInMillis())
            .setValue(listValue)
            .setValueType(ValueType.LIST)
            .build();
    attributeList.add(keyValue);

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

    return CreateExperiment.newBuilder()
        .setProjectId(projectId)
        .setName(experimentName)
        .setDescription("This is a experiment description.")
        .setDateCreated(Calendar.getInstance().getTimeInMillis())
        .setDateUpdated(Calendar.getInstance().getTimeInMillis())
        .addTags("tag_x")
        .addTags("tag_y")
        .addAllAttributes(attributeList)
        .addAllArtifacts(artifactList)
        .build();
  }

  @Test
  public void a_experimentCreateTest() {
    LOGGER.info("Create Experiment test start................................");

    CreateExperiment createExperimentRequest =
        getCreateExperimentRequest(project.getId(), experiment.getName());

    try {
      experimentServiceStub.createExperiment(createExperimentRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    try {
      createExperimentRequest = createExperimentRequest.toBuilder().setProjectId("xyz").build();
      experimentServiceStub.createExperiment(createExperimentRequest);
      fail();
    } catch (StatusRuntimeException e) {
      checkEqualsAssert(e);
    }

    try {
      createExperimentRequest =
          createExperimentRequest.toBuilder().setProjectId(project.getId()).addTags("").build();
      experimentServiceStub.createExperiment(createExperimentRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    try {
      String tag52 = "Human Activity Recognition using Smartphone Dataset";
      createExperimentRequest =
          createExperimentRequest.toBuilder().setProjectId(project.getId()).addTags(tag52).build();
      experimentServiceStub.createExperiment(createExperimentRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    try {
      String name =
          "Experiment of Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset";
      createExperimentRequest = getCreateExperimentRequest(project.getId(), name);
      experimentServiceStub.createExperiment(createExperimentRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Create Experiment test stop................................");
  }

  @Test
  public void a_experimentCreateNegativeTest() {
    LOGGER.info("Create Experiment Negative test start................................");
    List<KeyValue> attributeList = new ArrayList<>();
    Value stringValue =
        Value.newBuilder()
            .setStringValue("attributes_value_" + Calendar.getInstance().getTimeInMillis())
            .build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_" + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build());
    stringValue =
        Value.newBuilder()
            .setStringValue("attributes_value_" + Calendar.getInstance().getTimeInMillis())
            .build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_" + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build());

    CreateExperiment request =
        CreateExperiment.newBuilder()
            .setName("experiment-" + Calendar.getInstance().getTimeInMillis())
            .setDescription("This is a experiment description.")
            .addTags("tag_" + Calendar.getInstance().getTimeInMillis())
            .addTags("tag_" + +Calendar.getInstance().getTimeInMillis())
            .addAllAttributes(attributeList)
            .build();

    try {
      experimentServiceStub.createExperiment(request);
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }
  }

  @Test
  public void b_getExperimentsInProject() {
    LOGGER.info("Get Experiment of project test start................................");

    GetExperimentsInProject getExperiment =
        GetExperimentsInProject.newBuilder().setProjectId(project.getId()).build();
    GetExperimentsInProject.Response experimentResponse =
        experimentServiceStub.getExperimentsInProject(getExperiment);
    LOGGER.info("GetExperimentsInProject.Response " + experimentResponse.getExperimentsCount());
    assertEquals(
        "Experiments count not match with expected experiment count",
        1,
        experimentResponse.getExperimentsList().size());
    assertEquals(
        "Experiment list not contain expected experiment",
        experiment,
        experimentResponse.getExperimentsList().get(0));

    LOGGER.info("Get Experiment of project test stop................................");
  }

  @Test
  public void b_getExperimentsWithPaginationInProject() {
    LOGGER.info(
        "Get Experiment with pagination of project test start................................");

    Map<String, Experiment> experimentMap = new HashMap<>();

    CreateExperiment createExperimentRequest =
        getCreateExperimentRequest(project.getId(), "Experiment-" + new Date().getTime());
    Value intValue = Value.newBuilder().setNumberValue(12345).build();
    KeyValue keyValue1 =
        KeyValue.newBuilder()
            .setKey("attribute_2_2")
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build();
    createExperimentRequest = createExperimentRequest.toBuilder().addAttributes(keyValue1).build();
    CreateExperiment.Response createExperimentResponse =
        experimentServiceStub.createExperiment(createExperimentRequest);
    Experiment experiment1 = createExperimentResponse.getExperiment();
    experimentMap.put(experiment1.getId(), experiment1);
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        createExperimentResponse.getExperiment().getName());

    createExperimentRequest =
        getCreateExperimentRequest(project.getId(), "Experiment-" + new Date().getTime());
    intValue = Value.newBuilder().setNumberValue(9876543).build();
    KeyValue keyValue2 =
        KeyValue.newBuilder()
            .setKey("attribute_2_2")
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build();
    createExperimentRequest = createExperimentRequest.toBuilder().addAttributes(keyValue2).build();
    createExperimentResponse = experimentServiceStub.createExperiment(createExperimentRequest);
    Experiment experiment2 = createExperimentResponse.getExperiment();
    experimentMap.put(experiment2.getId(), experiment2);
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        createExperimentResponse.getExperiment().getName());

    int pageLimit = 2;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      GetExperimentsInProject getExperiment =
          GetExperimentsInProject.newBuilder()
              .setProjectId(project.getId())
              .setPageNumber(pageNumber)
              .setPageLimit(pageLimit)
              .setAscending(true)
              .setSortKey(ModelDBConstants.NAME)
              .build();

      GetExperimentsInProject.Response experimentResponse =
          experimentServiceStub.getExperimentsInProject(getExperiment);

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          experimentResponse.getTotalRecords());

      if (experimentResponse.getExperimentsList() != null
          && experimentResponse.getExperimentsList().size() > 0) {
        isExpectedResultFound = true;
        LOGGER.info(
            "GetExperimentsInProject Response : " + experimentResponse.getExperimentsCount());
        for (Experiment exp : experimentResponse.getExperimentsList()) {
          if (exp.getId().equals(experiment.getId())) {
            continue;
          }
          assertEquals(
              "Experiment not match with expected Experiment", experimentMap.get(exp.getId()), exp);
        }

      } else {
        if (isExpectedResultFound) {
          LOGGER.warn("More Experiment not found in database");
          assertTrue(true);
        } else {
          fail("Expected experiment not found in response");
        }
        break;
      }
    }

    pageLimit = 1;
    int count = 0;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      GetExperimentsInProject getExperiment =
          GetExperimentsInProject.newBuilder()
              .setProjectId(project.getId())
              .setPageNumber(pageNumber)
              .setPageLimit(pageLimit)
              .setAscending(true)
              .setSortKey("attributes.attribute_2_2")
              .build();

      GetExperimentsInProject.Response experimentResponse =
          experimentServiceStub.getExperimentsInProject(getExperiment);

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          experimentResponse.getTotalRecords());

      if (experimentResponse.getExperimentsList() != null
          && experimentResponse.getExperimentsList().size() > 0) {

        LOGGER.info(
            "GetExperimentsInProject Response : " + experimentResponse.getExperimentsCount());
        for (Experiment experiment : experimentResponse.getExperimentsList()) {
          if (!experimentMap.containsKey(experiment.getId())) {
            continue;
          }
          assertEquals(
              "Experiment not match with expected Experiment",
              experimentMap.get(experiment.getId()),
              experiment);

          if (count == 0) {
            assertEquals(
                "Experiment attributes not match with expected experiment attributes",
                experiment1.getAttributesList(),
                experiment.getAttributesList());
          } else if (count == 1) {
            assertEquals(
                "Experiment attributes not match with expected experiment attributes",
                experiment2.getAttributesList(),
                experiment.getAttributesList());
          }
          count++;
        }

      } else {
        LOGGER.warn("More Experiment not found in database");
        assertTrue(true);
        break;
      }
    }

    GetExperimentsInProject getExperiment =
        GetExperimentsInProject.newBuilder()
            .setProjectId(project.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(true)
            .setSortKey("observations.attribute.attr_1")
            .build();
    try {
      experimentServiceStub.getExperimentsInProject(getExperiment);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get Experiment with pagination of project test stop................................");
  }

  @Test
  public void b_getExperimentsInProjectNegativeTest() {
    LOGGER.info("Get Experiment of project Negative test start................................");

    GetExperimentsInProject getExperiment = GetExperimentsInProject.newBuilder().build();
    try {
      experimentServiceStub.getExperimentsInProject(getExperiment);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getExperiment = GetExperimentsInProject.newBuilder().setProjectId("hjhfdkshjfhdsk").build();
    try {
      experimentServiceStub.getExperimentsInProject(getExperiment);
      fail();
    } catch (StatusRuntimeException ex) {
      checkEqualsAssert(ex);
    }

    LOGGER.info("Get Experiment of project Negative test stop................................");
  }

  @Test
  public void c_getExperimentById() {
    LOGGER.info("Get Experiment by ID test start................................");

    GetExperimentById experimentRequest =
        GetExperimentById.newBuilder().setId(experiment.getId()).build();

    GetExperimentById.Response response =
        experimentServiceStub.getExperimentById(experimentRequest);
    LOGGER.info("UpdateExperimentNameOrDescription Response : \n" + response.getExperiment());
    assertEquals(
        "Experiment not match with expected experiment", experiment, response.getExperiment());

    LOGGER.info("Get Experiment by ID of project test stop................................");
  }

  @Test
  public void c_getExperimentByIdNegativeTest() {
    LOGGER.info("Get Experiment by ID Negative test start................................");

    GetExperimentById experimentRequest = GetExperimentById.newBuilder().build();

    try {
      experimentServiceStub.getExperimentById(experimentRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    experimentRequest = GetExperimentById.newBuilder().setId("jdhfjkdhsfhdskjf").build();
    try {
      experimentServiceStub.getExperimentById(experimentRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
    }

    LOGGER.info("Get Experiment by ID Negative test stop................................");
  }

  @Test
  public void d_updateExperimentNameOrDescriptionOldTest() {
    LOGGER.info("Update Experiment Name & Description test start................................");

    UpdateExperimentNameOrDescription upDescriptionRequest =
        UpdateExperimentNameOrDescription.newBuilder()
            .setId(experiment.getId())
            .setName(
                "Test Update Experiment Name Or Description "
                    + Calendar.getInstance().getTimeInMillis())
            .setDescription(
                "This is update from UpdateExperimentNameOrDescription "
                    + Calendar.getInstance().getTimeInMillis())
            .build();

    UpdateExperimentNameOrDescription.Response response =
        experimentServiceStub.updateExperimentNameOrDescription(upDescriptionRequest);
    LOGGER.info("UpdateExperimentNameOrDescription Response : " + response.getExperiment());
    assertEquals(
        "Experiment name not match with expected experiment name",
        upDescriptionRequest.getName(),
        response.getExperiment().getName());
    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    try {
      String name =
          "Experiment of Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset";
      upDescriptionRequest = upDescriptionRequest.toBuilder().setName(name).build();
      experimentServiceStub.updateExperimentNameOrDescription(upDescriptionRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Update Experiment Name & Description test stop................................");
  }

  @Test
  public void d_updateExperimentNameOrDescriptionNegativeOldTest() {
    LOGGER.info("Update Experiment Name & Description Negative test start........");

    UpdateExperimentNameOrDescription upDescriptionRequest =
        UpdateExperimentNameOrDescription.newBuilder()
            .setName(
                "Test Update Experiment Name Or Description "
                    + Calendar.getInstance().getTimeInMillis())
            .setDescription(
                "This is update from UpdateExperimentNameOrDescription "
                    + Calendar.getInstance().getTimeInMillis())
            .build();

    try {
      experimentServiceStub.updateExperimentNameOrDescription(upDescriptionRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Update Experiment Name & Description Negative test stop..............");
  }

  @Test
  public void d_updateExperimentNameOrDescription() {
    LOGGER.info("Update Experiment Name & Description test start................................");

    UpdateExperimentDescription upDescriptionRequest =
        UpdateExperimentDescription.newBuilder()
            .setId(experiment.getId())
            .setDescription(
                "This is update from UpdateExperimentDescription "
                    + Calendar.getInstance().getTimeInMillis())
            .build();

    UpdateExperimentDescription.Response response =
        experimentServiceStub.updateExperimentDescription(upDescriptionRequest);
    LOGGER.info("UpdateExperimentDescription Response : " + response.getExperiment());
    assertEquals(
        "Experiment name not match with expected experiment name",
        experiment.getName(),
        response.getExperiment().getName());
    assertEquals(
        "Experiment description not match with expected experiment name",
        upDescriptionRequest.getDescription(),
        response.getExperiment().getDescription());
    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    UpdateExperimentName updateNameRequest =
        UpdateExperimentName.newBuilder()
            .setId(experiment.getId())
            .setName("Test Update Experiment Name" + Calendar.getInstance().getTimeInMillis())
            .build();

    UpdateExperimentName.Response updateNameResponse =
        experimentServiceStub.updateExperimentName(updateNameRequest);
    LOGGER.info("UpdateExperimentName Response : " + response.getExperiment());
    assertEquals(
        "Experiment name not match with expected experiment name",
        updateNameRequest.getName(),
        updateNameResponse.getExperiment().getName());
    assertEquals(
        "Experiment description not match with expected experiment name",
        experiment.getDescription(),
        response.getExperiment().getDescription());
    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        updateNameResponse.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    try {
      String name =
          "Experiment of Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset Human Activity Recognition using Smartphone Dataset";
      updateNameRequest = updateNameRequest.toBuilder().setName(name).build();
      experimentServiceStub.updateExperimentName(updateNameRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    UpdateExperimentNameOrDescription.Response updateExperimentNameOrDescriptionResponse =
        experimentServiceStub.updateExperimentNameOrDescription(
            UpdateExperimentNameOrDescription.newBuilder().setId(experiment.getId()).build());
    LOGGER.info(
        "UpdateExperimentNameOrDescription Response : "
            + updateExperimentNameOrDescriptionResponse.getExperiment());
    assertFalse(
        "Experiment name is empty",
        updateExperimentNameOrDescriptionResponse.getExperiment().getName().isEmpty());
    assertEquals(
        "Experiment description not match with expected experiment name",
        experiment.getDescription(),
        updateExperimentNameOrDescriptionResponse.getExperiment().getDescription());
    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        updateExperimentNameOrDescriptionResponse.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    LOGGER.info("Update Experiment Name & Description test stop................................");
  }

  @Test
  public void d_updateExperimentNameOrDescriptionNegativeTest() {
    LOGGER.info("Update Experiment Name & Description Negative test start.........");

    UpdateExperimentName upNameRequest =
        UpdateExperimentName.newBuilder()
            .setName("Test Update Experiment Name" + Calendar.getInstance().getTimeInMillis())
            .build();

    try {
      experimentServiceStub.updateExperimentName(upNameRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    UpdateExperimentDescription upDescriptionRequest =
        UpdateExperimentDescription.newBuilder()
            .setDescription(
                "Test Update Experiment Description " + Calendar.getInstance().getTimeInMillis())
            .build();

    try {
      experimentServiceStub.updateExperimentDescription(upDescriptionRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Update Experiment Name & Description Negative test stop........");
  }

  @Test
  public void e_addExperimentTags() {
    LOGGER.info("Add Experiment tags test start................................");

    // Add Tags
    try {
      // start off with basic test of distinct tags
      List<String> tags = new ArrayList<>();
      tags.add("Test Update tag ");
      tags.add("Test Update tag 2 ");

      AddExperimentTags updateExperimentTags =
          AddExperimentTags.newBuilder().setId(experiment.getId()).addAllTags(tags).build();

      AddExperimentTags.Response aet_response =
          experimentServiceStub.addExperimentTags(updateExperimentTags);
      // there are already 2 tags created by utility functions
      LOGGER.info("AddExperimentTags Response : " + aet_response.getExperiment());
      assertEquals(
          experiment.getTagsCount() + tags.size(), aet_response.getExperiment().getTagsCount());

      assertNotEquals(
          "Experiment date_updated field not update on database",
          experiment.getDateUpdated(),
          aet_response.getExperiment().getDateUpdated());
      experiment = aet_response.getExperiment();

      // test when some tags are repeated, expectation is only the new tags are added
      tags = new ArrayList<>();
      tags.add("Test Update tag 3");
      tags.add("Test Update tag 2 ");

      updateExperimentTags =
          AddExperimentTags.newBuilder().setId(experiment.getId()).addAllTags(tags).build();

      aet_response = experimentServiceStub.addExperimentTags(updateExperimentTags);
      LOGGER.info("AddExperimentTags Response : " + aet_response.getExperiment());
      assertEquals(experiment.getTagsCount() + 1, aet_response.getExperiment().getTagsCount());
      experiment = aet_response.getExperiment();

      // test when all tags are repeated, there should be no change in tags
      tags = new ArrayList<>();
      tags.add("Test Update tag 3");
      tags.add("Test Update tag 2 ");

      updateExperimentTags =
          AddExperimentTags.newBuilder().setId(experiment.getId()).addAllTags(tags).build();
      aet_response = experimentServiceStub.addExperimentTags(updateExperimentTags);
      LOGGER.info("AddExperimentTags Response : " + aet_response.getExperiment());
      assertEquals(experiment.getTagsCount(), aet_response.getExperiment().getTagsCount());

      try {
        String tag52 = "Human Activity Recognition using Smartphone Dataset";
        updateExperimentTags = updateExperimentTags.toBuilder().addTags(tag52).build();
        experimentServiceStub.addExperimentTags(updateExperimentTags);
        fail();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        LOGGER.warn(
            "Error Code : " + status.getCode() + " Description : " + status.getDescription());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      }

    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      fail();
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
    }
    LOGGER.info("Add Experiment tags test stop................................");
  }

  @Test
  public void ea_addExperimentTagsNegativeTest() {
    LOGGER.info("Add Experiment tags Negative test start................................");

    List<String> tags = new ArrayList<>();
    tags.add("Test Update tag " + Calendar.getInstance().getTimeInMillis());
    tags.add("Test Update tag 2 " + Calendar.getInstance().getTimeInMillis());

    AddExperimentTags updateExperimentTags =
        AddExperimentTags.newBuilder().addAllTags(tags).build();

    try {
      experimentServiceStub.addExperimentTags(updateExperimentTags);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add Experiment tags Negative test stop................................");
  }

  @Test
  public void eb_addExperimentTag() {
    LOGGER.info("Add Experiment tag test start................................");

    // Add Tag
    AddExperimentTag updateExperimentTag =
        AddExperimentTag.newBuilder().setId(experiment.getId()).setTag("tag_abc").build();

    AddExperimentTag.Response aet_response =
        experimentServiceStub.addExperimentTag(updateExperimentTag);
    // there are already 2 tags created by utility functions
    LOGGER.info("AddExperimentTag Response : " + aet_response.getExperiment());
    assertEquals(
        "Experiment tags not match with expected experiment tags",
        experiment.getTagsCount() + 1,
        aet_response.getExperiment().getTagsCount());
    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        aet_response.getExperiment().getDateUpdated());
    experiment = aet_response.getExperiment();

    try {
      String tag52 = "Human Activity Recognition using Smartphone Dataset";
      updateExperimentTag = updateExperimentTag.toBuilder().setTag(tag52).build();
      experimentServiceStub.addExperimentTag(updateExperimentTag);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add Experiment tags test stop................................");
  }

  @Test
  public void ec_addExperimentTagNegativeTest() {
    LOGGER.info("Add Experiment tag negative test start................................");

    AddExperimentTag updateExperimentTag = AddExperimentTag.newBuilder().setTag("Tag_xyz").build();

    try {
      experimentServiceStub.addExperimentTag(updateExperimentTag);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add Experiment tags negative test stop................................");
  }

  @Test
  public void ee_getExperimentTags() {
    LOGGER.info("Get Experiment tags test start................................");

    GetTags getExperimentTags = GetTags.newBuilder().setId(experiment.getId()).build();
    GetTags.Response response = experimentServiceStub.getExperimentTags(getExperimentTags);
    LOGGER.info("GetExperimentTags Response : " + response.getTagsList());
    assertEquals(
        "Tags not match with expected tags", experiment.getTagsList(), response.getTagsList());

    LOGGER.info("Get Experiment tags test stop................................");
  }

  @Test
  public void ee_getExperimentTagsNegativeTest() {
    LOGGER.info("Get Experiment tags Negative test start................................");

    GetTags getExperimentTags = GetTags.newBuilder().build();
    try {
      experimentServiceStub.getExperimentTags(getExperimentTags);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Get Experiment tags Negative test stop................................");
  }

  @Test
  public void f_deleteExperimentTags() {
    LOGGER.info("Delete Experiment tags test start................................");

    e_addExperimentTags();
    List<String> removableTags = experiment.getTagsList();
    if (experiment.getTagsList().size() > 1) {
      removableTags = experiment.getTagsList().subList(0, experiment.getTagsList().size() - 1);
    }

    DeleteExperimentTags deleteExperimentTags =
        DeleteExperimentTags.newBuilder()
            .setId(experiment.getId())
            .addAllTags(removableTags)
            .build();

    DeleteExperimentTags.Response response =
        experimentServiceStub.deleteExperimentTags(deleteExperimentTags);
    LOGGER.info("DeleteExperimentTags Response : " + response.getExperiment().getTagsList());
    assertTrue(response.getExperiment().getTagsList().size() <= 1);

    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    if (response.getExperiment().getTagsList().size() > 0) {
      deleteExperimentTags =
          DeleteExperimentTags.newBuilder().setId(experiment.getId()).setDeleteAll(true).build();

      response = experimentServiceStub.deleteExperimentTags(deleteExperimentTags);
      LOGGER.info("DeleteExperimentTags Response : " + response.getExperiment().getTagsList());
      assertEquals(0, response.getExperiment().getTagsList().size());

      assertNotEquals(
          "Experiment date_updated field not update on database",
          experiment.getDateUpdated(),
          response.getExperiment().getDateUpdated());
    }

    LOGGER.info("Delete Experiment tags test stop................................");
  }

  @Test
  public void fa_deleteExperimentTagsNegativeTest() {
    LOGGER.info("Delete Experiment tags Negative test start................................");

    DeleteExperimentTags deleteExperimentTags = DeleteExperimentTags.newBuilder().build();
    try {
      experimentServiceStub.deleteExperimentTags(deleteExperimentTags);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Delete Experiment tags Negative test stop................................");
  }

  @Test
  public void fb_deleteExperimentTag() {
    LOGGER.info("Delete Experiment tag test start................................");

    e_addExperimentTags();
    // Delete Tag
    DeleteExperimentTag deleteExperimentTag =
        DeleteExperimentTag.newBuilder()
            .setId(experiment.getId())
            .setTag(experiment.getTagsList().get(0))
            .build();

    DeleteExperimentTag.Response response =
        experimentServiceStub.deleteExperimentTag(deleteExperimentTag);
    LOGGER.info("DeleteExperimentTag Response : " + response.getExperiment().getTagsList());
    assertEquals(
        "Experiment tags not match with expected experiment tags",
        experiment.getTagsCount() - 1,
        response.getExperiment().getTagsCount());

    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    LOGGER.info("Delete Experiment tag test stop................................");
  }

  @Test
  public void fc_deleteExperimentTagNegativeTest() {
    LOGGER.info("Delete Experiment tag negative test start................................");

    DeleteExperimentTag deleteExperimentTag = DeleteExperimentTag.newBuilder().build();
    try {
      experimentServiceStub.deleteExperimentTag(deleteExperimentTag);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Delete Experiment tag negative test stop................................");
  }

  @Test
  public void g_addAttribute() {
    LOGGER.info("Add Experiment attribute test start................................");

    Value stringValue =
        Value.newBuilder()
            .setStringValue("Attributes_Value_add_" + Calendar.getInstance().getTimeInMillis())
            .build();
    KeyValue keyValue =
        KeyValue.newBuilder()
            .setKey("Attributes_add " + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build();

    AddAttributes addAttributesRequest =
        AddAttributes.newBuilder().setId(experiment.getId()).setAttribute(keyValue).build();

    try {
      AddAttributes.Response response = experimentServiceStub.addAttribute(addAttributesRequest);
      LOGGER.info("AddAttributes Response : " + response.getStatus());
      assertTrue(response.getStatus());

      GetExperimentById getExperimentById =
          GetExperimentById.newBuilder().setId(experiment.getId()).build();
      GetExperimentById.Response getExperimentByIdResponse =
          experimentServiceStub.getExperimentById(getExperimentById);
      assertNotEquals(
          "Experiment date_updated field not update on database",
          experiment.getDateUpdated(),
          getExperimentByIdResponse.getExperiment().getDateUpdated());
      experiment = getExperimentByIdResponse.getExperiment();

    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      fail();
    }

    LOGGER.info("Add Experiment attribute test stop................................");
  }

  @Test
  public void g_addAttributeNegativeTest() {
    LOGGER.info("Add Experiment attribute Negative test start................................");

    Value stringValue =
        Value.newBuilder()
            .setStringValue("Attributes_Value_add_" + Calendar.getInstance().getTimeInMillis())
            .build();
    KeyValue keyValue =
        KeyValue.newBuilder()
            .setKey("Attributes_add " + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.STRING)
            .build();

    AddAttributes addAttributesRequest = AddAttributes.newBuilder().setAttribute(keyValue).build();

    try {
      experimentServiceStub.addAttribute(addAttributesRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    addAttributesRequest =
        AddAttributes.newBuilder().setId("dhfjkdshfd").setAttribute(keyValue).build();

    try {
      experimentServiceStub.addAttribute(addAttributesRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    LOGGER.info("Add Experiment attribute Negative test stop................................");
  }

  @Test
  public void gg_addExperimentAttributes() {
    LOGGER.info("Add Experiment attributes test start................................");

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

    AddExperimentAttributes addAttributesRequest =
        AddExperimentAttributes.newBuilder()
            .setId(experiment.getId())
            .addAllAttributes(attributeList)
            .build();

    AddExperimentAttributes.Response response =
        experimentServiceStub.addExperimentAttributes(addAttributesRequest);
    LOGGER.info("AddExperimentAttributes Response : " + response.getExperiment());
    assertTrue(response.getExperiment().getAttributesList().containsAll(attributeList));

    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    LOGGER.info("Add Experiment attributes test stop................................");
  }

  @Test
  public void gg_addExperimentAttributesNegativeTest() {
    LOGGER.info("Add Experiment attributes Negative test start................................");

    List<KeyValue> attributeList = new ArrayList<>();
    Value intValue = Value.newBuilder().setNumberValue(1.1).build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_" + Calendar.getInstance().getTimeInMillis())
            .setValue(intValue)
            .setValueType(ValueType.NUMBER)
            .build());
    Value stringValue =
        Value.newBuilder()
            .setStringValue("attributes_value_" + Calendar.getInstance().getTimeInMillis())
            .build();
    attributeList.add(
        KeyValue.newBuilder()
            .setKey("attribute_" + Calendar.getInstance().getTimeInMillis())
            .setValue(stringValue)
            .setValueType(ValueType.BLOB)
            .build());

    AddExperimentAttributes addAttributesRequest =
        AddExperimentAttributes.newBuilder().addAllAttributes(attributeList).build();

    try {
      experimentServiceStub.addExperimentAttributes(addAttributesRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    addAttributesRequest = AddExperimentAttributes.newBuilder().setId(experiment.getId()).build();
    try {
      experimentServiceStub.addExperimentAttributes(addAttributesRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Add Experiment attributes Negative test stop................................");
  }

  @Test
  public void h_getExperimentAttributes() {
    LOGGER.info("Get Experiment attributes test start................................");

    gg_addExperimentAttributes();
    List<KeyValue> attributes = experiment.getAttributesList();
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
        GetAttributes.newBuilder().setId(experiment.getId()).addAllAttributeKeys(keys).build();

    GetAttributes.Response response =
        experimentServiceStub.getExperimentAttributes(getAttributesRequest);
    assertEquals(keys.size(), response.getAttributesList().size());
    LOGGER.info("getExperimentAttributes Response : " + response.getAttributesList());

    getAttributesRequest =
        GetAttributes.newBuilder().setId(experiment.getId()).setGetAll(true).build();

    response = experimentServiceStub.getExperimentAttributes(getAttributesRequest);
    assertEquals(attributes.size(), response.getAttributesList().size());
    LOGGER.info("getExperimentAttributes Response : " + response.getAttributesList());

    LOGGER.info("Get Experiment attributes test stop................................");
  }

  @Test
  public void h_getExperimentAttributesNegativeTest() {
    LOGGER.info("Get Experiment attribute Negative test start................................");

    GetAttributes getAttributesRequest = GetAttributes.newBuilder().build();

    try {
      experimentServiceStub.getExperimentAttributes(getAttributesRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getAttributesRequest = GetAttributes.newBuilder().setId("dsfdsfdsfds").setGetAll(true).build();

    try {
      experimentServiceStub.getExperimentAttributes(getAttributesRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    LOGGER.info("Get Experiment attribute Negative test stop................................");
  }

  @Test
  public void hh_deleteExperimentAttributes() {
    LOGGER.info("Delete Experiment Attributes test start................................");

    gg_addExperimentAttributes();
    List<KeyValue> attributes = experiment.getAttributesList();
    LOGGER.info("Attributes size : " + attributes.size());

    if (attributes.size() == 0) {
      LOGGER.warn("Experiment Attributes not found in database ");
      fail();
      return;
    }

    List<String> keys = new ArrayList<>();
    for (int index = 0; index < attributes.size() - 1; index++) {
      KeyValue keyValue = attributes.get(index);
      keys.add(keyValue.getKey());
    }
    LOGGER.info("Attributes key size : " + keys.size());

    DeleteExperimentAttributes deleteExperimentAttributes =
        DeleteExperimentAttributes.newBuilder()
            .setId(experiment.getId())
            .addAllAttributeKeys(keys)
            .build();

    DeleteExperimentAttributes.Response response =
        experimentServiceStub.deleteExperimentAttributes(deleteExperimentAttributes);
    LOGGER.info("DeleteExperimentAttributes Response : " + response.getExperiment());
    assertTrue(response.getExperiment().getAttributesList().size() <= 1);

    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    if (response.getExperiment().getAttributesList().size() != 0) {
      deleteExperimentAttributes =
          DeleteExperimentAttributes.newBuilder()
              .setId(experiment.getId())
              .setDeleteAll(true)
              .build();

      response = experimentServiceStub.deleteExperimentAttributes(deleteExperimentAttributes);
      LOGGER.info("DeleteExperimentAttributes Response : " + response.getExperiment());
      assertEquals(0, response.getExperiment().getAttributesList().size());

      assertNotEquals(
          "Experiment date_updated field not update on database",
          experiment.getDateUpdated(),
          response.getExperiment().getDateUpdated());
      experiment = response.getExperiment();
    }

    LOGGER.info("Delete Experiment Attributes test stop................................");
  }

  @Test
  public void hh_deleteExperimentAttributesNegativeTest() {
    LOGGER.info("Delete Experiment Attributes Negative test start................................");

    DeleteExperimentAttributes deleteExperimentAttributes =
        DeleteExperimentAttributes.newBuilder().build();

    try {
      experimentServiceStub.deleteExperimentAttributes(deleteExperimentAttributes);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    deleteExperimentAttributes =
        DeleteExperimentAttributes.newBuilder()
            .setId(experiment.getId())
            .setDeleteAll(true)
            .build();

    DeleteExperimentAttributes.Response response =
        experimentServiceStub.deleteExperimentAttributes(deleteExperimentAttributes);
    LOGGER.info("DeleteExperimentAttributes Response : " + response.getExperiment());
    assertEquals(0, response.getExperiment().getAttributesList().size());

    LOGGER.info("Delete Experiment Attributes Negative test stop................................");
  }

  @Test
  public void i_getExperimentByName() {
    LOGGER.info("Get Experiment by name test start................................");

    GetExperimentByName getExperimentRequest =
        GetExperimentByName.newBuilder()
            .setProjectId(experiment.getProjectId())
            .setName(experiment.getName())
            .build();

    GetExperimentByName.Response getExperimentResponse =
        experimentServiceStub.getExperimentByName(getExperimentRequest);
    LOGGER.info("GetExperimentByName Response : \n" + getExperimentResponse.getExperiment());
    assertEquals(
        "Experiment name not match with expected experiment name",
        experiment.getName(),
        getExperimentResponse.getExperiment().getName());
    experiment = getExperimentResponse.getExperiment();

    LOGGER.info("Get Experiment by name of project test stop................................");
  }

  @Test
  public void i_getExperimentByNameNegativeTest() {
    LOGGER.info("Get Experiment by name Negative test start................................");

    GetExperimentByName experimentRequest = GetExperimentByName.newBuilder().build();

    try {
      experimentServiceStub.getExperimentByName(experimentRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    experimentRequest = GetExperimentByName.newBuilder().setName(experiment.getName()).build();
    try {
      experimentServiceStub.getExperimentByName(experimentRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Get Experiment by name Negative test stop................................");
  }

  @Test
  public void z_deleteExperimentNegativeTest() {
    LOGGER.info("Delete Experiment Negative test start................................");

    DeleteExperiment deleteExperimentRequest = DeleteExperiment.newBuilder().build();

    try {
      experimentServiceStub.deleteExperiment(deleteExperimentRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("Delete Experiment Negative test stop................................");
  }

  @Test
  public void logExperimentCodeVersionTest() {
    LOGGER.info("Log Experiment code version test start................................");

    LogExperimentCodeVersion logExperimentCodeVersionRequest =
        LogExperimentCodeVersion.newBuilder()
            .setId(experiment.getId())
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
    LogExperimentCodeVersion.Response logExperimentCodeVersionResponse =
        experimentServiceStub.logExperimentCodeVersion(logExperimentCodeVersionRequest);
    CodeVersion codeVersion =
        logExperimentCodeVersionResponse.getExperiment().getCodeVersionSnapshot();
    assertEquals(
        "Experiment codeVersion not match with expected experiment codeVersion",
        logExperimentCodeVersionRequest.getCodeVersion(),
        codeVersion);
    experiment = logExperimentCodeVersionResponse.getExperiment();

    try {
      logExperimentCodeVersionRequest =
          LogExperimentCodeVersion.newBuilder()
              .setId(experiment.getId())
              .setCodeVersion(
                  CodeVersion.newBuilder()
                      .setCodeArchive(
                          Artifact.newBuilder()
                              .setKey("code_version_image")
                              .setPath("https://xyz_path_string.com/image.png")
                              .setArtifactType(ArtifactType.CODE)
                              .build())
                      .build())
              .build();
      experimentServiceStub.logExperimentCodeVersion(logExperimentCodeVersionRequest);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info("Log Experiment code version test stop................................");
  }

  @Test
  public void getExperimentCodeVersionTest() {
    LOGGER.info("Get Experiment code version test start................................");

    LogExperimentCodeVersion logExperimentCodeVersionRequest =
        LogExperimentCodeVersion.newBuilder()
            .setId(experiment.getId())
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
    LogExperimentCodeVersion.Response logExperimentCodeVersionResponse =
        experimentServiceStub.logExperimentCodeVersion(logExperimentCodeVersionRequest);
    CodeVersion codeVersion =
        logExperimentCodeVersionResponse.getExperiment().getCodeVersionSnapshot();
    assertEquals(
        "Experiment codeVersion not match with expected experiment codeVersion",
        logExperimentCodeVersionRequest.getCodeVersion(),
        codeVersion);
    experiment = logExperimentCodeVersionResponse.getExperiment();

    GetExperimentCodeVersion getExperimentCodeVersionRequest =
        GetExperimentCodeVersion.newBuilder().setId(experiment.getId()).build();
    GetExperimentCodeVersion.Response getExperimentCodeVersionResponse =
        experimentServiceStub.getExperimentCodeVersion(getExperimentCodeVersionRequest);
    assertEquals(
        "Experiment codeVersion not match with expected experiment codeVersion",
        codeVersion,
        getExperimentCodeVersionResponse.getCodeVersion());

    LOGGER.info("Get Experiment code version test stop................................");
  }

  @Test
  public void logArtifactsTest() {
    LOGGER.info(" Log Artifacts in Experiment test start................................");

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

    LogExperimentArtifacts logArtifactRequest =
        LogExperimentArtifacts.newBuilder()
            .setId(experiment.getId())
            .addAllArtifacts(artifacts)
            .build();

    LogExperimentArtifacts.Response response =
        experimentServiceStub.logArtifacts(logArtifactRequest);

    LOGGER.info("LogArtifact Response : \n" + response.getExperiment());
    assertEquals(
        "Experiment artifacts not match with expected artifacts",
        (experiment.getArtifactsCount() + artifacts.size()),
        response.getExperiment().getArtifactsList().size());

    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    GetExperimentById getExperimentById =
        GetExperimentById.newBuilder().setId(experiment.getId()).build();
    GetExperimentById.Response getExperimentByIdResponse =
        experimentServiceStub.getExperimentById(getExperimentById);
    assertEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        getExperimentByIdResponse.getExperiment().getDateUpdated());

    LOGGER.info("Log Artifacts in Experiment tags test stop................................");
  }

  @Test
  public void m_logArtifactsNegativeTest() {
    LOGGER.info(" Log Artifacts in Experiment Negative test start................................");

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

    LogExperimentArtifacts logArtifactRequest =
        LogExperimentArtifacts.newBuilder().addAllArtifacts(artifacts).build();
    try {
      experimentServiceStub.logArtifacts(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    logArtifactRequest =
        LogExperimentArtifacts.newBuilder().setId("asda").addAllArtifacts(artifacts).build();
    try {
      experimentServiceStub.logArtifacts(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    logArtifactRequest =
        LogExperimentArtifacts.newBuilder()
            .setId(experiment.getId())
            .addAllArtifacts(experiment.getArtifactsList())
            .build();
    try {
      experimentServiceStub.logArtifacts(logArtifactRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.ALREADY_EXISTS.getCode(), status.getCode());
    }

    LOGGER.info(
        "Log Artifacts in Experiment tags Negative test stop................................");
  }

  @Test
  public void getArtifactsTest() {
    LOGGER.info("Get Artifacts from Experiment test start................................");

    GetArtifacts getArtifactsRequest = GetArtifacts.newBuilder().setId(experiment.getId()).build();

    GetArtifacts.Response response = experimentServiceStub.getArtifacts(getArtifactsRequest);

    LOGGER.info("GetArtifacts Response : " + response.getArtifactsCount());
    assertEquals(
        "Experiment artifacts not matched with expected artifacts",
        experiment.getArtifactsList(),
        response.getArtifactsList());

    LOGGER.info("Get Artifacts from Experiment tags test stop................................");
  }

  @Test
  public void n_getArtifactsNegativeTest() {
    LOGGER.info(
        "Get Artifacts from Experiment Negative test start................................");

    GetArtifacts getArtifactsRequest = GetArtifacts.newBuilder().build();

    try {
      experimentServiceStub.getArtifacts(getArtifactsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    getArtifactsRequest = GetArtifacts.newBuilder().setId("dssaa").build();

    try {
      experimentServiceStub.getArtifacts(getArtifactsRequest);
      fail();
    } catch (StatusRuntimeException ex) {
      Status status = Status.fromThrowable(ex);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    LOGGER.info(
        "Get Artifacts from Experiment Negative tags test stop................................");
  }

  @Test
  public void deleteExperimentArtifacts() {
    LOGGER.info("Delete Experiment Artifacts test start................................");

    logArtifactsTest();
    List<Artifact> artifacts = experiment.getArtifactsList();
    LOGGER.info("Artifacts size : " + artifacts.size());
    if (artifacts.isEmpty()) {
      fail("Artifacts not found");
    }

    DeleteExperimentArtifact request =
        DeleteExperimentArtifact.newBuilder()
            .setId(experiment.getId())
            .setKey(artifacts.get(0).getKey())
            .build();

    DeleteExperimentArtifact.Response response = experimentServiceStub.deleteArtifact(request);
    LOGGER.info(
        "DeleteExperimentArtifacts Response : \n" + response.getExperiment().getArtifactsList());
    assertFalse(response.getExperiment().getArtifactsList().contains(artifacts.get(0)));

    assertNotEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        response.getExperiment().getDateUpdated());
    experiment = response.getExperiment();

    GetExperimentById getExperimentById =
        GetExperimentById.newBuilder().setId(experiment.getId()).build();
    GetExperimentById.Response getExperimentByIdResponse =
        experimentServiceStub.getExperimentById(getExperimentById);
    assertEquals(
        "Experiment date_updated field not update on database",
        experiment.getDateUpdated(),
        getExperimentByIdResponse.getExperiment().getDateUpdated());
    experiment = getExperimentByIdResponse.getExperiment();

    LOGGER.info("Delete Experiment Artifacts test stop................................");
  }

  @Test
  public void deleteExperiments() {
    LOGGER.info("Batch Delete Experiment test start................................");

    ExperimentRunTest experimentRunTest = new ExperimentRunTest();
    // Create project
    ProjectTest projectTest = new ProjectTest();
    CreateProject createProjectRequest =
        projectTest.getCreateProjectRequest("project-" + new Date().getTime());
    CreateProject.Response createProjectResponse =
        projectServiceStub.createProject(createProjectRequest);
    Project project = createProjectResponse.getProject();
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected project name",
        createProjectRequest.getName(),
        project.getName());

    List<String> experimentIds = new ArrayList<>();
    List<ExperimentRun> experimentRunList = new ArrayList<>();
    for (int count = 0; count < 5; count++) {
      // Create experiment of above project
      CreateExperiment createExperimentRequest =
          getCreateExperimentRequest(
              project.getId(), "Experiment-" + new Date().getTime() + "-" + count);
      CreateExperiment.Response createExperimentResponse =
          experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment = createExperimentResponse.getExperiment();
      experimentIds.add(experiment.getId());
      LOGGER.info("Experiment created successfully");
      assertEquals(
          "Experiment name not match with expected Experiment name",
          createExperimentRequest.getName(),
          createExperimentResponse.getExperiment().getName());

      CreateExperimentRun createExperimentRunRequest =
          experimentRunTest.getCreateExperimentRunRequest(
              project.getId(),
              experiment.getId(),
              "ExperimentRun-" + new Date().getTime() + "-" + count);
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun1 = createExperimentRunResponse.getExperimentRun();
      experimentRunList.add(experimentRun1);
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun1.getName());

      createExperimentRunRequest =
          experimentRunTest.getCreateExperimentRunRequest(
              project.getId(), experiment.getId(), "ExperimentRun_sprt_2_" + count);
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun2 = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun2.getName());

      // Create comment for above experimentRun1 & experimentRun3
      // comment for experiment1
      AddComment addCommentRequest =
          AddComment.newBuilder()
              .setEntityId(experimentRun1.getId())
              .setMessage(
                  "Hello, this project is intreasting." + Calendar.getInstance().getTimeInMillis())
              .build();
      commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
      LOGGER.info("\n Comment added successfully for ExperimentRun1 \n");
      // comment for experimentRun3
      addCommentRequest =
          AddComment.newBuilder()
              .setEntityId(experimentRun1.getId())
              .setMessage(
                  "Hello, this project is intreasting." + Calendar.getInstance().getTimeInMillis())
              .build();
      commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
      LOGGER.info("\n Comment added successfully for ExperimentRun3 \n");
    }

    DeleteExperiments deleteExperimentsRequest =
        DeleteExperiments.newBuilder().addAllIds(experimentIds).build();

    DeleteExperiments.Response response =
        experimentServiceStub.deleteExperiments(deleteExperimentsRequest);
    LOGGER.info("DeleteExperiment Response : " + response.getStatus());
    assertTrue(response.getStatus());

    // Delete entities by cron job
    deleteEntitiesCron.run();
    deleteEntitiesCron.run();

    for (String experimentId : experimentIds) {

      // Start cross-checking for experiment
      try {
        GetExperimentById getExperiment =
            GetExperimentById.newBuilder().setId(experimentId).build();
        experimentServiceStub.getExperimentById(getExperiment);
        fail();
      } catch (StatusRuntimeException ex) {
        Status status = Status.fromThrowable(ex);
        LOGGER.info("Error Code : " + status.getCode() + " Error : " + status.getDescription());
        assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
      }

      // Start cross-checking for experimentRun
      try {
        GetExperimentRunsInExperiment getExperimentRuns =
            GetExperimentRunsInExperiment.newBuilder().setExperimentId(experimentId).build();
        experimentRunServiceStub.getExperimentRunsInExperiment(getExperimentRuns);
        fail();
      } catch (StatusRuntimeException e) {
        Status status = Status.fromThrowable(e);
        LOGGER.info("Error Code : " + status.getCode() + " Error : " + status.getDescription());
        assertEquals(Status.NOT_FOUND.getCode(), status.getCode());
      }
    }

    // Start cross-checking for comment of experimentRun
    // For experimentRun1
    GetComments getCommentsRequest =
        GetComments.newBuilder().setEntityId(experimentRunList.get(0).getId()).build();
    GetComments.Response getCommentsResponse;
    try {
      getCommentsResponse = commentServiceBlockingStub.getExperimentRunComments(getCommentsRequest);
      assertTrue(getCommentsResponse.getCommentsList().isEmpty());
    } catch (StatusRuntimeException e) {
      checkEqualsAssert(e);
    }
    // For experimentRun3
    getCommentsRequest =
        GetComments.newBuilder().setEntityId(experimentRunList.get(0).getId()).build();
    try {
      getCommentsResponse = commentServiceBlockingStub.getExperimentRunComments(getCommentsRequest);
      assertTrue(getCommentsResponse.getCommentsList().isEmpty());
    } catch (StatusRuntimeException e) {
      checkEqualsAssert(e);
    }

    // Delete Project
    DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
    DeleteProject.Response deleteProjectResponse = projectServiceStub.deleteProject(deleteProject);
    LOGGER.info("Project deleted successfully");
    LOGGER.info(deleteProjectResponse.toString());
    assertTrue(deleteProjectResponse.getStatus());

    LOGGER.info("Batch Delete Experiment test stop................................");
  }
}
