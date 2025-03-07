package ai.verta.modeldb;

import static org.junit.Assert.*;

import ai.verta.common.KeyValue;
import ai.verta.common.KeyValueQuery;
import ai.verta.common.OperatorEnum;
import ai.verta.uac.GetUser;
import ai.verta.uac.ResourceVisibility;
import ai.verta.uac.UserInfo;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FindProjectEntitiesTest extends TestsInit {

  private static final Logger LOGGER = LogManager.getLogger(FindProjectEntitiesTest.class);

  // Project Entities
  private static Project project1;
  private static Project project2;
  private static Project project3;
  private static Project project4;
  private static Map<String, Project> projectMap = new HashMap<>();

  // Experiment Entities
  private static Experiment experiment1;
  private static Experiment experiment2;
  private static Experiment experiment3;
  private static Experiment experiment4;
  private static Map<String, Experiment> experimentMap = new HashMap<>();

  // ExperimentRun Entities
  private static ExperimentRun experimentRun11;
  private static ExperimentRun experimentRun12;
  private static ExperimentRun experimentRun21;
  private static ExperimentRun experimentRun22;
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
    for (String projectId : projectMap.keySet()) {
      DeleteProject deleteProject = DeleteProject.newBuilder().setId(projectId).build();
      DeleteProject.Response deleteProjectResponse =
          projectServiceStub.deleteProject(deleteProject);
      LOGGER.info("Project deleted successfully");
      LOGGER.info(deleteProjectResponse.toString());
      assertTrue(deleteProjectResponse.getStatus());
    }
    projectMap = new HashMap<>();
    experimentMap = new HashMap<>();
    experimentRunMap = new HashMap<>();
  }

  private static void createProjectEntities() {
    ProjectTest projectTest = new ProjectTest();

    // Create two project of above project
    CreateProject createProjectRequest =
        projectTest.getCreateProjectRequest("Project-1-" + new Date().getTime());
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
    createProjectRequest =
        createProjectRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_2")
            .build();
    CreateProject.Response createProjectResponse =
        projectServiceStub.createProject(createProjectRequest);
    project1 = createProjectResponse.getProject();
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected Project name",
        createProjectRequest.getName(),
        project1.getName());

    // project2 of above project
    createProjectRequest = projectTest.getCreateProjectRequest("Project-2-" + new Date().getTime());
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
    createProjectRequest =
        createProjectRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_3")
            .addTags("Tag_4")
            .build();
    createProjectResponse = projectServiceStub.createProject(createProjectRequest);
    project2 = createProjectResponse.getProject();
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected Project name",
        createProjectRequest.getName(),
        project2.getName());

    // project3 of above project
    createProjectRequest = projectTest.getCreateProjectRequest("Project-3-" + new Date().getTime());
    attribute1 =
        KeyValue.newBuilder()
            .setKey("attribute_1")
            .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
            .build();
    attribute2 =
        KeyValue.newBuilder()
            .setKey("attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
            .build();
    createProjectRequest =
        createProjectRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_5")
            .addTags("Tag_6")
            .build();
    createProjectResponse = projectServiceStub.createProject(createProjectRequest);
    project3 = createProjectResponse.getProject();
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected Project name",
        createProjectRequest.getName(),
        project3.getName());

    // project4 of above project
    createProjectRequest = projectTest.getCreateProjectRequest("Project-4-" + new Date().getTime());
    attribute1 =
        KeyValue.newBuilder()
            .setKey("attribute_1")
            .setValue(Value.newBuilder().setNumberValue(1.00).build())
            .build();
    attribute2 =
        KeyValue.newBuilder()
            .setKey("attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.001212).build())
            .build();
    KeyValue attribute3 =
        KeyValue.newBuilder()
            .setKey("attribute_3")
            .setValue(Value.newBuilder().setNumberValue(1.1).build())
            .build();
    createProjectRequest =
        createProjectRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addAttributes(attribute3)
            .addTags("Tag_5")
            .addTags("Tag_7")
            .addTags("Tag_8")
            .setVisibility(ResourceVisibility.PRIVATE)
            .build();
    createProjectResponse = projectServiceStub.createProject(createProjectRequest);
    project4 = createProjectResponse.getProject();
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected Project name",
        createProjectRequest.getName(),
        project4.getName());

    projectMap.put(project1.getId(), project1);
    projectMap.put(project2.getId(), project2);
    projectMap.put(project3.getId(), project3);
    projectMap.put(project4.getId(), project4);
  }

  private static void createExperimentEntities() {
    // Create two experiment of above project
    CreateExperiment createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project1.getId(), "Experiment-1-" + new Date().getTime());
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
    experiment1 = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment1.getName());

    // experiment2 of above project
    createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project1.getId(), "Experiment-2-" + new Date().getTime());
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
    createExperimentRequest =
        createExperimentRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_3")
            .addTags("Tag_4")
            .build();
    createExperimentResponse = experimentServiceStub.createExperiment(createExperimentRequest);
    experiment2 = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment2.getName());

    // experiment3 of above project
    createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project1.getId(), "Experiment-3-" + new Date().getTime());
    attribute1 =
        KeyValue.newBuilder()
            .setKey("attribute_1")
            .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
            .build();
    attribute2 =
        KeyValue.newBuilder()
            .setKey("attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.6543210).build())
            .build();
    createExperimentRequest =
        createExperimentRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_1")
            .addTags("Tag_5")
            .addTags("Tag_6")
            .build();
    createExperimentResponse = experimentServiceStub.createExperiment(createExperimentRequest);
    experiment3 = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment3.getName());

    // experiment4 of above project
    createExperimentRequest =
        ExperimentTest.getCreateExperimentRequest(
            project1.getId(), "Experiment-4-" + new Date().getTime());
    attribute1 =
        KeyValue.newBuilder()
            .setKey("attribute_1")
            .setValue(Value.newBuilder().setNumberValue(1.00).build())
            .build();
    attribute2 =
        KeyValue.newBuilder()
            .setKey("attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.001212).build())
            .build();
    createExperimentRequest =
        createExperimentRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_5")
            .addTags("Tag_7")
            .addTags("Tag_8")
            .build();
    createExperimentResponse = experimentServiceStub.createExperiment(createExperimentRequest);
    experiment4 = createExperimentResponse.getExperiment();
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment4.getName());

    experimentMap.put(experiment1.getId(), experiment1);
    experimentMap.put(experiment2.getId(), experiment2);
    experimentMap.put(experiment3.getId(), experiment3);
    experimentMap.put(experiment4.getId(), experiment4);
  }

  private static void createExperimentRunEntities() {
    CreateExperimentRun createExperimentRunRequest =
        ExperimentRunTest.getCreateExperimentRunRequest(
            project1.getId(), experiment1.getId(), "ExperimentRun-1-" + new Date().getTime());
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
            .setCodeVersion("4.0")
            .addMetrics(metric1)
            .addMetrics(metric2)
            .addHyperparameters(hyperparameter1)
            .build();
    CreateExperimentRun.Response createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    experimentRun11 = createExperimentRunResponse.getExperimentRun();
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun11.getName());

    createExperimentRunRequest =
        ExperimentRunTest.getCreateExperimentRunRequest(
            project1.getId(), experiment1.getId(), "ExperimentRun-2-" + new Date().getTime());
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
            .setCodeVersion("3.0")
            .addMetrics(metric1)
            .addMetrics(metric2)
            .addHyperparameters(hyperparameter1)
            .build();
    createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    experimentRun12 = createExperimentRunResponse.getExperimentRun();
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun12.getName());

    createExperimentRunRequest =
        ExperimentRunTest.getCreateExperimentRunRequest(
            project1.getId(), experiment2.getId(), "ExperimentRun-3-" + new Date().getTime());
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
            .setCodeVersion("2.0")
            .addMetrics(metric1)
            .addMetrics(metric2)
            .addHyperparameters(hyperparameter1)
            .build();
    createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    experimentRun21 = createExperimentRunResponse.getExperimentRun();
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun21.getName());

    createExperimentRunRequest =
        ExperimentRunTest.getCreateExperimentRunRequest(
            project1.getId(), experiment2.getId(), "ExperimentRun-4-" + new Date().getTime());
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
            .setCodeVersion("1.0")
            .addMetrics(metric1)
            .addMetrics(metric2)
            .addHyperparameters(hyperparameter1)
            .addTags("test_tag_123")
            .addTags("test_tag_456")
            .build();
    createExperimentRunResponse =
        experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
    experimentRun22 = createExperimentRunResponse.getExperimentRun();
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun22.getName());

    experimentRunMap.put(experimentRun11.getId(), experimentRun11);
    experimentRunMap.put(experimentRun12.getId(), experimentRun12);
    experimentRunMap.put(experimentRun21.getId(), experimentRun21);
    experimentRunMap.put(experimentRun22.getId(), experimentRun22);
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

  /** Validation check for the predicate value with empty string which is not valid */
  @Test
  public void findProjectPredicateValueEmptyNegativeTest() {
    LOGGER.info(
        "FindProjects predicate value is empty negative test start................................");

    // Validate check for predicate value not empty
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value stringValueType = Value.newBuilder().setStringValue("").build();

    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(stringValueType)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addProjectIds(project1.getId())
            .addAllPredicates(predicates)
            // .setIdsOnly(true)
            .build();
    try {
      projectServiceStub.findProjects(findProjects);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    // If key is not set in predicate
    findProjects =
        FindProjects.newBuilder()
            .addProjectIds(project1.getId())
            .addPredicates(
                KeyValueQuery.newBuilder()
                    .setValue(Value.newBuilder().setNumberValue(11).build())
                    .build())
            .build();

    try {
      projectServiceStub.findProjects(findProjects);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "FindProjects predicate value is empty negative test stop ................................");
  }

  /** Validate check for protobuf struct type in KeyValueQuery not implemented */
  @Test
  public void findProjectStructTypeNotImplemented() {
    LOGGER.info(
        "check for protobuf struct type in KeyValueQuery not implemented test start................................");

    // Validate check for struct Type not implemented
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(17.1716586149719).build();

    Struct.Builder struct = Struct.newBuilder();
    struct.putFields("number_value", numValue);
    struct.build();
    Value structValue = Value.newBuilder().setStructValue(struct).build();

    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(structValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addProjectIds(project1.getId())
            .addAllPredicates(predicates)
            .build();

    try {
      projectServiceStub.findProjects(findProjects);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.UNIMPLEMENTED.getCode(), status.getCode());
    }

    LOGGER.info(
        "check for protobuf struct type in KeyValueQuery not implemented test stop................................");
  }

  /** find projects with value of attributes.attribute_1 <= 0.6543210 */
  @Test
  public void findProjectsByAttributesTest() {
    LOGGER.info("FindProjects by attributes test start................................");

    // get project with value of attributes.attribute_1 <= 0.6543210
    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindProjects findProjects = FindProjects.newBuilder().addPredicates(keyValueQuery).build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);
    List<Project> expectedProjects = new ArrayList<>();
    List<Project> staleProjects = new ArrayList<>();
    for (Project project : response.getProjectsList()) {
      if (projectMap.containsKey(project.getId())) {
        expectedProjects.add(project);
      } else {
        staleProjects.add(project);
      }
    }
    LOGGER.info("FindProjects Response : " + expectedProjects);
    assertEquals("Project count not match with expected project count", 3, expectedProjects.size());

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords() - staleProjects.size());

    for (Project fetchedProject : expectedProjects) {
      boolean doesAttributeExist = false;
      for (KeyValue fetchedAttribute : fetchedProject.getAttributesList()) {
        if (fetchedAttribute.getKey().equals("attribute_1")) {
          doesAttributeExist = true;
          assertTrue(
              "Project attributes.attribute_1 not match with expected project attributes.attribute_1",
              fetchedAttribute.getValue().getNumberValue() <= 0.6543210);
        }
      }
      if (!doesAttributeExist) {
        fail("Expected attribute not found in fetched attributes");
      }
    }

    findProjects =
        FindProjects.newBuilder()
            .setIdsOnly(false)
            .setAscending(true)
            .setSortKey("attributes.attribute_3")
            .build();

    response = projectServiceStub.findProjects(findProjects);
    expectedProjects = new ArrayList<>();
    staleProjects = new ArrayList<>();
    for (Project project : response.getProjectsList()) {
      if (projectMap.containsKey(project.getId())) {
        expectedProjects.add(project);
      } else {
        staleProjects.add(project);
      }
    }
    assertEquals(
        "Total records count not matched with expected records count",
        4,
        response.getTotalRecords() - staleProjects.size());
    assertEquals("Project count not match with expected project count", 4, expectedProjects.size());
    // TODO: ordering not consistent
    //    assertEquals(
    //        "Project Id not match with expected project Id",
    //        project4.getId(),
    //        response.getProjects(3).getId());
    //    assertTrue("Project Id not match with expected project Id",
    //    		project4.getId().equals(response.getProjects(3).getId())||
    //    		project4.getId().equals(response.getProjects(0).getId()));

    LOGGER.info("FindProjects by attributes test stop ................................");
  }

  /**
   * Find projects with value of attributes.attribute_1 <= 0.6543210 & attributes.attribute_2 ==
   * 0.31
   */
  @Test
  public void findProjectsByMultipleAttributeConditionTest() {
    LOGGER.info(
        "FindProjects by multiple attribute condition test start................................");

    // get project with value of attributes.attribute_1 <= 0.6543210 & attributes.attribute_2 ==
    // 0.31
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.31).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_2")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery2);

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addAllPredicates(predicates)
            .setIdsOnly(true)
            .build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);
    LOGGER.info("FindProjects Response : " + response.getProjectsCount());
    assertEquals(
        "Project count not match with expected project count", 1, response.getProjectsCount());
    assertEquals(
        "Project not match with expected project",
        project2.getId(),
        response.getProjectsList().get(0).getId());
    assertNotEquals(
        "Project not match with expected project", project2, response.getProjectsList().get(0));
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info(
        "FindProjects by multiple attribute condition test stop................................");
  }

  /** get projectRun with value of metrics.accuracy >= 0.6543210 & tags == Tag_7 */
  @Test
  public void findProjectsByMetricsAndTagsTest() {
    LOGGER.info(
        "FindProjects by and condition of metrics & tags test start................................");

    // get projectRun with value of metrics.accuracy >= 0.6543210 & tags == Tag_7
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value stringValue = Value.newBuilder().setStringValue("Tag_7").build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery);

    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.GTE)
            .build();
    predicates.add(keyValueQuery2);

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addAllPredicates(predicates)
            .build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);
    LOGGER.info("FindProjects Response : " + response.getProjectsCount());
    assertEquals(
        "Project count not match with expected project count", 1, response.getProjectsCount());
    assertEquals(
        "Project not match with expected project",
        project4.getId(),
        response.getProjectsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info(
        "FindProjects by and condition of metrics & tags test stop................................");
  }

  /** Find projects with value of endTime */
  @Test
  public void findProjectsByProjectEndTimeTest() {
    LOGGER.info("FindProjects by project endtime test start................................");

    // get project with value of endTime
    Value stringValue =
        Value.newBuilder().setStringValue(String.valueOf(project4.getDateCreated())).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.DATE_CREATED)
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addPredicates(keyValueQuery)
            .build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);
    LOGGER.info("FindProjects Response : " + response.getProjectsCount());
    assertEquals(
        "Project count not match with expected project count", 1, response.getProjectsCount());
    assertEquals(
        "ProjectRun not match with expected projectRun",
        project4.getId(),
        response.getProjectsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info("FindProjects by project endtime test stop................................");
  }

  /** Find projects by attributes with pagination */
  @Test
  public void findProjectsByAttributesWithPaginationTest() {
    LOGGER.info(
        "FindProjects by attributes with pagination test start................................");

    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    int pageLimit = 2;
    int count = 0;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      FindProjects findProjects =
          FindProjects.newBuilder()
              .addAllProjectIds(projectMap.keySet())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(true)
              .setSortKey("name")
              .build();

      FindProjects.Response response = projectServiceStub.findProjects(findProjects);

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (response.getProjectsList() != null && response.getProjectsList().size() > 0) {
        isExpectedResultFound = true;
        for (Project project : response.getProjectsList()) {
          assertEquals(
              "Project not match with expected project",
              projectMap.get(project.getId()).getId(),
              project.getId());

          if (count == 0) {
            assertEquals(
                "Project name not match with expected project name",
                project1.getName(),
                project.getName());
          } else if (count == 1) {
            assertEquals(
                "Project name not match with expected project name",
                project2.getName(),
                project.getName());
          } else if (count == 2) {
            assertEquals(
                "Project name not match with expected project name",
                project3.getName(),
                project.getName());
          }
          count++;
        }
      } else {
        if (isExpectedResultFound) {
          LOGGER.warn("More Project not found in database");
          assertTrue(true);
        } else {
          fail("Expected project not found in response");
        }
        break;
      }
    }

    LOGGER.info(
        "FindProjects by attributes with pagination test stop................................");
  }

  /** Check observations.attributes not support */
  @Test
  public void findProjectsNotSupportObservationsAttributesTest() {
    LOGGER.info(
        "FindProjects not support the observation.attributes test start................................");

    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addPredicates(keyValueQuery2)
            .setAscending(false)
            .setSortKey("observations.attributes.attr_1")
            .build();

    try {
      projectServiceStub.findProjects(findProjects);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "FindProjects not support the observation.attributes test stop................................");
  }

  /** Find projects with value of tags == test_tag_123 */
  @Test
  public void findProjectsByTagsTest() {
    LOGGER.info("FindProjects by tags test start................................");

    // get project with value of tags == test_tag_123
    Value stringValue1 = Value.newBuilder().setStringValue("Tag_1").build();
    KeyValueQuery keyValueQueryTag1 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue1)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    // get projectRun with value of tags == test_tag_456
    Value stringValue2 = Value.newBuilder().setStringValue("Tag_5").build();
    KeyValueQuery keyValueQueryTag2 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue2)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addPredicates(keyValueQueryTag1)
            .addPredicates(keyValueQueryTag2)
            .build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);
    LOGGER.info("FindProjects Response : " + response.getProjectsCount());
    assertEquals(
        "Project count not match with expected project count", 1, response.getProjectsCount());
    assertEquals(
        "Project not match with expected project",
        project3.getId(),
        response.getProjectsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info("FindProjects by tags test stop................................");
  }

  /** Find projects with attribute predicates and sort by attribute key */
  @Test
  public void findAndSortProjectsByAttributeTest() {
    LOGGER.info("Find and sort Projects by attributes test start................................");

    Value numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "Project count not match with expected project count", 3, response.getProjectsCount());

    KeyValueQuery keyValueQueryAccuracy =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.654321).build())
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addPredicates(keyValueQueryAttribute_1)
            .addPredicates(keyValueQueryAccuracy)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();
    response = projectServiceStub.findProjects(findProjects);

    assertEquals(
        "Total records count not matched with expected records count",
        2,
        response.getTotalRecords());
    assertEquals(
        "Project count not match with expected project count", 2, response.getProjectsCount());

    numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setIdsOnly(true)
            .setSortKey("attributes.attribute_1")
            .build();

    response = projectServiceStub.findProjects(findProjects);

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "Project count not match with expected project count", 3, response.getProjectsCount());

    for (int index = 0; index < response.getProjectsCount(); index++) {
      Project project = response.getProjectsList().get(index);
      if (index == 0) {
        assertNotEquals("Project not match with expected project", project3, project);
        assertEquals(
            "Project Id not match with expected project Id", project3.getId(), project.getId());
      } else if (index == 1) {
        assertNotEquals("Project not match with expected project", project2, project);
        assertEquals(
            "Project Id not match with expected project Id", project2.getId(), project.getId());
      } else if (index == 2) {
        assertNotEquals("Project not match with expected project", project1, project);
        assertEquals(
            "Project Id not match with expected project Id", project1.getId(), project.getId());
      }
    }

    LOGGER.info("Find and sort Projects by attributes test stop................................");
  }

  /** Find projects by name key */
  @Test
  public void findProjectsByNameTest() {
    LOGGER.info("Find Projects by name test start................................");

    Value stringValue = Value.newBuilder().setStringValue("CT_1").build();
    KeyValueQuery keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("name")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.CONTAIN)
            .build();

    FindProjects findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectMap.keySet())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setIdsOnly(true)
            .build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);

    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());
    assertEquals(
        "Project Id not match with expected project Id",
        project1.getId(),
        response.getProjects(0).getId());

    LOGGER.info("Find Projects by name test stop................................");
  }

  /** Validation check for the predicate value with empty string which is not valid */
  @Test
  public void findExperimentPredicateValueEmptyNegativeTest() {
    LOGGER.info(
        "FindExperiments predicate value is empty negative test start................................");

    // Validate check for predicate value not empty
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value stringValueType = Value.newBuilder().setStringValue("").build();

    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(stringValueType)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addAllPredicates(predicates)
            // .setIdsOnly(true)
            .build();
    try {
      experimentServiceStub.findExperiments(findExperiments);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    // If key is not set in predicate
    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(
                KeyValueQuery.newBuilder()
                    .setValue(Value.newBuilder().setNumberValue(11).build())
                    .build())
            .build();

    try {
      experimentServiceStub.findExperiments(findExperiments);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info(
        "FindExperiments predicate value is empty negative test stop................................");
  }

  /** Validate check for protobuf struct type in KeyValueQuery not implemented */
  @Test
  public void findExperimentsStructTypeNotImplemented() {
    LOGGER.info(
        "Check for protobuf struct type in KeyValueQuery not implemented in findExperiments test start........");

    // Validate check for struct Type not implemented
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(17.1716586149719).build();

    Struct.Builder struct = Struct.newBuilder();
    struct.putFields("number_value", numValue);
    struct.build();
    Value structValue = Value.newBuilder().setStructValue(struct).build();

    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(structValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addAllPredicates(predicates)
            .build();

    try {
      experimentServiceStub.findExperiments(findExperiments);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.UNIMPLEMENTED.getCode(), status.getCode());
    }

    LOGGER.info(
        "Check for protobuf struct type in KeyValueQuery not implemented in findExperiments test stop........");
  }

  /** Find experiments with value of attributes.attribute_1 <= 0.6543210 */
  @Test
  public void findExperimentsByAttributesTest() {
    LOGGER.info("FindExperiments by attributes test start................................");

    // get experiment with value of attributes.attribute_1 <= 0.6543210
    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);
    LOGGER.info("FindExperiments Response : " + response.getExperimentsList());
    assertEquals(
        "Experiment count not match with expected experiment count",
        3,
        response.getExperimentsList().size());

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());

    for (Experiment fetchedExperiment : response.getExperimentsList()) {
      boolean doesAttributeExist = false;
      for (KeyValue fetchedAttribute : fetchedExperiment.getAttributesList()) {
        if (fetchedAttribute.getKey().equals("attribute_1")) {
          doesAttributeExist = true;
          assertTrue(
              "Experiment attributes.attribute_1 not match with expected experiment attributes.attribute_1",
              fetchedAttribute.getValue().getNumberValue() <= 0.6543210);
        }
      }
      if (!doesAttributeExist) {
        fail("Expected attribute not found in fetched attributes");
      }
    }

    LOGGER.info("FindExperiments by attributes test start................................");
  }

  /**
   * get experiment with value of attributes.attribute_1 <= 0.6543210 & attributes.attribute_2 ==
   * 0.31
   */
  @Test
  public void findExperimentsByMultipleAttributesTest() {
    LOGGER.info(
        "FindExperiments by multiple attributes condition test start................................");

    // get experiment with value of attributes.attribute_1 <= 0.6543210 & attributes.attribute_2 ==
    // 0.31
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.31).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_2")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery2);

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addAllPredicates(predicates)
            .setIdsOnly(true)
            .build();

    FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);
    LOGGER.info("FindExperiments Response : " + response.getExperimentsCount());
    assertEquals(
        "Experiment count not match with expected experiment count",
        1,
        response.getExperimentsCount());
    assertEquals(
        "Experiment not match with expected experiment",
        experiment2.getId(),
        response.getExperimentsList().get(0).getId());
    assertNotEquals(
        "Experiment not match with expected experiment",
        experiment2,
        response.getExperimentsList().get(0));
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info(
        "FindExperiments by multiple attributes condition test stop................................");
  }

  /** Find experiment with value of metrics.accuracy >= 0.6543210 & tags == Tag_7 */
  @Test
  public void findExperimentsByMetricsAndTagsTest() {
    LOGGER.info("FindExperiments by metrics and tags test start................................");

    // get experimentRun with value of metrics.accuracy >= 0.6543210 & tags == Tag_7
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value stringValue = Value.newBuilder().setStringValue("Tag_7").build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery);

    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.GTE)
            .build();
    predicates.add(keyValueQuery2);

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .addAllExperimentIds(experimentMap.keySet())
            .addAllPredicates(predicates)
            .build();

    FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);
    LOGGER.info("FindExperiments Response : " + response.getExperimentsCount());
    assertEquals(
        "Experiment count not match with expected experiment count",
        1,
        response.getExperimentsCount());
    assertEquals(
        "Experiment not match with expected experiment",
        experiment4.getId(),
        response.getExperimentsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info("FindExperiments by metrics and tags test stop................................");
  }

  /** Find experiments with value of endTime */
  @Test
  public void findExperimentsByExperimentEndTimeTest() {
    LOGGER.info("FindExperiments by experiment endtime test start................................");

    // get experiment with value of endTime
    Value stringValue =
        Value.newBuilder().setStringValue(String.valueOf(experiment4.getDateCreated())).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.DATE_CREATED)
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);
    LOGGER.info("FindExperiments Response : " + response.getExperimentsCount());
    assertEquals(
        "Experiment count not match with expected experiment count",
        1,
        response.getExperimentsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experiment4.getId(),
        response.getExperimentsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info("FindExperiments by experiment endtime test stop................................");
  }

  /** Find experiments by attributes with pagination */
  @Test
  public void findExperimentsByAttributesWithPaginationTest() {
    LOGGER.info(
        "FindExperiments by attribute with pagination test start................................");

    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    int pageLimit = 2;
    int count = 0;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      FindExperiments findExperiments =
          FindExperiments.newBuilder()
              .setProjectId(project1.getId())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(true)
              .setSortKey("name")
              .build();

      FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (response.getExperimentsList() != null && response.getExperimentsList().size() > 0) {
        isExpectedResultFound = true;
        for (Experiment experiment : response.getExperimentsList()) {
          assertTrue(
              "Experiment not match with expected experiment",
              experimentMap.containsKey(experiment.getId()));

          if (count == 0) {
            assertEquals(
                "Experiment name not match with expected experiment name",
                experiment1.getName(),
                experiment.getName());
          } else if (count == 1) {
            assertEquals(
                "Experiment name not match with expected experiment name",
                experiment2.getName(),
                experiment.getName());
          } else if (count == 2) {
            assertEquals(
                "Experiment name not match with expected experiment name",
                experiment3.getName(),
                experiment.getName());
          }
          count++;
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

    LOGGER.info(
        "FindExperiments by attribute with pagination test stop................................");
  }

  /** Check observations.attributes not support */
  @Test
  public void findExperimentsNotSupportObservationsAttributesTest() {
    LOGGER.info("FindExperiments not support the observation.attributes test start............");

    Value numValue = Value.newBuilder().setNumberValue(0.31).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery2)
            .setAscending(false)
            .setSortKey("observations.attribute.attr_1")
            .build();

    try {
      experimentServiceStub.findExperiments(findExperiments);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("FindExperiments not support the observation.attributes test stop............");
  }

  /** Find experiments with value of tags == test_tag_123 */
  @Test
  public void findExperimentsByTagsTest() {
    LOGGER.info("FindExperiments by tags test start................................");

    Value stringValue1 = Value.newBuilder().setStringValue("Tag_1").build();
    KeyValueQuery keyValueQueryTag1 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue1)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    // get experimentRun with value of tags == test_tag_456
    Value stringValue2 = Value.newBuilder().setStringValue("Tag_5").build();
    KeyValueQuery keyValueQueryTag2 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue2)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryTag1)
            .addPredicates(keyValueQueryTag2)
            .build();

    FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);
    LOGGER.info("FindExperiments Response : " + response.getExperimentsCount());
    assertEquals(
        "Experiment count not match with expected experiment count",
        1,
        response.getExperimentsCount());
    assertEquals(
        "Experiment not match with expected experiment",
        experiment3.getId(),
        response.getExperimentsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info("FindExperiments by tags test stop................................");
  }

  /** Find experiments with value of endTime */
  @Test
  public void findAndSortExperimentsByAttributesTest() {
    LOGGER.info(
        "Find and sort Experiments by attributes test start................................");

    Value numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();

    FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "Experiment count not match with expected experiment count",
        3,
        response.getExperimentsCount());

    KeyValueQuery keyValueQueryAccuracy =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.654321).build())
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .addPredicates(keyValueQueryAccuracy)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();
    response = experimentServiceStub.findExperiments(findExperiments);

    assertEquals(
        "Total records count not matched with expected records count",
        2,
        response.getTotalRecords());
    assertEquals(
        "Experiment count not match with expected experiment count",
        2,
        response.getExperimentsCount());

    numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setIdsOnly(true)
            .setSortKey("attributes.attribute_1")
            .build();

    response = experimentServiceStub.findExperiments(findExperiments);

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "Experiment count not match with expected experiment count",
        3,
        response.getExperimentsCount());

    for (int index = 0; index < response.getExperimentsCount(); index++) {
      Experiment experiment = response.getExperimentsList().get(index);
      if (index == 0) {
        assertNotEquals("Experiment not match with expected experiment", experiment3, experiment);
        assertEquals(
            "Experiment Id not match with expected experiment Id",
            experiment3.getId(),
            experiment.getId());
      } else if (index == 1) {
        assertNotEquals("Experiment not match with expected experiment", experiment2, experiment);
        assertEquals(
            "Experiment Id not match with expected experiment Id",
            experiment2.getId(),
            experiment.getId());
      } else if (index == 2) {
        assertNotEquals("Experiment not match with expected experiment", experiment1, experiment);
        assertEquals(
            "Experiment Id not match with expected experiment Id",
            experiment1.getId(),
            experiment.getId());
      }
    }

    LOGGER.info(
        "Find and sort Experiments by attributes test stop................................");
  }

  /** Validation check for the predicate value with empty string which is not valid */
  @Test
  public void findExperimentRunPredicateValueEmptyNegativeTest() {
    LOGGER.info("FindExperimentRuns predicate value is empty negative test start.....");

    // Validate check for predicate value not empty
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value stringValueType = Value.newBuilder().setStringValue("").build();

    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(stringValueType)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .setExperimentId(experiment1.getId())
            .addAllPredicates(predicates)
            // .setIdsOnly(true)
            .build();
    AdvancedQueryExperimentRunsResponse response =
        hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    assertEquals("Expected response not found", 0, response.getHydratedExperimentRunsCount());

    // If key is not set in predicate
    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .setExperimentId(experiment1.getId())
            .addPredicates(
                KeyValueQuery.newBuilder()
                    .setValue(Value.newBuilder().setNumberValue(11).build())
                    .build())
            .build();

    try {
      experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("FindExperimentRuns predicate value is empty negative test stop.....");
  }

  /** Validate check for protobuf struct type in KeyValueQuery not implemented */
  @Test
  public void findExperimentRunStructTypeNotImplemented() {
    LOGGER.info(
        "Check for protobuf struct type in KeyValueQuery not implemented test start.......");

    // Validate check for struct Type not implemented
    Value numValue = Value.newBuilder().setNumberValue(17.1716586149719).build();

    Struct.Builder struct = Struct.newBuilder();
    struct.putFields("number_value", numValue);
    struct.build();
    Value structValue = Value.newBuilder().setStructValue(struct).build();

    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(structValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .setExperimentId(experiment1.getId())
            .addPredicates(keyValueQuery)
            .build();

    try {
      experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.UNIMPLEMENTED.getCode(), status.getCode());
    }

    LOGGER.info("Check for protobuf struct type in KeyValueQuery not implemented test stop.......");
  }

  /** Find experimentRun with value of metrics.loss <= 0.6543210 */
  @Test
  public void findExperimentRunsByMetricsTest() {
    LOGGER.info("FindExperimentRuns by metrics test start................................");

    // get experimentRun with value of metrics.loss <= 0.6543210
    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getExperimentRunsList());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        3,
        response.getExperimentRunsList().size());

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());

    for (ExperimentRun fetchedExperimentRun : response.getExperimentRunsList()) {
      boolean doesMetricExist = false;
      for (KeyValue fetchedMetric : fetchedExperimentRun.getMetricsList()) {
        if (fetchedMetric.getKey().equals("loss")) {
          doesMetricExist = true;
          assertTrue(
              "ExperimentRun metrics.loss not match with expected experimentRun metrics.loss",
              fetchedMetric.getValue().getNumberValue() <= 0.6543210);
        }
      }
      if (!doesMetricExist) {
        fail("Expected metric not found in fetched metrics");
      }
    }

    LOGGER.info("FindExperimentRuns by metrics test stop................................");
  }

  /** Find experimentRun with value of metrics.loss <= 0.6543210 & metrics.accuracy == 0.31 */
  @Test
  public void findExperimentRunsByMultipleMetricsTest() {
    LOGGER.info(
        "FindExperimentRuns by multiple metrics condition test start................................");

    // get experimentRun with value of metrics.loss <= 0.6543210 & metrics.accuracy == 0.31
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.31).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("metrics.accuracy")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery2);

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .setExperimentId(experiment1.getId())
            .addAllPredicates(predicates)
            .setIdsOnly(true)
            .build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun12.getId(),
        response.getExperimentRunsList().get(0).getId());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun12,
        response.getExperimentRunsList().get(0));
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info(
        "FindExperimentRuns by multiple metrics condition test stop................................");
  }

  /**
   * Find experimentRun with value of metrics.accuracy >= 0.6543210 & hyperparameters.tuning ==
   * 2.545
   */
  @Test
  public void findExperimentRunsByMetricAndHyperparameterTest() {
    LOGGER.info(
        "FindExperimentRuns by metric and hyperparameter test start................................");

    // get experimentRun with value of metrics.accuracy >= 0.6543210 & hyperparameters.tuning ==
    // 2.545
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(2.545).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("hyperparameters.tuning")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.GTE)
            .build();
    predicates.add(keyValueQuery2);

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .addAllExperimentRunIds(experimentRunMap.keySet())
            .addAllPredicates(predicates)
            .build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun22.getId(),
        response.getExperimentRunsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info(
        "FindExperimentRuns by metric and hyperparameter test stop................................");
  }

  /** Find experimentRun with value of endTime */
  @Test
  public void findExperimentRunsByEndTimeTest() {
    LOGGER.info(
        "FindExperimentRuns by ExperimentRun EndTime test start................................");

    Value stringValue =
        Value.newBuilder().setStringValue(String.valueOf(experimentRun22.getEndTime())).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("end_time")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun22.getId(),
        response.getExperimentRunsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info(
        "FindExperimentRuns by ExperimentRun EndTime test stop................................");
  }

  /** Find experimentRun by metrics and sort by code_version with pagination */
  @Test
  @Ignore("sort by code_version not supported")
  public void findExperimentRunsByMetricsWithPaginationTest() {
    LOGGER.info(
        "FindExperimentRuns by metrics sort by code_version with pagination test start..................");

    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    int pageLimit = 2;
    int count = 0;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      FindExperimentRuns findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project1.getId())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(true)
              .setSortKey("code_version")
              .build();

      FindExperimentRuns.Response response =
          experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (response.getExperimentRunsList() != null && response.getExperimentRunsList().size() > 0) {
        isExpectedResultFound = true;
        for (ExperimentRun experimentRun : response.getExperimentRunsList()) {
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRunMap.get(experimentRun.getId()),
              experimentRun);

          if (count == 0) {
            assertEquals(
                "ExperimentRun code version not match with expected experimentRun code version",
                experimentRun21.getCodeVersion(),
                experimentRun.getCodeVersion());
          } else if (count == 1) {
            assertEquals(
                "ExperimentRun code version not match with expected experimentRun code version",
                experimentRun12.getCodeVersion(),
                experimentRun.getCodeVersion());
          } else if (count == 2) {
            assertEquals(
                "ExperimentRun code version not match with expected experimentRun code version",
                experimentRun11.getCodeVersion(),
                experimentRun.getCodeVersion());
          }
          count++;
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

    LOGGER.info(
        "FindExperimentRuns by metrics sort by code_version with pagination test stop..................");
  }

  /** Find experimentRun by metrics and sort by hyperparameters with pagination */
  @Test
  public void findRunsByMetricsSoryByHyprWithPaginationTest() {
    LOGGER.info(
        "FindExperimentRuns by metrics sort by Hyperparameters with pagination test start.......");

    Value numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    int pageLimit = 2;
    int count = 0;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      FindExperimentRuns findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project1.getId())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(false)
              .setSortKey("hyperparameters.tuning")
              .build();

      FindExperimentRuns.Response response =
          experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (response.getExperimentRunsList() != null && response.getExperimentRunsList().size() > 0) {
        isExpectedResultFound = true;
        for (ExperimentRun experimentRun : response.getExperimentRunsList()) {
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRunMap.get(experimentRun.getId()),
              experimentRun);

          if (count == 0) {
            assertEquals(
                "ExperimentRun hyperparameter not match with expected experimentRun hyperparameter",
                experimentRun11.getHyperparametersList(),
                experimentRun.getHyperparametersList());
          } else if (count == 1) {
            assertEquals(
                "ExperimentRun hyperparameter not match with expected experimentRun hyperparameter",
                experimentRun12.getHyperparametersList(),
                experimentRun.getHyperparametersList());
          } else if (count == 2) {
            assertEquals(
                "ExperimentRun hyperparameter not match with expected experimentRun hyperparameter",
                experimentRun21.getHyperparametersList(),
                experimentRun.getHyperparametersList());
          }
          count++;
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

    LOGGER.info(
        "FindExperimentRuns by metrics sort by Hyperparameters with pagination test stop.......");
  }

  /** Check observations.attributes not support */
  @Test
  public void findExperimentRunsNotSupportObservationsAttributesTest() {
    LOGGER.info("FindExperimentRuns not support the observation.attributes test start.........");

    Value numValue = Value.newBuilder().setNumberValue(0.31).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("metrics.accuracy")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery2)
            .setAscending(false)
            .setSortKey("observations.attribute.attr_1")
            .build();

    try {
      experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("FindExperimentRuns not support the observation.attributes test stop.........");
  }

  /** Find experimentRun with value of tags */
  @Test
  public void findExperimentRunsByTagsTest() {
    LOGGER.info("FindExperimentRuns by tags test start.........");

    // get experimentRun with value of tags == test_tag_123
    Value stringValue1 = Value.newBuilder().setStringValue("test_tag_123").build();
    KeyValueQuery keyValueQueryTag1 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue1)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    // get experimentRun with value of tags == test_tag_456
    Value stringValue2 = Value.newBuilder().setStringValue("test_tag_456").build();
    KeyValueQuery keyValueQueryTag2 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue2)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryTag1)
            .addPredicates(keyValueQueryTag2)
            .build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun22.getId(),
        response.getExperimentRunsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info("FindExperimentRuns by tags test stop.........");
  }

  /** Find experimentRun with value of metrics.loss <= 0.6543210 & sort key metrics.loss */
  @Test
  public void findAndSortExperimentRunsByMetricsTest() {
    LOGGER.info("Find and Sort ExperimentRuns by metrics test start.........");

    Value numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQueryLoss =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryLoss)
            .setAscending(false)
            .setSortKey("metrics.loss")
            .build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        3,
        response.getExperimentRunsCount());

    KeyValueQuery keyValueQueryAccuracy =
        KeyValueQuery.newBuilder()
            .setKey("metrics.accuracy")
            .setValue(Value.newBuilder().setNumberValue(0.654321).build())
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryLoss)
            .addPredicates(keyValueQueryAccuracy)
            .setAscending(false)
            .setSortKey("metrics.loss")
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

    numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQueryLoss =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryLoss)
            .setAscending(false)
            .setIdsOnly(true)
            .setSortKey("metrics.loss")
            .build();

    response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        3,
        response.getExperimentRunsCount());

    for (int index = 0; index < response.getExperimentRunsCount(); index++) {
      ExperimentRun experimentRun = response.getExperimentRunsList().get(index);
      if (index == 0) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun21, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun21.getId(),
            experimentRun.getId());
      } else if (index == 1) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun12, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun12.getId(),
            experimentRun.getId());
      } else if (index == 2) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun11, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun11.getId(),
            experimentRun.getId());
      }
    }

    LOGGER.info("Find and Sort ExperimentRuns by metrics test stop.........");
  }

  @Test
  public void findExperimentRunsNegativeTest() {
    LOGGER.info("FindExperimentRuns Negative test start................................");

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder().setProjectId("12321").build();
    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    assertEquals("Expected response not found", 0, response.getExperimentRunsCount());

    List<String> experimentRunIds = new ArrayList<>();
    experimentRunIds.add("abc");
    experimentRunIds.add("xyz");
    findExperimentRuns =
        FindExperimentRuns.newBuilder().addAllExperimentRunIds(experimentRunIds).build();
    response = experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    assertEquals("Expected response not found", 0, response.getExperimentRunsCount());

    LOGGER.info("FindExperimentRuns Negative test stop................................");
  }

  @Test
  public void findExperimentRunsWithUnwinedTest() {
    LOGGER.info("FindExperimentRuns with unwind test start................................");

    Value strValueLoss = Value.newBuilder().setStringValue("foo").build();
    KeyValueQuery keyValueQueryLoss1 =
        KeyValueQuery.newBuilder()
            .setKey("experiment_id")
            .setValue(strValueLoss)
            .setOperator(OperatorEnum.Operator.NE)
            .build();
    strValueLoss = Value.newBuilder().setStringValue("boo").build();
    KeyValueQuery keyValueQueryLoss2 =
        KeyValueQuery.newBuilder()
            .setKey("experiment_id")
            .setValue(strValueLoss)
            .setOperator(OperatorEnum.Operator.NE)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryLoss1)
            .addPredicates(keyValueQueryLoss2)
            .setAscending(false)
            .setIdsOnly(false)
            .setSortKey("metrics.loss")
            .build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);

    assertEquals(
        "Total records count not matched with expected records count",
        4,
        response.getTotalRecords());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        4,
        response.getExperimentRunsCount());

    for (int index = 0; index < response.getExperimentRunsCount(); index++) {
      ExperimentRun experimentRun = response.getExperimentRunsList().get(index);
      if (index == 0) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun22, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun22.getId(),
            experimentRun.getId());
      } else if (index == 1) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun21, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun21.getId(),
            experimentRun.getId());
      } else if (index == 2) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun12, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun12.getId(),
            experimentRun.getId());
      }
    }

    LOGGER.info("FindExperimentRuns with unwind test stop................................");
  }

  /** Find experiments by workspace */
  @Test
  public void findExperimentsByWorkspaceTest() {
    LOGGER.info("FindExperiments by workspace test start................................");

    FindExperiments findExperiments = FindExperiments.newBuilder().build();

    FindExperiments.Response response = experimentServiceStub.findExperiments(findExperiments);
    List<Experiment> expectedExperiments = new ArrayList<>();
    List<Experiment> staleExperiments = new ArrayList<>();
    for (Experiment exp : response.getExperimentsList()) {
      if (experimentMap.containsKey(exp.getId())) {
        expectedExperiments.add(exp);
      } else {
        staleExperiments.add(exp);
      }
    }
    LOGGER.info("FindExperiments Response : " + response.getExperimentsCount());
    assertEquals(
        "Experiment count not match with expected experiment count",
        experimentMap.size(),
        expectedExperiments.size());
    assertEquals(
        "Total records count not matched with expected records count",
        experimentMap.size(),
        response.getTotalRecords() - staleExperiments.size());

    findExperiments = FindExperiments.newBuilder().addExperimentIds(experiment1.getId()).build();

    response = experimentServiceStub.findExperiments(findExperiments);
    LOGGER.info("FindExperiments Response : " + response.getExperimentsCount());
    assertEquals(
        "Experiment count not match with expected experiment count",
        1,
        response.getExperimentsCount());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info("FindExperiments by workspace test stop................................");
  }

  /** Find experimentRun with value of endTime */
  @Test
  public void findExperimentRunsWithoutProjectExperimentTest() {
    LOGGER.info(
        "FindExperimentRuns without project & experiment test start................................");

    Value stringValue =
        Value.newBuilder().setStringValue(String.valueOf(experimentRun22.getEndTime())).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("end_time")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder().addPredicates(keyValueQuery).build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun22.getId(),
        response.getExperimentRunsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    LOGGER.info(
        "FindExperimentRuns without project & experiment test stop................................");
  }

  @Test
  public void findProjectsByFuzzyOwnerTest() {
    LOGGER.info("FindProjects by owner fuzzy search test start................................");
    if (!testConfig.hasAuth()) {
      assertTrue(true);
      return;
    }
    GetUser getUserRequest =
        GetUser.newBuilder().setEmail(authClientInterceptor.getClient1Email()).build();
    // Get the user info by vertaId form the AuthService
    UserInfo testUser1 = uacServiceStub.getUser(getUserRequest);
    String testUser1UserName = testUser1.getVertaInfo().getUsername();

    // get project with value of attributes.attribute_1 <= 0.6543210
    Value stringValue =
        Value.newBuilder().setStringValue(testUser1UserName.substring(0, 2)).build();
    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("owner")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.CONTAIN)
            .build();

    FindProjects findProjects = FindProjects.newBuilder().addPredicates(keyValueQuery).build();

    FindProjects.Response response = projectServiceStub.findProjects(findProjects);
    List<Project> projectList = new ArrayList<>();
    for (Project project : response.getProjectsList()) {
      if (projectMap.containsKey(project.getId())) {
        projectList.add(project);
      }
    }
    LOGGER.info("FindProjects Response : " + projectList.size());
    assertEquals("Project count not match with expected project count", 4, projectList.size());

    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("owner")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.NOT_CONTAIN)
            .build();
    findProjects = FindProjects.newBuilder().addPredicates(keyValueQuery).build();

    response = projectServiceStub.findProjects(findProjects);
    projectList = new ArrayList<>();
    for (Project project : response.getProjectsList()) {
      if (projectMap.containsKey(project.getId())) {
        projectList.add(project);
      }
    }
    assertEquals(
        "Total records count not matched with expected records count", 0, projectList.size());

    stringValue = Value.newBuilder().setStringValue("asdasdasd").build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("owner")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.CONTAIN)
            .build();

    findProjects = FindProjects.newBuilder().addPredicates(keyValueQuery).build();

    response = projectServiceStub.findProjects(findProjects);
    projectList = new ArrayList<>();
    for (Project project : response.getProjectsList()) {
      if (projectMap.containsKey(project.getId())) {
        projectList.add(project);
      }
    }
    LOGGER.info("FindProjects Response : " + projectList.size());
    assertEquals("Project count not match with expected project count", 0, projectList.size());

    LOGGER.info("FindProjects by owner fuzzy search test stop ................................");
  }

  @Test
  public void findExperimentRunsByExperimentTest() {
    LOGGER.info("FindExperimentRuns by experiment test start.........");

    FindExperimentRuns findExperimentRuns =
        FindExperimentRuns.newBuilder().setExperimentId(experiment1.getId()).build();

    FindExperimentRuns.Response response =
        experimentRunServiceStub.findExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        2,
        response.getExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun12.getId(),
        response.getExperimentRunsList().get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        2,
        response.getTotalRecords());

    LOGGER.info("FindExperimentRuns by experiment test stop.........");
  }
}
