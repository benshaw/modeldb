package ai.verta.modeldb;

import static ai.verta.modeldb.CollaboratorUtils.addCollaboratorRequestProject;
import static ai.verta.modeldb.CollaboratorUtils.addCollaboratorRequestProjectInterceptor;
import static org.junit.Assert.*;

import ai.verta.common.*;
import ai.verta.common.OperatorEnum.Operator;
import ai.verta.uac.*;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HydratedServiceTest extends TestsInit {

  private static final Logger LOGGER = LogManager.getLogger(HydratedServiceTest.class.getName());

  // Project Entities
  private static Project project1;
  private static Project project2;
  private static Project project3;
  private static Project project4;
  private static Map<String, Project> projectsMap = new HashMap<>();

  // Experiment Entities
  private static Experiment experiment1;
  private static Experiment experiment2;
  private static Experiment experiment3;
  private static Experiment experiment4;
  private static Map<String, Experiment> experimentMap = new HashMap<>();

  // ExperimentRun Entities
  private static ExperimentRun experimentRun1;
  private static ExperimentRun experimentRun2;
  private static ExperimentRun experimentRun3;
  private static ExperimentRun experimentRun4;
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
    for (String projectId : projectsMap.keySet()) {
      DeleteProject deleteProject = DeleteProject.newBuilder().setId(projectId).build();
      DeleteProject.Response deleteProjectResponse =
          projectServiceStub.deleteProject(deleteProject);
      LOGGER.info("Project deleted successfully");
      LOGGER.info(deleteProjectResponse.toString());
      assertTrue(deleteProjectResponse.getStatus());
    }

    projectsMap = new HashMap<>();
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
    projectsMap.put(project1.getId(), project1);
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
    projectsMap.put(project2.getId(), project2);
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
    projectsMap.put(project3.getId(), project3);
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
    createProjectRequest =
        createProjectRequest
            .toBuilder()
            .addAttributes(attribute1)
            .addAttributes(attribute2)
            .addTags("Tag_5")
            .addTags("Tag_7")
            .addTags("Tag_8")
            .setVisibility(ResourceVisibility.PRIVATE)
            .build();
    createProjectResponse = projectServiceStub.createProject(createProjectRequest);
    project4 = createProjectResponse.getProject();
    projectsMap.put(project4.getId(), project4);
    LOGGER.info("Project created successfully");
    assertEquals(
        "Project name not match with expected Project name",
        createProjectRequest.getName(),
        project4.getName());
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
    experimentMap.put(experiment1.getId(), experiment1);
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
    experimentMap.put(experiment2.getId(), experiment2);
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
    experimentMap.put(experiment3.getId(), experiment3);
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
    experimentMap.put(experiment4.getId(), experiment4);
    LOGGER.info("Experiment created successfully");
    assertEquals(
        "Experiment name not match with expected Experiment name",
        createExperimentRequest.getName(),
        experiment4.getName());
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
    experimentRun1 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun1.getId(), experimentRun1);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun1.getName());

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
    experimentRun2 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun2.getId(), experimentRun2);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun2.getName());

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
    experimentRun3 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun3.getId(), experimentRun3);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun3.getName());

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
    experimentRun4 = createExperimentRunResponse.getExperimentRun();
    experimentRunMap.put(experimentRun4.getId(), experimentRun4);
    LOGGER.info("ExperimentRun created successfully");
    assertEquals(
        "ExperimentRun name not match with expected ExperimentRun name",
        createExperimentRunRequest.getName(),
        experimentRun4.getName());
  }

  @Test
  public void a_getHydratedProjectsTest() {
    LOGGER.info("Get hydrated projects data test start................................");

    // Create comment for above experimentRun1 & experimentRun3
    // comment for experiment1
    AddComment addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun1.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    LOGGER.info("Comment added successfully for ExperimentRun1");
    // comment for experimentRun3
    addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun3.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    LOGGER.info("Comment added successfully for ExperimentRun3");

    if (testConfig.hasAuth()) {
      // For Collaborator1
      AddCollaboratorRequest addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project1, CollaboratorTypeEnum.CollaboratorType.READ_WRITE, authClientInterceptor);
      collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator1 added successfully");
    }

    GetHydratedProjects.Response getHydratedProjectsResponse =
        hydratedServiceBlockingStub.getHydratedProjects(GetHydratedProjects.newBuilder().build());

    for (String projectId : projectsMap.keySet()) {
      assertTrue(
          "HydratedProjects does not have expected project",
          getHydratedProjectsResponse.getHydratedProjectsList().stream()
              .anyMatch(hydratedProject -> hydratedProject.getProject().getId().equals(projectId)));
    }

    Map<String, HydratedProject> hydratedProjectMap = new HashMap<>();
    for (HydratedProject hydratedProject : getHydratedProjectsResponse.getHydratedProjectsList()) {
      hydratedProjectMap.put(hydratedProject.getProject().getId(), hydratedProject);
    }

    for (Project existingProject : projectsMap.values()) {
      assertEquals(
          "Expected project does not exist in the hydrated projects",
          existingProject.getName(),
          hydratedProjectMap.get(existingProject.getId()).getProject().getName());
      assertEquals(
          "Expected project owner does not match with the hydratedProject owner",
          existingProject.getOwner(),
          authService.getVertaIdFromUserInfo(
              hydratedProjectMap.get(existingProject.getId()).getOwnerUserInfo()));
    }

    LOGGER.info("Get hydrated projects data test stop................................");
  }

  @Test
  public void b_getHydratedProjectTest() {
    LOGGER.info("Get hydrated project data test start................................");

    // Create comment for above experimentRun1 & experimentRun3
    // comment for experiment1
    AddComment addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun1.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    LOGGER.info("Comment added successfully for ExperimentRun1");
    // comment for experimentRun3
    addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun3.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    LOGGER.info("Comment added successfully for ExperimentRun3");

    if (testConfig.hasAuth()) {
      GetUser getUserRequest =
          GetUser.newBuilder().setEmail(authClientInterceptor.getClient2Email()).build();
      // Get the user info by vertaId form the AuthService
      UserInfo shareWithUserInfo = uacServiceStub.getUser(getUserRequest);

      // Create two collaborator for above project
      List<String> collaboratorUsers = new ArrayList<>();
      // For Collaborator1
      AddCollaboratorRequest addCollaboratorRequest =
          addCollaboratorRequestProject(
              project1,
              shareWithUserInfo.getEmail(),
              CollaboratorTypeEnum.CollaboratorType.READ_WRITE);
      collaboratorUsers.add(authService.getVertaIdFromUserInfo(shareWithUserInfo));
      collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator1 added successfully");

      GetHydratedProjectById.Response getHydratedProjectResponse =
          hydratedServiceBlockingStub.getHydratedProjectById(
              GetHydratedProjectById.newBuilder().setId(project1.getId()).build());

      assertEquals(
          "HydratedProject does not match with expected project",
          project1.getName(),
          getHydratedProjectResponse.getHydratedProject().getProject().getName());

      assertEquals(
          "Expected project owner does not match with the hydratedProject owner",
          project1.getOwner(),
          authService.getVertaIdFromUserInfo(
              getHydratedProjectResponse.getHydratedProject().getOwnerUserInfo()));

      assertEquals(
          "Expected shared project user count does not match with the hydratedProject shared project user count",
          collaboratorUsers.size(),
          getHydratedProjectResponse.getHydratedProject().getCollaboratorUserInfosCount());

      LOGGER.info("existing project collaborator count: " + collaboratorUsers.size());
      for (String existingUserId : collaboratorUsers) {
        boolean match = false;
        for (CollaboratorUserInfo collaboratorUserInfo :
            getHydratedProjectResponse.getHydratedProject().getCollaboratorUserInfosList()) {
          if (existingUserId.equals(
              collaboratorUserInfo.getCollaboratorUserInfo().getVertaInfo().getUserId())) {
            LOGGER.info("existing project collborator : " + existingUserId);
            LOGGER.info(
                "Hydrated project collborator : "
                    + authService.getVertaIdFromUserInfo(
                        collaboratorUserInfo.getCollaboratorUserInfo()));
            match = true;
            break;
          }
        }
        if (!match) {
          LOGGER.warn("Hydrated collaborator user not match with existing collaborator user");
          fail();
        }
      }
    }

    LOGGER.info("Get hydrated project data test stop................................");
  }

  @Test
  public void a_getHydratedExperimentRunsTest() {
    LOGGER.info("Get hydrated ExperimentRuns data test start................................");

    // Create comment for above experimentRun1 & experimentRun3
    // comment for experiment1
    AddComment addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun1.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    AddComment.Response commentResponse =
        commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    Comment experimentRun1Comment = commentResponse.getComment();
    LOGGER.info("Comment added successfully for ExperimentRun1");
    // comment for experimentRun3
    addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun3.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    LOGGER.info("Comment added successfully for ExperimentRun3");

    if (testConfig.hasAuth()) {
      // Create two collaborator for above project
      // For Collaborator1
      AddCollaboratorRequest addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project1, CollaboratorTypeEnum.CollaboratorType.READ_WRITE, authClientInterceptor);
      collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator1 added successfully");
    }

    int pageLimit = 2;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      GetHydratedExperimentRunsByProjectId.Response getHydratedExperimentRunsResponse =
          hydratedServiceBlockingStub.getHydratedExperimentRunsInProject(
              GetHydratedExperimentRunsByProjectId.newBuilder()
                  .setProjectId(project1.getId())
                  .setPageNumber(pageNumber)
                  .setPageLimit(pageLimit)
                  .setAscending(true)
                  .setSortKey(ModelDBConstants.NAME)
                  .build());

      assertEquals(
          "HydratedExperimentRuns count does not match with existing ExperimentRun count",
          experimentRunMap.size(),
          getHydratedExperimentRunsResponse.getTotalRecords());

      if (getHydratedExperimentRunsResponse.getHydratedExperimentRunsList() != null
          && getHydratedExperimentRunsResponse.getHydratedExperimentRunsList().size() > 0) {
        isExpectedResultFound = true;
        for (HydratedExperimentRun hydratedExperimentRun :
            getHydratedExperimentRunsResponse.getHydratedExperimentRunsList()) {
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRunMap.get(hydratedExperimentRun.getExperimentRun().getId()),
              hydratedExperimentRun.getExperimentRun());

          if (testConfig.hasAuth()) {
            assertEquals(
                "Expected experimentRun owner does not match with the hydratedExperimentRun owner",
                experimentRunMap.get(hydratedExperimentRun.getExperimentRun().getId()).getOwner(),
                authService.getVertaIdFromUserInfo(hydratedExperimentRun.getOwnerUserInfo()));

            if (hydratedExperimentRun.getExperimentRun().getName().equals("ExperiemntRun1")) {
              assertEquals(
                  "Expected experimentRun owner does not match with the hydratedExperimentRun owner",
                  Collections.singletonList(experimentRun1Comment),
                  hydratedExperimentRun.getCommentsList());
            }
          }
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

    GetHydratedExperimentRunsByProjectId.Response getHydratedExperimentRunsResponse =
        hydratedServiceBlockingStub.getHydratedExperimentRunsInProject(
            GetHydratedExperimentRunsByProjectId.newBuilder()
                .setProjectId(project1.getId())
                .setPageNumber(1)
                .setPageLimit(1)
                .setAscending(false)
                .setSortKey("metrics.loss")
                .build());

    assertEquals(
        "Total records count not matched with expected records count",
        4,
        getHydratedExperimentRunsResponse.getTotalRecords());
    assertEquals(
        "ExperimentRuns count not match with expected experimentRuns count",
        1,
        getHydratedExperimentRunsResponse.getHydratedExperimentRunsCount());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun4,
        getHydratedExperimentRunsResponse.getHydratedExperimentRuns(0).getExperimentRun());
    assertEquals(
        "Experiment name not match with expected Experiment name",
        experiment2.getName(),
        getHydratedExperimentRunsResponse.getHydratedExperimentRuns(0).getExperiment().getName());

    LOGGER.info("Get hydrated ExperimentRuns data test stop................................");
  }

  @Test
  public void a_getHydratedExperimentRunByIdTest() {
    LOGGER.info("Get hydrated ExperimentRun By ID data test start................................");
    // Create comment for above experimentRun1 & experimentRun3
    // comment for experiment1
    AddComment addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun1.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    AddComment.Response addCommentResponse =
        commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    Comment comment1 = addCommentResponse.getComment();
    LOGGER.info("Comment added successfully for ExperimentRun1");
    // comment for experimentRun3
    addCommentRequest =
        AddComment.newBuilder()
            .setEntityId(experimentRun3.getId())
            .setMessage(
                "Hello, this project is interesting." + Calendar.getInstance().getTimeInMillis())
            .build();
    commentServiceBlockingStub.addExperimentRunComment(addCommentRequest);
    LOGGER.info("Comment added successfully for ExperimentRun3");

    // Create two collaborator for above project
    // For Collaborator1
    if (testConfig.hasAuth()) {
      AddCollaboratorRequest addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project1, CollaboratorTypeEnum.CollaboratorType.READ_WRITE, authClientInterceptor);
      collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator1 added successfully");
    }

    GetHydratedExperimentRunById.Response getHydratedExperimentRunsResponse =
        hydratedServiceBlockingStub.getHydratedExperimentRunById(
            GetHydratedExperimentRunById.newBuilder().setId(experimentRun1.getId()).build());

    assertEquals(
        "ExperimentRun not match with expected ExperimentRun",
        experimentRun1,
        getHydratedExperimentRunsResponse.getHydratedExperimentRun().getExperimentRun());

    assertEquals(
        "Experiment name not match with expected Experiment name",
        experiment1.getName(),
        getHydratedExperimentRunsResponse.getHydratedExperimentRun().getExperiment().getName());

    if (testConfig.hasAuth()) {
      assertEquals(
          "Hydrated comments not match with expected ExperimentRun comments",
          Collections.singletonList(comment1),
          getHydratedExperimentRunsResponse.getHydratedExperimentRun().getCommentsList());
    }

    LOGGER.info("Get hydrated ExperimentRun By ID data test stop................................");
  }

  @Test
  public void getHydratedExperimentsTest() {
    LOGGER.info("Get hydrated Experiments data test start................................");

    GetHydratedExperimentsByProjectId.Response getHydratedExperimentsResponse =
        hydratedServiceBlockingStub.getHydratedExperimentsByProjectId(
            GetHydratedExperimentsByProjectId.newBuilder().setProjectId(project1.getId()).build());

    assertEquals(
        "HydratedExperiments count does not match with existing Experiment count",
        experimentMap.size(),
        getHydratedExperimentsResponse.getHydratedExperimentsCount());

    Map<String, HydratedExperiment> hydratedExperimentMap = new HashMap<>();
    for (HydratedExperiment hydratedExperiment :
        getHydratedExperimentsResponse.getHydratedExperimentsList()) {
      hydratedExperimentMap.put(hydratedExperiment.getExperiment().getId(), hydratedExperiment);
    }

    for (Experiment experiment : experimentMap.values()) {
      Experiment responseExperiment = hydratedExperimentMap.get(experiment.getId()).getExperiment();
      List<String> tags = experiment.getTagsList().stream().sorted().collect(Collectors.toList());
      experiment =
          experiment
              .toBuilder()
              .clearTags()
              .addAllTags(tags)
              .setDateUpdated(responseExperiment.getDateUpdated())
              .setVersionNumber(responseExperiment.getVersionNumber())
              .build();
      experimentMap.put(experiment.getId(), experiment);
      assertEquals(
          "Expected experimentRun not exist in the hydrated experimentRun",
          experiment,
          responseExperiment);
      assertEquals(
          "Expected experimentRun owner not match with the hydratedExperimentRun owner",
          experiment.getOwner(),
          authService.getVertaIdFromUserInfo(
              hydratedExperimentMap.get(experiment.getId()).getOwnerUserInfo()));
    }

    LOGGER.info("Get hydrated ExperimentRuns data test stop................................");
  }

  @Test
  public void findHydratedExperimentRunsTest() {
    LOGGER.info("FindHydratedExperimentRuns test start................................");

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
    AdvancedQueryExperimentRunsResponse hydratedResponse =
        hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    assertEquals(
        "Expected response not found", 0, hydratedResponse.getHydratedExperimentRunsCount());

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
      hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    // Validate check for struct Type not implemented
    predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(17.1716586149719).build();

    Struct.Builder struct = Struct.newBuilder();
    struct.putFields("number_value", numValue);
    struct.build();
    Value structValue = Value.newBuilder().setStructValue(struct).build();

    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(structValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .setExperimentId(experiment1.getId())
            .addAllPredicates(predicates)
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.UNIMPLEMENTED.getCode(), status.getCode());
    }

    // get experimentRun with value of metrics.loss <= 0.6543210
    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    AdvancedQueryExperimentRunsResponse response =
        hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    List<ExperimentRun> experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        3,
        experimentRuns.size());

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());

    for (ExperimentRun fetchedExperimentRun : experimentRuns) {
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

    // get experimentRun with value of metrics.loss <= 0.6543210 & metrics.accuracy == 0.31
    predicates = new ArrayList<>();
    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery =
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

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .setExperimentId(experiment1.getId())
            .addAllPredicates(predicates)
            .setIdsOnly(true)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        experimentRuns.size());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun2.getId(),
        experimentRuns.get(0).getId());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun2,
        experimentRuns.get(0));
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    // get experimentRun with value of metrics.accuracy >= 0.6543210 & hyperparameters.tuning ==
    // 2.545
    predicates = new ArrayList<>();
    numValue = Value.newBuilder().setNumberValue(2.545).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("hyperparameters.tuning")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.GTE)
            .build();
    predicates.add(keyValueQuery2);

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .addAllExperimentRunIds(experimentRunMap.keySet())
            .addAllPredicates(predicates)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        experimentRuns.size());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun4.getId(),
        experimentRuns.get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());
    assertEquals(
        "Expected experimentRun owner not match with the hydratedExperimentRun owner",
        experimentRun4.getOwner(),
        authService.getVertaIdFromUserInfo(
            response.getHydratedExperimentRunsList().get(0).getOwnerUserInfo()));

    // get experimentRun with value of endTime == 1550837
    Value stringValue =
        Value.newBuilder().setStringValue(String.valueOf(experimentRun4.getEndTime())).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("end_time")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        experimentRuns.size());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun4.getId(),
        experimentRuns.get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());
    assertEquals(
        "Expected experimentRun owner not match with the hydratedExperimentRun owner",
        experimentRun4.getOwner(),
        authService.getVertaIdFromUserInfo(
            response.getHydratedExperimentRunsList().get(0).getOwnerUserInfo()));

    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("metrics.loss")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    int pageLimit = 2;
    int count = 0;
    boolean isExpectedResultFound = false;
    // TODO: do not handled sort_key as `code_version`
    /*for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project1.getId())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(true)
              .setSortKey("code_version")
              .build();

      response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
      LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
      experimentRuns = new ArrayList<>();
      for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
        experimentRuns.add(hydratedExperimentRun.getExperimentRun());
      }

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (!experimentRuns.isEmpty()) {
        isExpectedResultFound = true;
        for (int index = 0; index < experimentRuns.size(); index++) {
          ExperimentRun experimentRun = experimentRuns.get(index);
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRunMap.get(experimentRun.getId()),
              experimentRun);

          String responseUsername =
              authService.getVertaIdFromUserInfo(
                  response.getHydratedExperimentRunsList().get(index).getOwnerUserInfo());
          if (count == 0) {
            assertEquals(
                "ExperimentRun code version not match with expected experimentRun code version",
                experimentRun3.getCodeVersion(),
                experimentRun.getCodeVersion());
            assertEquals(
                "Expected experimentRun owner not match with the hydratedExperimentRun owner",
                experimentRun3.getOwner(),
                responseUsername);
          } else if (count == 1) {
            assertEquals(
                "ExperimentRun code version not match with expected experimentRun code version",
                experimentRun2.getCodeVersion(),
                experimentRun.getCodeVersion());
            assertEquals(
                "Expected experimentRun owner not match with the hydratedExperimentRun owner",
                experimentRun2.getOwner(),
                responseUsername);
          } else if (count == 2) {
            assertEquals(
                "ExperimentRun code version not match with expected experimentRun code version",
                experimentRun1.getCodeVersion(),
                experimentRun.getCodeVersion());
            assertEquals(
                "Expected experimentRun owner not match with the hydratedExperimentRun owner",
                experimentRun1.getOwner(),
                responseUsername);
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
    }*/

    pageLimit = 2;
    count = 0;
    isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      findExperimentRuns =
          FindExperimentRuns.newBuilder()
              .setProjectId(project1.getId())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(false)
              .setSortKey("hyperparameters.tuning")
              .build();

      response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
      LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
      experimentRuns = new ArrayList<>();
      for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
        experimentRuns.add(hydratedExperimentRun.getExperimentRun());
      }

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (!experimentRuns.isEmpty()) {
        isExpectedResultFound = true;
        for (int index = 0; index < experimentRuns.size(); index++) {
          ExperimentRun experimentRun = experimentRuns.get(index);
          assertEquals(
              "ExperimentRun not match with expected experimentRun",
              experimentRunMap.get(experimentRun.getId()),
              experimentRun);

          String responseUsername =
              authService.getVertaIdFromUserInfo(
                  response.getHydratedExperimentRunsList().get(index).getOwnerUserInfo());
          if (count == 0) {
            assertEquals(
                "ExperimentRun hyperparameter not match with expected experimentRun hyperparameter",
                experimentRun1.getHyperparametersList(),
                experimentRun.getHyperparametersList());
            assertEquals(
                "Expected experimentRun owner not match with the hydratedExperimentRun owner",
                experimentRun1.getOwner(),
                responseUsername);
          } else if (count == 1) {
            assertEquals(
                "ExperimentRun hyperparameter not match with expected experimentRun hyperparameter",
                experimentRun2.getHyperparametersList(),
                experimentRun.getHyperparametersList());
            assertEquals(
                "Expected experimentRun owner not match with the hydratedExperimentRun owner",
                experimentRun2.getOwner(),
                responseUsername);
          } else if (count == 2) {
            assertEquals(
                "ExperimentRun hyperparameter not match with expected experimentRun hyperparameter",
                experimentRun3.getHyperparametersList(),
                experimentRun.getHyperparametersList());
            assertEquals(
                "Expected experimentRun owner not match with the hydratedExperimentRun owner",
                experimentRun3.getOwner(),
                responseUsername);
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

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery2)
            .setAscending(false)
            .setSortKey("observations.attribute.attr_1")
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

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

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryTag1)
            .addPredicates(keyValueQueryTag2)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        1,
        experimentRuns.size());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experimentRun4.getId(),
        experimentRuns.get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());
    assertEquals(
        "Expected experimentRun owner not match with the hydratedExperimentRun owner",
        experimentRun4.getOwner(),
        authService.getVertaIdFromUserInfo(
            response.getHydratedExperimentRunsList().get(0).getOwnerUserInfo()));

    Value numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQueryLoss =
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
            .setSortKey("metrics.loss")
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        3,
        experimentRuns.size());

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
    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }

    assertEquals(
        "Total records count not matched with expected records count",
        2,
        response.getTotalRecords());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        2,
        experimentRuns.size());

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

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    LOGGER.info("FindExperimentRuns Response : " + response.getHydratedExperimentRunsCount());
    experimentRuns = new ArrayList<>();
    for (HydratedExperimentRun hydratedExperimentRun : response.getHydratedExperimentRunsList()) {
      experimentRuns.add(hydratedExperimentRun.getExperimentRun());
    }

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "ExperimentRun count not match with expected experimentRun count",
        3,
        experimentRuns.size());

    for (int index = 0; index < experimentRuns.size(); index++) {
      ExperimentRun experimentRun = experimentRuns.get(index);
      if (index == 0) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun3, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun3.getId(),
            experimentRun.getId());
      } else if (index == 1) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun2, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun2.getId(),
            experimentRun.getId());
      } else if (index == 2) {
        assertEquals(
            "ExperimentRun not match with expected experimentRun", experimentRun1, experimentRun);
        assertEquals(
            "ExperimentRun Id not match with expected experimentRun Id",
            experimentRun1.getId(),
            experimentRun.getId());
      }
    }

    keyValueQueryLoss =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.ID)
            .setValue(Value.newBuilder().setStringValue("xyz").build())
            .setOperator(Operator.EQ)
            .build();

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryLoss)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    assertEquals("Expected response not found", 0, response.getHydratedExperimentRunsCount());

    keyValueQueryLoss =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.ID)
            .setValue(Value.newBuilder().setStringValue("xyz").build())
            .setOperator(Operator.NE)
            .build();

    findExperimentRuns =
        FindExperimentRuns.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryLoss)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperimentRuns(findExperimentRuns);
    assertEquals("Expected response not found", 0, response.getHydratedExperimentRunsCount());

    LOGGER.info("FindHydratedExperimentRuns test stop................................");
  }

  @Test
  public void getHydratedExperimentsWithPaginationInProject() {
    LOGGER.info(
        "Get Experiment with pagination of project test start................................");
    int pageLimit = 2;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      GetHydratedExperimentsByProjectId getExperiment =
          GetHydratedExperimentsByProjectId.newBuilder()
              .setProjectId(project1.getId())
              .setPageNumber(pageNumber)
              .setPageLimit(pageLimit)
              .setAscending(true)
              .setSortKey(ModelDBConstants.NAME)
              .build();

      GetHydratedExperimentsByProjectId.Response experimentResponse =
          hydratedServiceBlockingStub.getHydratedExperimentsByProjectId(getExperiment);

      assertEquals(
          "Total records count not matched with expected records count",
          experimentMap.size(),
          experimentResponse.getTotalRecords());

      List<Experiment> experimentList = new ArrayList<>();
      for (HydratedExperiment hydratedExperiment :
          experimentResponse.getHydratedExperimentsList()) {
        experimentList.add(hydratedExperiment.getExperiment());
      }

      if (!experimentList.isEmpty()) {
        isExpectedResultFound = true;
        LOGGER.info("GetExperimentsInProject Response : " + experimentList.size());
        for (Experiment experiment : experimentList) {
          Experiment expectedExperiment = experimentMap.get(experiment.getId());
          List<String> tags =
              expectedExperiment.getTagsList().stream().sorted().collect(Collectors.toList());
          expectedExperiment =
              expectedExperiment
                  .toBuilder()
                  .clearTags()
                  .addAllTags(tags)
                  .setDateUpdated(experiment.getDateUpdated())
                  .setVersionNumber(experiment.getVersionNumber())
                  .build();
          experimentMap.put(expectedExperiment.getId(), expectedExperiment);
          assertEquals(
              "Experiment not match with expected Experiment", expectedExperiment, experiment);
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
      GetHydratedExperimentsByProjectId getExperiment =
          GetHydratedExperimentsByProjectId.newBuilder()
              .setProjectId(project1.getId())
              .setPageNumber(pageNumber)
              .setPageLimit(pageLimit)
              .setAscending(true)
              .setSortKey("attributes.attribute_2_2")
              .build();

      GetHydratedExperimentsByProjectId.Response experimentResponse =
          hydratedServiceBlockingStub.getHydratedExperimentsByProjectId(getExperiment);

      List<Experiment> experimentList = new ArrayList<>();
      for (HydratedExperiment hydratedExperiment :
          experimentResponse.getHydratedExperimentsList()) {
        experimentList.add(hydratedExperiment.getExperiment());
      }

      assertEquals(
          "Total records count not matched with expected records count",
          experimentMap.size(),
          experimentResponse.getTotalRecords());

      if (!experimentList.isEmpty()) {

        LOGGER.info("GetExperimentsInProject Response : " + experimentList.size());
        for (Experiment experiment : experimentList) {
          assertEquals(
              "Experiment not match with expected Experiment",
              experimentMap.get(experiment.getId()),
              experiment);

          assertEquals(
              "ExperimentRun code version not match with expected experimentRun code version",
              experimentMap.get(experiment.getId()).getAttributesList(),
              experiment.getAttributesList());
          count++;
        }

      } else {
        LOGGER.warn("More Experiment not found in database");
        assertTrue(true);
        break;
      }
    }

    GetHydratedExperimentsByProjectId getExperiment =
        GetHydratedExperimentsByProjectId.newBuilder()
            .setProjectId(project1.getId())
            .setPageNumber(1)
            .setPageLimit(1)
            .setAscending(true)
            .setSortKey("observations.attribute.attr_1")
            .build();
    try {
      hydratedServiceBlockingStub.getHydratedExperimentsByProjectId(getExperiment);
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
  public void findHydratedExperimentsTest() {
    LOGGER.info("FindHydratedExperiments test start................................");

    // Validate check for predicate value not empty
    List<KeyValueQuery> predicates = new ArrayList<>();
    Value stringValueType = Value.newBuilder().setStringValue("").build();

    KeyValueQuery keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(stringValueType)
            .setOperator(Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    FindExperiments findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addAllPredicates(predicates)
            // .setIdsOnly(true)
            .build();
    try {
      hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
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
      hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    // Validate check for struct Type not implemented
    predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(17.1716586149719).build();

    Struct.Builder struct = Struct.newBuilder();
    struct.putFields("number_value", numValue);
    struct.build();
    Value structValue = Value.newBuilder().setStructValue(struct).build();

    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(structValue)
            .setOperator(Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addAllPredicates(predicates)
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.UNIMPLEMENTED.getCode(), status.getCode());
    }

    // get experiment with value of attributes.attribute_1 <= 0.6543210
    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(Operator.LTE)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    AdvancedQueryExperimentsResponse response =
        hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    List<Experiment> experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }
    LOGGER.info("FindExperiments Response size: " + experimentList.size());
    assertEquals(
        "Experiment count not match with expected experiment count", 3, experimentList.size());

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());

    for (Experiment fetchedExperiment : experimentList) {
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

    // get experiment with value of attributes.attribute_1 <= 0.6543210 & attributes.attribute_2 ==
    // 0.31
    predicates = new ArrayList<>();
    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.31).build();
    KeyValueQuery keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_2")
            .setValue(numValue)
            .setOperator(Operator.EQ)
            .build();
    predicates.add(keyValueQuery2);

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addAllPredicates(predicates)
            .setIdsOnly(true)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }
    LOGGER.info("FindExperiments Response : " + experimentList.size());
    assertEquals(
        "Experiment count not match with expected experiment count", 1, experimentList.size());
    assertEquals(
        "Experiment not match with expected experiment",
        experiment2.getId(),
        experimentList.get(0).getId());
    assertNotEquals(
        "Experiment not match with expected experiment", experiment2, experimentList.get(0));
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    // get experimentRun with value of metrics.accuracy >= 0.6543210 & tags == Tag_7
    predicates = new ArrayList<>();
    Value stringValue = Value.newBuilder().setStringValue("Tag_7").build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue)
            .setOperator(Operator.EQ)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(Operator.GTE)
            .build();
    predicates.add(keyValueQuery2);

    findExperiments =
        FindExperiments.newBuilder()
            .addAllExperimentIds(experimentMap.keySet())
            .addAllPredicates(predicates)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }
    LOGGER.info("FindExperiments Response : " + experimentList.size());
    assertEquals(
        "Experiment count not match with expected experiment count", 1, experimentList.size());
    assertEquals(
        "Experiment not match with expected experiment",
        experiment4.getId(),
        experimentList.get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    // get experiment with value of endTime
    stringValue =
        Value.newBuilder().setStringValue(String.valueOf(experiment4.getDateCreated())).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.DATE_CREATED)
            .setValue(stringValue)
            .setOperator(Operator.EQ)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }
    LOGGER.info("FindExperiments Response : " + experimentList.size());
    assertEquals(
        "Experiment count not match with expected experiment count", 1, experimentList.size());
    assertEquals(
        "ExperimentRun not match with expected experimentRun",
        experiment4.getId(),
        experimentList.get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(Operator.LTE)
            .build();

    int pageLimit = 2;
    int count = 0;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      findExperiments =
          FindExperiments.newBuilder()
              .setProjectId(project1.getId())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(true)
              .setSortKey("name")
              .build();

      response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
      experimentList = new ArrayList<>();
      for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
        experimentList.add(hydratedExperiment.getExperiment());
      }

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (!experimentList.isEmpty()) {
        isExpectedResultFound = true;
        for (Experiment experiment : experimentList) {
          Experiment expectedExperiment = experimentMap.get(experiment.getId());
          List<String> tags =
              expectedExperiment.getTagsList().stream().sorted().collect(Collectors.toList());
          expectedExperiment =
              expectedExperiment
                  .toBuilder()
                  .clearTags()
                  .addAllTags(tags)
                  .setDateUpdated(experiment.getDateUpdated())
                  .setVersionNumber(experiment.getVersionNumber())
                  .build();
          experimentMap.put(expectedExperiment.getId(), expectedExperiment);
          assertEquals(
              "Experiment not match with expected experiment", expectedExperiment, experiment);

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

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQuery2)
            .setAscending(false)
            .setSortKey("observations.attribute.attr_1")
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    // get experiment with value of tags == test_tag_123
    Value stringValue1 = Value.newBuilder().setStringValue("Tag_1").build();
    KeyValueQuery keyValueQueryTag1 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue1)
            .setOperator(Operator.EQ)
            .build();
    // get experimentRun with value of tags == test_tag_456
    Value stringValue2 = Value.newBuilder().setStringValue("Tag_5").build();
    KeyValueQuery keyValueQueryTag2 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue2)
            .setOperator(Operator.EQ)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryTag1)
            .addPredicates(keyValueQueryTag2)
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }
    LOGGER.info("FindExperiments Response : " + experimentList.size());
    assertEquals(
        "Experiment count not match with expected experiment count", 1, experimentList.size());
    assertEquals(
        "Experiment not match with expected experiment",
        experiment3.getId(),
        experimentList.get(0).getId());
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    Value numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(Operator.LTE)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "Experiment count not match with expected experiment count", 3, experimentList.size());

    KeyValueQuery keyValueQueryAccuracy =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.654321).build())
            .setOperator(Operator.LTE)
            .build();
    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .addPredicates(keyValueQueryAccuracy)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();
    response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }

    assertEquals(
        "Total records count not matched with expected records count",
        2,
        response.getTotalRecords());
    assertEquals(
        "Experiment count not match with expected experiment count", 2, experimentList.size());

    numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(Operator.LTE)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setIdsOnly(true)
            .setSortKey("attributes.attribute_1")
            .build();

    response = hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
    experimentList = new ArrayList<>();
    for (HydratedExperiment hydratedExperiment : response.getHydratedExperimentsList()) {
      experimentList.add(hydratedExperiment.getExperiment());
    }

    assertEquals(
        "Total records count not matched with expected records count",
        3,
        response.getTotalRecords());
    assertEquals(
        "Experiment count not match with expected experiment count", 3, experimentList.size());

    for (int index = 0; index < experimentList.size(); index++) {
      Experiment experiment = experimentList.get(index);
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

    keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.ID)
            .setValue(Value.newBuilder().setStringValue("xyz").build())
            .setOperator(Operator.EQ)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
      if (testConfig.hasAuth()) {
        fail();
      }
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.ID)
            .setValue(Value.newBuilder().setStringValue("xyz").build())
            .setOperator(Operator.NE)
            .build();

    findExperiments =
        FindExperiments.newBuilder()
            .setProjectId(project1.getId())
            .addPredicates(keyValueQueryAttribute_1)
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedExperiments(findExperiments);
      if (testConfig.hasAuth()) {
        fail();
      }
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("FindExperimentRuns test stop................................");
  }

  @Test
  public void getHydratedProjectsByPagination() {
    LOGGER.info("Get Hydrated Project by pagination test start................................");
    GetHydratedProjects getHydratedProjects = GetHydratedProjects.newBuilder().build();
    GetHydratedProjects.Response response =
        hydratedServiceBlockingStub.getHydratedProjects(getHydratedProjects);
    LOGGER.info("GetHydratedProjects Count : " + response.getTotalRecords());
    List<Project> projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    assertEquals(
        "HydratedProjects count not match with expected HydratedProjects count",
        projectsMap.size(),
        projectList.size());

    for (Project project : projectList) {
      if (projectsMap.get(project.getId()) == null) {
        fail("Project not found in the expected project list");
      }
    }

    int pageLimit = 1;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      getHydratedProjects =
          GetHydratedProjects.newBuilder()
              .setPageNumber(pageNumber)
              .setPageLimit(pageLimit)
              .setAscending(false)
              .setSortKey(ModelDBConstants.NAME)
              .build();

      GetHydratedProjects.Response hydratedProjectsResponse =
          hydratedServiceBlockingStub.getHydratedProjects(getHydratedProjects);

      projectList = new ArrayList<>();
      for (HydratedProject hydratedProject : hydratedProjectsResponse.getHydratedProjectsList()) {
        if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
          projectList.add(hydratedProject.getProject());
        }
      }

      if (!projectList.isEmpty()) {
        isExpectedResultFound = true;
        LOGGER.info("GetProjects Response : " + projectList.size());
        for (Project project : projectList) {
          Project expectedProject = projectsMap.get(project.getId());
          expectedProject =
              expectedProject
                  .toBuilder()
                  .setDateUpdated(project.getDateUpdated())
                  .setVersionNumber(project.getVersionNumber())
                  .build();
          projectsMap.put(expectedProject.getId(), expectedProject);
          assertEquals("Project not match with expected Project", expectedProject, project);
        }

        if (pageNumber == 1) {
          assertEquals(
              "Project not match with expected Project",
              projectsMap.get(projectList.get(0).getId()),
              project4);
        } else if (pageNumber == 3) {
          assertEquals(
              "Project not match with expected Project",
              projectsMap.get(projectList.get(0).getId()),
              project2);
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

    LOGGER.info("Get Hydrated project by pagination test stop................................");
  }

  @Test
  public void findHydratedProjectsTest() {
    LOGGER.info("FindHydratedProjects test start................................");

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
      hydratedServiceBlockingStub.findHydratedProjects(findProjects);
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
      hydratedServiceBlockingStub.findHydratedProjects(findProjects);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    // Validate check for struct Type not implemented
    predicates = new ArrayList<>();
    Value numValue = Value.newBuilder().setNumberValue(17.1716586149719).build();

    Struct.Builder struct = Struct.newBuilder();
    struct.putFields("number_value", numValue);
    struct.build();
    Value structValue = Value.newBuilder().setStructValue(struct).build();

    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(structValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    predicates.add(keyValueQuery);

    findProjects =
        FindProjects.newBuilder()
            .addProjectIds(project1.getId())
            .addAllPredicates(predicates)
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedProjects(findProjects);
      fail();
    } catch (StatusRuntimeException exc) {
      Status status = Status.fromThrowable(exc);
      assertEquals(Status.UNIMPLEMENTED.getCode(), status.getCode());
    }

    // get project with value of attributes.attribute_1 <= 0.6543210
    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    findProjects = FindProjects.newBuilder().addPredicates(keyValueQuery).build();

    AdvancedQueryProjectsResponse response =
        hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    List<Project> projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    LOGGER.info("FindProjects Response : " + projectList.size());
    assertEquals("Project count not match with expected project count", 3, projectList.size());

    for (Project fetchedProject : projectList) {
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

    // get project with value of attributes.attribute_1 <= 0.6543210 & attributes.attribute_2 ==
    // 0.31
    predicates = new ArrayList<>();
    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery =
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

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addAllPredicates(predicates)
            .setIdsOnly(true)
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    LOGGER.info("FindProjects Response : " + projectList.size());
    assertEquals("Project count not match with expected project count", 1, projectList.size());
    assertEquals(
        "Project not match with expected project", project2.getId(), projectList.get(0).getId());
    assertNotEquals("Project not match with expected project", project2, projectList.get(0));
    assertEquals(
        "Total records count not matched with expected records count",
        1,
        response.getTotalRecords());

    // get projectRun with value of metrics.accuracy >= 0.6543210 & tags == Tag_7
    predicates = new ArrayList<>();
    Value stringValue = Value.newBuilder().setStringValue("Tag_7").build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    predicates.add(keyValueQuery);

    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.GTE)
            .build();
    predicates.add(keyValueQuery2);

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addAllPredicates(predicates)
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    LOGGER.info("FindProjects Response : " + projectList.size());
    assertEquals("Project count not match with expected project count", 1, projectList.size());
    assertEquals(
        "Project not match with expected project", project4.getId(), projectList.get(0).getId());

    // get project with value of endTime
    stringValue =
        Value.newBuilder().setStringValue(String.valueOf(project4.getDateCreated())).build();
    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.DATE_CREATED)
            .setValue(stringValue)
            .setOperator(OperatorEnum.Operator.EQ)
            .build();

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addPredicates(keyValueQuery)
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    LOGGER.info("FindProjects Response : " + projectList.size());
    assertEquals("Project count not match with expected project count", 1, projectList.size());
    assertEquals(
        "ProjectRun not match with expected projectRun",
        project4.getId(),
        projectList.get(0).getId());

    numValue = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValue)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    int pageLimit = 2;
    int count = 0;
    boolean isExpectedResultFound = false;
    for (int pageNumber = 1; pageNumber < 100; pageNumber++) {
      findProjects =
          FindProjects.newBuilder()
              .addAllProjectIds(projectsMap.keySet())
              .addPredicates(keyValueQuery2)
              .setPageLimit(pageLimit)
              .setPageNumber(pageNumber)
              .setAscending(true)
              .setSortKey("name")
              .build();

      response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
      projectList = new ArrayList<>();
      for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
        if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
          projectList.add(hydratedProject.getProject());
        }
      }

      assertEquals(
          "Total records count not matched with expected records count",
          3,
          response.getTotalRecords());

      if (projectList.size() > 0) {
        isExpectedResultFound = true;
        for (Project project : projectList) {
          Project expectedProject = projectsMap.get(project.getId());
          expectedProject =
              expectedProject
                  .toBuilder()
                  .setDateUpdated(project.getDateUpdated())
                  .setVersionNumber(project.getVersionNumber())
                  .build();
          projectsMap.put(expectedProject.getId(), expectedProject);
          assertEquals("Project not match with expected Project", expectedProject, project);

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

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addPredicates(keyValueQuery2)
            .setAscending(false)
            .setSortKey("observations.attribute.attr_1")
            .build();

    try {
      hydratedServiceBlockingStub.findHydratedProjects(findProjects);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

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

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addPredicates(keyValueQueryTag1)
            .addPredicates(keyValueQueryTag2)
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    LOGGER.info("FindProjects Response : " + projectList.size());
    assertEquals("Project count not match with expected project count", 1, projectList.size());
    assertEquals(
        "Project not match with expected project", project3.getId(), projectList.get(0).getId());

    Value numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    KeyValueQuery keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    assertEquals("Project count not match with expected project count", 3, projectList.size());

    KeyValueQuery keyValueQueryAccuracy =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_2")
            .setValue(Value.newBuilder().setNumberValue(0.654321).build())
            .setOperator(OperatorEnum.Operator.LTE)
            .build();
    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addPredicates(keyValueQueryAttribute_1)
            .addPredicates(keyValueQueryAccuracy)
            .setAscending(false)
            .setSortKey("attributes.attribute_1")
            .build();
    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    assertEquals("Project count not match with expected project count", 2, projectList.size());

    numValueLoss = Value.newBuilder().setNumberValue(0.6543210).build();
    keyValueQueryAttribute_1 =
        KeyValueQuery.newBuilder()
            .setKey("attributes.attribute_1")
            .setValue(numValueLoss)
            .setOperator(OperatorEnum.Operator.LTE)
            .build();

    findProjects =
        FindProjects.newBuilder()
            .addAllProjectIds(projectsMap.keySet())
            .addPredicates(keyValueQueryAttribute_1)
            .setAscending(false)
            .setIdsOnly(true)
            .setSortKey("attributes.attribute_1")
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    assertEquals("Project count not match with expected project count", 3, projectList.size());

    for (int index = 0; index < projectList.size(); index++) {
      Project project = projectList.get(index);
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

    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(Value.newBuilder().setStringValue("_8").build())
            .setOperator(Operator.CONTAIN)
            .build();
    findProjects =
        FindProjects.newBuilder()
            .addPredicates(keyValueQuery)
            .setAscending(false)
            .setIdsOnly(false)
            .setSortKey("name")
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    assertEquals(
        "HydratedProject count not match with expected HydratedProject count",
        1,
        projectList.size());
    assertEquals(
        "HydratedProject Id not match with expected HydratedProject Id",
        project4.getId(),
        projectList.get(0).getId());

    KeyValueQuery keyValueQuery1 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(Value.newBuilder().setStringValue("_8").build())
            .setOperator(Operator.NOT_CONTAIN)
            .build();
    keyValueQuery2 =
        KeyValueQuery.newBuilder()
            .setKey("tags")
            .setValue(Value.newBuilder().setStringValue("_x").build())
            .setOperator(Operator.CONTAIN)
            .build();
    findProjects =
        FindProjects.newBuilder()
            .addPredicates(keyValueQuery1)
            .addPredicates(keyValueQuery2)
            .setAscending(false)
            .setIdsOnly(false)
            .setSortKey("name")
            .build();

    response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
    projectList = new ArrayList<>();
    for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
      if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
        projectList.add(hydratedProject.getProject());
      }
    }
    assertEquals(
        "HydratedProject count not match with expected HydratedProject count",
        3,
        projectList.size());
    assertEquals(
        "HydratedProject Id not match with expected HydratedProject Id",
        project3.getId(),
        projectList.get(0).getId());

    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.ID)
            .setValue(Value.newBuilder().setStringValue("xyz").build())
            .setOperator(OperatorEnum.Operator.EQ)
            .build();
    findProjects = FindProjects.newBuilder().addPredicates(keyValueQuery).build();

    try {
      response = hydratedServiceBlockingStub.findHydratedProjects(findProjects);
      if (!testConfig.hasAuth()) {
        assertEquals(0, response.getTotalRecords());
      } else {
        fail();
      }
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.PERMISSION_DENIED.getCode(), status.getCode());
    }

    keyValueQuery =
        KeyValueQuery.newBuilder()
            .setKey(ModelDBConstants.ID)
            .setValue(Value.newBuilder().setStringValue("xyz").build())
            .setOperator(OperatorEnum.Operator.NE)
            .build();
    findProjects = FindProjects.newBuilder().addPredicates(keyValueQuery).build();

    try {
      hydratedServiceBlockingStub.findHydratedProjects(findProjects);
      fail();
    } catch (StatusRuntimeException e) {
      Status status = Status.fromThrowable(e);
      LOGGER.warn("Error Code : " + status.getCode() + " Description : " + status.getDescription());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    }

    LOGGER.info("FindProjectRuns test stop................................");
  }

  @Test
  public void findHydratedProjectsByWorkspaceTest() {
    LOGGER.info("FindHydratedProjectsByWorkspace test start................................");

    if (!testConfig.hasAuth()) {
      assertTrue(true);
      return;
    }

    Map<String, Project> secondProjectMap = new HashMap<>();
    Map<String, Project> firstProjectMap = new HashMap<>();
    try {
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
          client2ProjectServiceStub.createProject(createProjectRequest);
      Project project1 = createProjectResponse.getProject();
      secondProjectMap.put(project1.getId(), project1);
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected Project name",
          createProjectRequest.getName(),
          project1.getName());

      // project2 of above project
      createProjectRequest =
          projectTest.getCreateProjectRequest("Project-2-" + new Date().getTime());
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
      createProjectResponse = client2ProjectServiceStub.createProject(createProjectRequest);
      Project project2 = createProjectResponse.getProject();
      secondProjectMap.put(project2.getId(), project2);
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected Project name",
          createProjectRequest.getName(),
          project2.getName());

      // project3 of above project
      createProjectRequest =
          projectTest.getCreateProjectRequest("Project-3-" + new Date().getTime());
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
      Project project3 = createProjectResponse.getProject();
      firstProjectMap.put(project3.getId(), project3);
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected Project name",
          createProjectRequest.getName(),
          project3.getName());

      // project4 of above project
      createProjectRequest =
          projectTest.getCreateProjectRequest("Project-4-" + new Date().getTime());
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
      createProjectRequest =
          createProjectRequest
              .toBuilder()
              .addAttributes(attribute1)
              .addAttributes(attribute2)
              .addTags("Tag_5")
              .addTags("Tag_7")
              .addTags("Tag_8")
              .setVisibility(ResourceVisibility.PRIVATE)
              .build();
      createProjectResponse = projectServiceStub.createProject(createProjectRequest);
      Project project4 = createProjectResponse.getProject();
      firstProjectMap.put(project4.getId(), project4);
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected Project name",
          createProjectRequest.getName(),
          project4.getName());

      GetUser getUserRequest =
          GetUser.newBuilder().setEmail(authClientInterceptor.getClient2Email()).build();
      // Get the user info by vertaId form the AuthService
      UserInfo secondUserInfo = uacServiceStub.getUser(getUserRequest);

      FindProjects findProjects =
          FindProjects.newBuilder()
              .addPredicates(
                  KeyValueQuery.newBuilder()
                      .setKey(ModelDBConstants.NAME)
                      .setValue(Value.newBuilder().setStringValue(project1.getName()).build())
                      .setOperator(OperatorEnum.Operator.EQ)
                      .build())
              .setWorkspaceName(secondUserInfo.getVertaInfo().getUsername())
              .build();

      AdvancedQueryProjectsResponse response =
          hydratedServiceBlockingStub.findHydratedProjects(findProjects);
      List<Project> projectList = new ArrayList<>();
      for (HydratedProject hydratedProject : response.getHydratedProjectsList()) {
        if (projectsMap.containsKey(hydratedProject.getProject().getId())) {
          projectList.add(hydratedProject.getProject());
        }
      }
      LOGGER.info("FindProjects Response : " + projectList.size());
      assertEquals("Project count not match with expected project count", 0, projectList.size());

      assertEquals(
          "Total records count not matched with expected records count", 0, projectList.size());
    } finally {
      for (String projectId : firstProjectMap.keySet()) {
        DeleteProject deleteProject = DeleteProject.newBuilder().setId(projectId).build();
        DeleteProject.Response deleteProjectResponse =
            projectServiceStub.deleteProject(deleteProject);
        LOGGER.info("Project deleted successfully");
        LOGGER.info(deleteProjectResponse.toString());
        assertTrue(deleteProjectResponse.getStatus());
      }
      for (String projectId : secondProjectMap.keySet()) {
        DeleteProject deleteProject = DeleteProject.newBuilder().setId(projectId).build();
        DeleteProject.Response deleteProjectResponse =
            client2ProjectServiceStub.deleteProject(deleteProject);
        LOGGER.info("Project deleted successfully");
        LOGGER.info(deleteProjectResponse.toString());
        assertTrue(deleteProjectResponse.getStatus());
      }
    }
    LOGGER.info("FindHydratedProjectsByUser test stop................................");
  }

  @Test
  public void checkCollaboratorDeleteActionTest() {
    LOGGER.info("Check collaborator has delete action test start.........");

    if (!testConfig.hasAuth()) {
      Assert.assertTrue(true);
      return;
    }

    // Create project
    ProjectTest projectTest = new ProjectTest();
    CreateProject createProjectRequest =
        projectTest.getCreateProjectRequest("Project-1-" + new Date().getTime());
    CreateProject.Response createProjectResponse =
        projectServiceStub.createProject(createProjectRequest);
    Project project = createProjectResponse.getProject();
    try {
      LOGGER.info("Project created successfully");
      assertEquals(
          "Project name not match with expected project name",
          createProjectRequest.getName(),
          project.getName());

      AddCollaboratorRequest addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project, CollaboratorTypeEnum.CollaboratorType.READ_WRITE, authClientInterceptor);
      AddCollaboratorRequest.Response projectCollaboratorResponse =
          collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator updated in server : " + projectCollaboratorResponse.getStatus());
      assertTrue(projectCollaboratorResponse.getStatus());

      CreateExperiment createExperimentRequest =
          ExperimentTest.getCreateExperimentRequest(
              project.getId(), "Experiment-1-" + new Date().getTime());
      CreateExperiment.Response createExperimentResponse =
          experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment1 = createExperimentResponse.getExperiment();
      LOGGER.info("Experiment created successfully");
      assertEquals(
          "Experiment name not match with expected Experiment name",
          createExperimentRequest.getName(),
          createExperimentResponse.getExperiment().getName());

      createExperimentRequest =
          ExperimentTest.getCreateExperimentRequest(
              project.getId(), "Experiment-2-" + new Date().getTime());
      createExperimentResponse = experimentServiceStub.createExperiment(createExperimentRequest);
      Experiment experiment2 = createExperimentResponse.getExperiment();
      LOGGER.info("Experiment created successfully");
      assertEquals(
          "Experiment name not match with expected Experiment name",
          createExperimentRequest.getName(),
          createExperimentResponse.getExperiment().getName());

      CreateExperimentRun createExperimentRunRequest =
          ExperimentRunTest.getCreateExperimentRunRequest(
              project.getId(), experiment1.getId(), "ExperimentRun-1-" + new Date().getTime());
      CreateExperimentRun.Response createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun11 = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun11.getName());

      createExperimentRunRequest =
          ExperimentRunTest.getCreateExperimentRunRequest(
              project.getId(), experiment1.getId(), "ExperimentRun-2-" + new Date().getTime());
      createExperimentRunResponse =
          experimentRunServiceStub.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun12 = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun12.getName());

      createExperimentRunRequest =
          ExperimentRunTest.getCreateExperimentRunRequest(
              project.getId(), experiment2.getId(), "ExperimentRun-3-" + new Date().getTime());
      createExperimentRunResponse =
          experimentRunServiceStubClient2.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun21 = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun21.getName());

      createExperimentRunRequest =
          ExperimentRunTest.getCreateExperimentRunRequest(
              project.getId(), experiment2.getId(), "ExperimentRun-4-" + new Date().getTime());
      createExperimentRunResponse =
          experimentRunServiceStubClient2.createExperimentRun(createExperimentRunRequest);
      ExperimentRun experimentRun22 = createExperimentRunResponse.getExperimentRun();
      LOGGER.info("ExperimentRun created successfully");
      assertEquals(
          "ExperimentRun name not match with expected ExperimentRun name",
          createExperimentRunRequest.getName(),
          experimentRun22.getName());

      addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project, CollaboratorTypeEnum.CollaboratorType.READ_ONLY, authClientInterceptor);
      projectCollaboratorResponse =
          collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator added in server : " + projectCollaboratorResponse.getStatus());
      assertTrue(projectCollaboratorResponse.getStatus());

      FindExperimentRuns findExperimentRuns =
          FindExperimentRuns.newBuilder().setProjectId(project.getId()).build();

      AdvancedQueryExperimentRunsResponse advancedQueryExperimentRunsResponse =
          hydratedServiceBlockingStubClient2.findHydratedExperimentRuns(findExperimentRuns);

      Action deleteAction =
          Action.newBuilder()
              .setModeldbServiceAction(ModelDBActionEnum.ModelDBServiceActions.DELETE)
              .setService(ServiceEnum.Service.MODELDB_SERVICE)
              .build();
      for (HydratedExperimentRun hydratedExperimentRun :
          advancedQueryExperimentRunsResponse.getHydratedExperimentRunsList()) {
        if (hydratedExperimentRun.getExperimentRun().equals(experimentRun21)
            && hydratedExperimentRun.getExperimentRun().equals(experimentRun22)) {
          assertTrue(
              "Experiment actions not match with expected action list",
              hydratedExperimentRun.getAllowedActionsList().contains(deleteAction));
        } else {
          assertFalse(
              "Experiment actions not match with expected action list",
              hydratedExperimentRun.getAllowedActionsList().contains(deleteAction));
        }
      }

      addCollaboratorRequest =
          addCollaboratorRequestProjectInterceptor(
              project, CollaboratorTypeEnum.CollaboratorType.READ_WRITE, authClientInterceptor);
      projectCollaboratorResponse =
          collaboratorServiceStubClient1.addOrUpdateProjectCollaborator(addCollaboratorRequest);
      LOGGER.info("Collaborator updated in server : " + projectCollaboratorResponse.getStatus());
      assertTrue(projectCollaboratorResponse.getStatus());

      advancedQueryExperimentRunsResponse =
          hydratedServiceBlockingStubClient2.findHydratedExperimentRuns(findExperimentRuns);

      for (HydratedExperimentRun hydratedExperimentRun :
          advancedQueryExperimentRunsResponse.getHydratedExperimentRunsList()) {
        if (hydratedExperimentRun.getExperimentRun().equals(experimentRun21)
            || hydratedExperimentRun.getExperimentRun().equals(experimentRun22)) {
          assertTrue(
              "Experiment actions not match with expected action list",
              hydratedExperimentRun.getAllowedActionsList().contains(deleteAction));
        } else {
          assertFalse(
              "Experiment actions not match with expected action list",
              hydratedExperimentRun.getAllowedActionsList().contains(deleteAction));
        }
      }
    } finally {
      DeleteProject deleteProject = DeleteProject.newBuilder().setId(project.getId()).build();
      DeleteProject.Response deleteProjectResponse =
          projectServiceStub.deleteProject(deleteProject);
      LOGGER.info("Project deleted successfully");
      LOGGER.info(deleteProjectResponse.toString());
      assertTrue(deleteProjectResponse.getStatus());

      deleteEntitiesCron.run();
    }

    LOGGER.info("Check collaborator has delete action test stop.........");
  }
}
