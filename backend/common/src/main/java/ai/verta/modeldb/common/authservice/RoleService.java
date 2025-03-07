package ai.verta.modeldb.common.authservice;

import ai.verta.common.ModelDBResourceEnum.ModelDBServiceResourceTypes;
import ai.verta.common.WorkspaceTypeEnum;
import ai.verta.modeldb.common.collaborator.CollaboratorBase;
import ai.verta.uac.*;
import com.google.protobuf.GeneratedMessageV3;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface RoleService {

  boolean createWorkspacePermissions(
      Optional<Long> workspaceId,
      Optional<String> workspaceName,
      String resourceId,
      String resourceName,
      Optional<Long> ownerId,
      ModelDBServiceResourceTypes resourceType,
      CollaboratorPermissions permissions,
      ResourceVisibility resourceVisibility,
      boolean isServiceUser);

  boolean deleteEntityResourcesWithServiceUser(
      List<String> entityIds, ModelDBServiceResourceTypes modelDBServiceResourceTypes);

  GetResourcesResponseItem getEntityResource(
      Optional<String> entityId,
      Optional<String> workspaceName,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes);

  default GetResourcesResponseItem getEntityResource(
      String entityId, ModelDBServiceResourceTypes modelDBServiceResourceTypes) {
    return getEntityResource(Optional.of(entityId), Optional.empty(), modelDBServiceResourceTypes);
  }

  List<GetResourcesResponseItem> getEntityResourcesByName(
      Optional<String> entityName,
      Optional<String> workspaceName,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes);

  GeneratedMessageV3 getOrgById(String orgId);

  GeneratedMessageV3 getTeamById(String teamId);

  List<GetResourcesResponseItem> getResourceItems(
      Workspace workspace,
      Set<String> resourceIds,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      boolean isServiceUser);

  List<GetResourcesResponseItem> getResourceItemsSpecialPersonalWorkspace(
      Workspace workspace,
      Set<String> resourceIds,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes);

  List<String> getWorkspaceRoleBindings(
      String workspaceId,
      WorkspaceTypeEnum.WorkspaceType workspaceType,
      String resourceId,
      String adminRole,
      ModelDBServiceResourceTypes resourceType,
      boolean orgScopedPublic,
      String globalSharing);

  String buildRoleBindingName(
      String roleName, String resourceId, CollaboratorBase collaborator, String resourceTypeName);

  String buildRoleBindingName(
      String roleName, String resourceId, String userId, String resourceTypeName);

  List<String> getAccessibleResourceIds(
      CollaboratorBase hostUserInfo,
      CollaboratorBase currentLoginUserInfo,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      List<String> requestedResourceIds);

  List<String> getAllowedResources(
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      ModelDBActionEnum.ModelDBServiceActions modelDBServiceActions,
      CollaboratorBase collaboratorBase);

  List<String> getSelfAllowedResources(
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      ModelDBActionEnum.ModelDBServiceActions modelDBServiceActions);

  List<String> getSelfDirectlyAllowedResources(
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      ModelDBActionEnum.ModelDBServiceActions modelDBServiceActions);

  void isSelfAllowed(
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      ModelDBActionEnum.ModelDBServiceActions modelDBServiceActions,
      String resourceId);

  List<String> getAccessibleResourceIdsByActions(
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      ModelDBActionEnum.ModelDBServiceActions modelDBServiceActions,
      List<String> requestedIdList);

  Map<String, Actions> getSelfAllowedActionsBatch(
      List<String> resourceIds, ModelDBServiceResourceTypes type);

  void createRoleBinding(
      String roleName,
      CollaboratorBase collaborator,
      String resourceId,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes);

  boolean deleteRoleBindingsUsingServiceUser(List<String> roleBindingNames);

  GeneratedMessageV3 getTeamByName(String orgId, String teamName);

  GeneratedMessageV3 getOrgByName(String name);

  List<Organization> listMyOrganizations();
}
