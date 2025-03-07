package ai.verta.modeldb.utils;

import ai.verta.common.*;
import ai.verta.common.ModelDBResourceEnum.ModelDBServiceResourceTypes;
import ai.verta.common.OperatorEnum.Operator;
import ai.verta.modeldb.*;
import ai.verta.modeldb.authservice.MDBRoleService;
import ai.verta.modeldb.common.CommonConstants;
import ai.verta.modeldb.common.CommonUtils;
import ai.verta.modeldb.common.authservice.AuthService;
import ai.verta.modeldb.common.exceptions.ModelDBException;
import ai.verta.modeldb.entities.*;
import ai.verta.modeldb.entities.config.ConfigBlobEntity;
import ai.verta.modeldb.entities.config.HyperparameterElementMappingEntity;
import ai.verta.modeldb.entities.metadata.LabelsMappingEntity;
import ai.verta.modeldb.entities.versioning.VersioningModeldbEntityMapping;
import ai.verta.modeldb.exceptions.InvalidArgumentException;
import ai.verta.modeldb.exceptions.PermissionDeniedException;
import ai.verta.modeldb.exceptions.UnimplementedException;
import ai.verta.modeldb.metadata.IDTypeEnum;
import ai.verta.modeldb.versioning.Blob;
import ai.verta.modeldb.versioning.BlobExpanded;
import ai.verta.uac.GetResourcesResponseItem;
import ai.verta.uac.ResourceVisibility;
import ai.verta.uac.UserInfo;
import ai.verta.uac.Workspace;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import com.google.rpc.Code;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.persistence.criteria.*;
import javax.persistence.criteria.CriteriaBuilder.Trimspec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.query.Query;

public class RdbmsUtils {

  private static final Logger LOGGER = LogManager.getLogger(RdbmsUtils.class);
  private static final String MAX_EPOCH_NUMBER_SQL_1 =
      "select max(o.epoch_number) From (select keyvaluemapping_id, epoch_number from observation where experiment_run_id ='";
  private static final String MAX_EPOCH_NUMBER_SQL_2 =
      "' and entity_name = 'ExperimentRunEntity') o, (select id from keyvalue where kv_key ='";
  private static final String MAX_EPOCH_NUMBER_SQL_3 =
      "' and  entity_name IS NULL) k where o.keyvaluemapping_id = k.id ";
  private static final String AND_QUERY_OPERATOR = " AND ";

  public static JobEntity generateJobEntity(Job job) {
    return new JobEntity(job);
  }

  public static ProjectEntity generateProjectEntity(Project project) {
    return new ProjectEntity(project);
  }

  // TODO: delete as it seems unused
  public static List<Project> convertProjectsFromProjectEntityList(
      MDBRoleService mdbRoleService,
      AuthService authService,
      List<ProjectEntity> projectEntityList) {
    List<Project> projects = new ArrayList<>();
    if (projectEntityList != null) {
      Map<Long, Workspace> cacheWorkspaceMap = new HashMap<>();
      Map<String, GetResourcesResponseItem> getResourcesMap = new HashMap<>();
      for (ProjectEntity projectEntity : projectEntityList) {
        projects.add(
            projectEntity.getProtoObject(
                mdbRoleService, authService, cacheWorkspaceMap, getResourcesMap));
      }
    }
    return projects;
  }

  public static ExperimentEntity generateExperimentEntity(Experiment experiment) {
    return new ExperimentEntity(experiment);
  }

  public static List<Experiment> convertExperimentsFromExperimentEntityList(
      List<ExperimentEntity> experimentEntityList) {
    List<Experiment> experiments = new ArrayList<>();
    if (experimentEntityList != null) {
      for (ExperimentEntity experimentEntity : experimentEntityList) {
        experiments.add(experimentEntity.getProtoObject());
      }
    }
    return experiments;
  }

  public static List<ExperimentRun> convertExperimentRunsFromExperimentRunEntityList(
      List<ExperimentRunEntity> experimentRunEntityList) {
    List<ExperimentRun> experimentRuns = new ArrayList<>();
    if (experimentRunEntityList != null) {
      for (ExperimentRunEntity experimentRunEntity : experimentRunEntityList) {
        experimentRuns.add(experimentRunEntity.getProtoObject());
      }
    }
    return experimentRuns;
  }

  public static ExperimentRunEntity generateExperimentRunEntity(ExperimentRun experimentRun) {
    return new ExperimentRunEntity(experimentRun);
  }

  public static AttributeEntity generateAttributeEntity(
      Object entity, String fieldType, KeyValue attribute) {
    return new AttributeEntity(entity, fieldType, attribute);
  }

  public static List<AttributeEntity> convertAttributesFromAttributeEntityList(
      Object entity, String fieldType, List<KeyValue> attributes) {
    List<AttributeEntity> attributeList = new ArrayList<>();
    if (attributes != null) {
      for (KeyValue attribute : attributes) {
        var attributeEntity = generateAttributeEntity(entity, fieldType, attribute);
        attributeList.add(attributeEntity);
      }
    }
    return attributeList;
  }

  public static List<KeyValue> convertAttributeEntityListFromAttributes(
      List<AttributeEntity> attributeEntityList) {

    LOGGER.trace("Converting AttributeEntityListFromAttributes ");
    List<KeyValue> attributeList = new ArrayList<>();
    if (attributeEntityList != null) {
      attributeList =
          attributeEntityList.stream()
              .map(AttributeEntity::getProtoObj)
              .sorted(Comparator.comparing(KeyValue::getKey))
              .collect(Collectors.toList());
    }
    LOGGER.trace("Converted AttributeEntityListFromAttributes ");
    return attributeList;
  }

  public static KeyValueEntity generateKeyValueEntity(
      Object entity, String fieldType, KeyValue keyValue) {
    return new KeyValueEntity(entity, fieldType, keyValue);
  }

  public static List<KeyValueEntity> convertKeyValuesFromKeyValueEntityList(
      Object entity, String fieldType, List<KeyValue> keyValueList) {
    List<KeyValueEntity> attributeList = new ArrayList<>();
    if (keyValueList != null) {
      for (KeyValue keyValue : keyValueList) {
        var keyValueEntity = generateKeyValueEntity(entity, fieldType, keyValue);
        attributeList.add(keyValueEntity);
      }
    }
    return attributeList;
  }

  public static List<KeyValue> convertKeyValueEntityListFromKeyValues(
      List<KeyValueEntity> keyValueEntityList) {

    LOGGER.trace("Converting KeyValueEntityListFromKeyValues ");
    List<KeyValue> attributeList = new ArrayList<>();
    if (keyValueEntityList != null) {
      attributeList =
          keyValueEntityList.stream()
              .map(KeyValueEntity::getProtoKeyValue)
              .sorted(Comparator.comparing(KeyValue::getKey))
              .collect(Collectors.toList());
    }
    LOGGER.trace("Converted KeyValueEntityListFromKeyValues ");
    return attributeList;
  }

  public static ArtifactEntity generateArtifactEntity(
      Object entity, String fieldType, Artifact artifact) {
    return new ArtifactEntity(entity, fieldType, artifact);
  }

  public static List<ArtifactEntity> convertArtifactsFromArtifactEntityList(
      Object entity, String fieldType, List<Artifact> artifactList) {
    List<ArtifactEntity> artifactEntityList = new ArrayList<>();
    if (artifactList != null) {
      return artifactList.stream()
          .map(artifact -> generateArtifactEntity(entity, fieldType, artifact))
          .collect(Collectors.toList());
    }
    return artifactEntityList;
  }

  public static List<Artifact> convertArtifactEntityListFromArtifacts(
      List<ArtifactEntity> artifactEntityList) {
    if (artifactEntityList != null) {
      return artifactEntityList.stream()
          .map(ArtifactEntity::getProtoObject)
          .sorted(Comparator.comparing(Artifact::getKey))
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public static ObservationEntity generateObservationEntity(
      Session session,
      Object entity,
      String fieldType,
      Observation observation,
      String entity_name,
      String entity_id) {
    if (session == null // request came from creating the entire ExperimentRun
        || observation.hasEpochNumber()) {
      if (observation.getEpochNumber().getKindCase() != KindCase.NUMBER_VALUE) {
        String invalidEpochMessage =
            "Observations can only have numeric epoch_number, condition not met in " + observation;
        throw new InvalidArgumentException(invalidEpochMessage);
      }
      return new ObservationEntity(entity, fieldType, observation);
    } else {
      if (entity_name.equalsIgnoreCase("ExperimentRunEntity") && observation.hasAttribute()) {
        StringBuilder MAX_EPOCH_NUMBER_SQL =
            new StringBuilder(MAX_EPOCH_NUMBER_SQL_1)
                .append(entity_id)
                .append(MAX_EPOCH_NUMBER_SQL_2)
                .append(observation.getAttribute().getKey())
                .append(MAX_EPOCH_NUMBER_SQL_3);
        Query sqlQuery = session.createSQLQuery(MAX_EPOCH_NUMBER_SQL.toString());
        BigInteger maxEpochNumber = (BigInteger) sqlQuery.uniqueResult();
        Long newEpochValue = maxEpochNumber == null ? 0L : maxEpochNumber.longValue() + 1;

        var new_observation =
            Observation.newBuilder(observation)
                .setEpochNumber(Value.newBuilder().setNumberValue(newEpochValue))
                .build();
        return new ObservationEntity(entity, fieldType, new_observation);
      } else {
        String unimplementedErrorMessage =
            "Observations not supported for non ExperimentRun entities, found "
                + entity_name
                + " in "
                + observation;
        LOGGER.warn(unimplementedErrorMessage);
        throw new UnimplementedException(unimplementedErrorMessage);
      }
    }
  }

  public static List<ObservationEntity> convertObservationsFromObservationEntityList(
      Session session,
      Object entity,
      String fieldType,
      List<Observation> observationList,
      String entity_name,
      String entity_id) {
    LOGGER.trace("Converting ObservationsFromObservationEntityList");
    LOGGER.trace("fieldType {}", fieldType);
    List<ObservationEntity> observationEntityList = new ArrayList<>();
    if (observationList != null) {
      LOGGER.trace("observationList size {}", observationList.size());
      for (Observation observation : observationList) {
        var observationEntity =
            generateObservationEntity(
                session, entity, fieldType, observation, entity_name, entity_id);
        observationEntityList.add(observationEntity);
      }
    }
    LOGGER.trace("observationList size 0");
    LOGGER.trace("Converted ObservationsFromObservationEntityList");
    return observationEntityList;
  }

  public static List<Observation> convertObservationEntityListFromObservations(
      List<ObservationEntity> observationEntityList) {
    List<Observation> observationList = new ArrayList<>();
    LOGGER.trace("Converting ObservationEntityListFromObservations");
    if (observationEntityList != null) {
      observationList =
          observationEntityList.stream()
              .map(ObservationEntity::getProtoObject)
              .sorted(Comparator.comparing(RdbmsUtils::getObservationCompareKey))
              .collect(Collectors.toList());
    }
    LOGGER.trace("Converted ObservationEntityListFromObservations");
    return observationList;
  }

  public static String getObservationCompareKey(Observation observation) {
    if (observation.hasArtifact()) {
      return observation.getArtifact().getKey();
    } else {
      return observation.getAttribute().getKey();
    }
  }

  public static List<TagsMapping> convertTagListFromTagMappingList(
      Object entity, List<String> tagsList) {
    List<TagsMapping> tagsMappings = new ArrayList<>();
    if (tagsList != null) {
      for (String tag : tagsList) {
        var tagsMapping = new TagsMapping(entity, tag);
        tagsMappings.add(tagsMapping);
      }
    }
    return tagsMappings;
  }

  public static List<String> convertTagsMappingListFromTagList(List<TagsMapping> tagsMappings) {
    if (tagsMappings != null) {
      return tagsMappings.stream().map(TagsMapping::getTag).sorted().collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public static List<FeatureEntity> convertFeatureListFromFeatureMappingList(
      Object entity, List<Feature> features) {
    List<FeatureEntity> featureEntities = new ArrayList<>();
    if (features != null) {
      for (Feature feature : features) {
        var featureEntity = new FeatureEntity(entity, feature);
        featureEntities.add(featureEntity);
      }
    }
    return featureEntities;
  }

  public static List<Feature> convertFeatureEntityListFromFeatureList(
      List<FeatureEntity> featureEntities) {
    if (featureEntities != null) {
      return featureEntities.stream()
          .map(FeatureEntity::getProtoObject)
          .sorted(Comparator.comparing(Feature::getName))
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public static DatasetEntity generateDatasetEntity(Dataset dataset) {
    return new DatasetEntity(dataset);
  }

  public static List<Dataset> convertDatasetsFromDatasetEntityList(
      MDBRoleService mdbRoleService, List<DatasetEntity> datasetEntityList) {
    List<Dataset> datasets = new ArrayList<>();
    if (datasetEntityList != null) {
      for (DatasetEntity datasetEntity : datasetEntityList) {
        datasets.add(datasetEntity.getProtoObject(mdbRoleService));
      }
    }
    return datasets;
  }

  public static CodeVersionEntity generateCodeVersionEntity(
      String fieldType, CodeVersion codeVersion) {
    return new CodeVersionEntity(fieldType, codeVersion);
  }

  public static GitSnapshotEntity generateGitSnapshotEntity(
      String fieldType, GitSnapshot gitSnapshot) {
    return new GitSnapshotEntity(fieldType, gitSnapshot);
  }

  public static DatasetVersionEntity generateDatasetVersionEntity(DatasetVersion datasetVersion) {
    return new DatasetVersionEntity(datasetVersion);
  }

  public static List<DatasetVersion> convertDatasetVersionsFromDatasetVersionEntityList(
      List<DatasetVersionEntity> datasetVersionEntityList) {
    List<DatasetVersion> datasetVersions = new ArrayList<>();
    if (datasetVersionEntityList != null) {
      for (DatasetVersionEntity datasetVersionEntity : datasetVersionEntityList) {
        datasetVersions.add(datasetVersionEntity.getProtoObject());
      }
    }
    return datasetVersions;
  }

  public static RawDatasetVersionInfoEntity generateRawDatasetVersionInfoEntity(
      String fieldType, RawDatasetVersionInfo rawDatasetVersionInfo) {
    return new RawDatasetVersionInfoEntity(fieldType, rawDatasetVersionInfo);
  }

  public static PathDatasetVersionInfoEntity generatePathDatasetVersionInfoEntity(
      String fieldType, PathDatasetVersionInfo pathDatasetVersionInfo) {
    return new PathDatasetVersionInfoEntity(fieldType, pathDatasetVersionInfo);
  }

  public static QueryDatasetVersionInfoEntity generateQueryDatasetVersionInfoEntity(
      String fieldType, QueryDatasetVersionInfo queryDatasetVersionInfo) {
    return new QueryDatasetVersionInfoEntity(fieldType, queryDatasetVersionInfo);
  }

  public static DatasetPartInfoEntity generateDatasetPartInfoEntity(
      Object entity, String fieldType, DatasetPartInfo datasetPartInfo) {
    return new DatasetPartInfoEntity(entity, fieldType, datasetPartInfo);
  }

  public static List<DatasetPartInfo> convertDatasetPartInfoEntityListFromDatasetPartInfos(
      List<DatasetPartInfoEntity> datasetPartInfoEntityList) {
    List<DatasetPartInfo> datasetPartInfos = new ArrayList<>();
    if (datasetPartInfoEntityList != null) {
      return datasetPartInfoEntityList.stream()
          .map(DatasetPartInfoEntity::getProtoObject)
          .collect(Collectors.toList());
    }
    return datasetPartInfos;
  }

  public static List<DatasetPartInfoEntity> convertDatasetPartInfosFromDatasetPartInfoEntityList(
      Object entity, String fieldType, List<DatasetPartInfo> datasetPartInfoList) {
    List<DatasetPartInfoEntity> datasetPartInfoEntityList = new ArrayList<>();
    if (datasetPartInfoList != null) {
      return datasetPartInfoList.stream()
          .map(datasetPartInfo -> generateDatasetPartInfoEntity(entity, fieldType, datasetPartInfo))
          .collect(Collectors.toList());
    }
    return datasetPartInfoEntityList;
  }

  public static QueryParameterEntity generateQueryParameterEntity(
      Object entity, String fieldType, QueryParameter queryParameter) {
    return new QueryParameterEntity(entity, fieldType, queryParameter);
  }

  public static List<QueryParameterEntity> convertQueryParametersFromQueryParameterEntityList(
      Object entity, String fieldType, List<QueryParameter> queryParameterList) {
    List<QueryParameterEntity> queryParameterEntityList = new ArrayList<>();
    if (queryParameterList != null) {
      for (QueryParameter keyValue : queryParameterList) {
        var keyValueEntity = generateQueryParameterEntity(entity, fieldType, keyValue);
        queryParameterEntityList.add(keyValueEntity);
      }
    }
    return queryParameterEntityList;
  }

  public static List<QueryParameter> convertQueryParameterEntityListFromQueryParameters(
      List<QueryParameterEntity> queryParameterEntityList) {
    List<QueryParameter> queryParameterList = new ArrayList<>();
    if (queryParameterEntityList != null) {
      for (QueryParameterEntity queryParameterEntity : queryParameterEntityList) {
        queryParameterList.add(queryParameterEntity.getProtoObject());
      }
    }
    return queryParameterList;
  }

  public static List<UserCommentEntity> convertUserCommentsFromUserCommentEntityList(
      Object entity, List<Comment> userComments) {
    List<UserCommentEntity> userCommentEntityList = new ArrayList<>();
    if (userComments != null) {
      return userComments.stream()
          .map(comment -> new UserCommentEntity(entity, comment))
          .collect(Collectors.toList());
    }
    return userCommentEntityList;
  }

  public static List<Comment> convertUserCommentListFromUserComments(
      List<UserCommentEntity> userCommentEntityList) {
    List<Comment> commentList = new ArrayList<>();
    if (userCommentEntityList != null) {
      return userCommentEntityList.stream()
          .map(UserCommentEntity::getProtoObject)
          .collect(Collectors.toList());
    }
    return commentList;
  }

  public static CommentEntity generateCommentEntity(EntityComment entityComment) {
    return new CommentEntity(entityComment);
  }

  /**
   * Return List of entity data and total count base on the given parameters
   *
   * @param session : hibernate session
   * @param entityName : entity name like ProjectEntity, DatasetEntity etc. Here we used the
   *     hibernate so this entity name is the same name of java class name
   * @param projectionFields : list of string which contains the selected field names
   * @param whereClauseParam : where clause parameter map contains key="entity field name",
   *     value="object[2] where position-1 = operator like EQ,IN etc. position-2 = entity field
   *     values"
   * @param pageNumber : page number for pagination
   * @param pageLimit : page limit for pagination
   * @param order : sort order for sorted list
   * @param sortBy : sort key for sorted list (default sortKey=DATE_UPDATED)
   * @param isNeedTotalCount : if service need the total count for pagination then
   *     'isNeedTotalCount' = true
   * @return {@link Map} : return Map where key=ModelDBConstants.DATA_LIST &
   *     ModelDBConstants.TOTAL_COUNT AND value=list of entity (ex: List<ProjectEntity>),
   *     total_count(Long)
   */
  public static Map<String, Object> findListWithPagination(
      Session session,
      String entityName,
      List<String> projectionFields,
      Map<String, Object[]> whereClauseParam,
      Integer pageNumber,
      Integer pageLimit,
      Boolean order,
      String sortBy,
      Boolean isNeedTotalCount) {
    var alias = "entity";
    var finalQueryBuilder = new StringBuilder();
    StringBuilder countQueryBuilder =
        new StringBuilder("SELECT COUNT (")
            .append(alias)
            .append(".")
            .append(ModelDBConstants.ID)
            .append(") ");

    if (projectionFields != null && !projectionFields.isEmpty()) {
      finalQueryBuilder.append("SELECT ");
      var index = 1;
      for (String selectedField : projectionFields) {
        finalQueryBuilder.append(alias).append(".");
        finalQueryBuilder.append(selectedField);
        if (index < projectionFields.size()) {
          finalQueryBuilder.append(", ");
          index++;
        }
      }
      finalQueryBuilder.append(" ");
    }

    finalQueryBuilder.append("FROM ").append(entityName).append(" ").append(alias).append(" ");
    countQueryBuilder.append("FROM ").append(entityName).append(" ").append(alias).append(" ");

    if (whereClauseParam != null && whereClauseParam.size() > 0) {
      finalQueryBuilder.append(" WHERE ");
      countQueryBuilder.append(" WHERE ");
      var index = 1;
      for (Map.Entry<String, Object[]> entityFieldEntry : whereClauseParam.entrySet()) {
        Object[] operatorWithValueArr = entityFieldEntry.getValue();
        finalQueryBuilder.append(" ").append(alias).append(".").append(entityFieldEntry.getKey());
        countQueryBuilder.append(" ").append(alias).append(".").append(entityFieldEntry.getKey());
        finalQueryBuilder
            .append(" ")
            .append(operatorWithValueArr[0])
            .append(" :")
            .append(entityFieldEntry.getKey());
        countQueryBuilder
            .append(" ")
            .append(operatorWithValueArr[0])
            .append(" :")
            .append(entityFieldEntry.getKey());

        if (index < whereClauseParam.size()) {
          finalQueryBuilder.append(AND_QUERY_OPERATOR);
          countQueryBuilder.append(AND_QUERY_OPERATOR);
          index++;
        }
      }
    }

    finalQueryBuilder
        .append(AND_QUERY_OPERATOR)
        .append(alias)
        .append(".")
        .append(ModelDBConstants.DELETED)
        .append(" = false ");
    countQueryBuilder
        .append(AND_QUERY_OPERATOR)
        .append(alias)
        .append(".")
        .append(ModelDBConstants.DELETED)
        .append(" = false ");

    sortBy = (sortBy == null || sortBy.isEmpty()) ? ModelDBConstants.DATE_UPDATED : sortBy;

    if (order) {
      finalQueryBuilder
          .append(" ORDER BY ")
          .append(alias)
          .append(".")
          .append(sortBy)
          .append(" ")
          .append(ModelDBConstants.ORDER_ASC);
    } else {
      finalQueryBuilder
          .append(" ORDER BY ")
          .append(alias)
          .append(".")
          .append(sortBy)
          .append(" ")
          .append(ModelDBConstants.ORDER_DESC);
    }

    var query = session.createQuery(finalQueryBuilder.toString());
    if (pageNumber != null && pageLimit != null && pageNumber != 0 && pageLimit != 0) {
      // Calculate number of documents to skip
      int skips = pageLimit * (pageNumber - 1);
      query.setFirstResult(skips);
      query.setMaxResults(pageLimit);
    }

    if (whereClauseParam != null && whereClauseParam.size() > 0) {
      for (Map.Entry<String, Object[]> entityFieldEntry : whereClauseParam.entrySet()) {
        Object[] operatorWithValueArr = entityFieldEntry.getValue();
        if (operatorWithValueArr[1] instanceof List) {
          List<?> objectList = (List<?>) operatorWithValueArr[1];
          query.setParameterList(entityFieldEntry.getKey(), objectList);
        } else {
          query.setParameter(entityFieldEntry.getKey(), operatorWithValueArr[1]);
        }
      }
    }

    var entityList = query.list();

    Map<String, Object> dataWithCountMap = new HashMap<>();
    dataWithCountMap.put(ModelDBConstants.DATA_LIST, entityList);

    if (isNeedTotalCount) {
      var countQuery = session.createQuery(countQueryBuilder.toString());
      if (whereClauseParam != null && whereClauseParam.size() > 0) {
        for (Map.Entry<String, Object[]> entityFieldEntry : whereClauseParam.entrySet()) {
          Object[] operatorWithValueArr = entityFieldEntry.getValue();
          if (operatorWithValueArr[1] instanceof List) {
            List<?> objectList = (List<?>) operatorWithValueArr[1];
            countQuery.setParameterList(entityFieldEntry.getKey(), objectList);
          } else {
            countQuery.setParameter(entityFieldEntry.getKey(), operatorWithValueArr[1]);
          }
        }
      }
      Long countResult = (Long) countQuery.uniqueResult();
      dataWithCountMap.put(ModelDBConstants.TOTAL_COUNT, countResult);
    }
    LOGGER.debug(entityName + " getting successfully, list size : {}", entityList.size());
    return dataWithCountMap;
  }

  public static String getRdbOperatorSymbol(Operator operator) {
    switch (operator) {
      case EQ:
        return " = ";
      case NE:
        return " <> ";
      case GT:
        return " > ";
      case GTE:
        return " >= ";
      case LT:
        return " < ";
      case LTE:
        return " <= ";
      case CONTAIN:
        return " LIKE ";
      case NOT_CONTAIN:
        return " NOT LIKE ";
      default:
        return " = ";
    }
  }

  /**
   * Return the where clause predicate based on given parameters
   *
   * @param builder : Hibernate criteria builder
   * @param valueExpression : field value path for criteria query
   * @param operator : operator like EQ, GTE, LE etc.
   * @param operatorValue : field value which set to where clause based on field, Operator and this
   *     value
   * @return {@link Predicate} : return predicate (where clause condition) for criteria query
   */
  private static Predicate getOperatorPredicate(
      CriteriaBuilder builder,
      Expression valueExpression,
      Operator operator,
      Object operatorValue) {
    switch (operator.ordinal()) {
      case Operator.GT_VALUE:
        return builder.greaterThan(valueExpression, (Comparable) operatorValue);
      case Operator.GTE_VALUE:
        return builder.greaterThanOrEqualTo(valueExpression, (Comparable) operatorValue);
      case Operator.LT_VALUE:
        return builder.lessThan(valueExpression, (Comparable) operatorValue);
      case Operator.LTE_VALUE:
        return builder.lessThanOrEqualTo(valueExpression, (Comparable) operatorValue);
      case Operator.NE_VALUE:
        return builder.notEqual(valueExpression, operatorValue);
      case Operator.CONTAIN_VALUE:
        return builder.like(
            builder.lower(valueExpression),
            ("%" + Pattern.compile((String) operatorValue) + "%").toLowerCase());
      case Operator.NOT_CONTAIN_VALUE:
        return builder.notLike(
            builder.lower(valueExpression),
            ("%" + Pattern.compile((String) operatorValue) + "%").toLowerCase());
      case Operator.IN_VALUE:
        return valueExpression.in(operatorValue);
      default:
        return builder.equal(valueExpression, operatorValue);
    }
  }

  /**
   * @param builder : Hibernate criteria builder
   * @param fieldName : field name which is set in entity like project.attributes, dataset.parent_id
   *     etc.
   * @param valueExpression : criteria query path for field value to set on query predicate
   * @param keyValueQuery : field contain the keyValue and operator for query which is set by
   *     frontend
   * @return {@link Predicate} : return predicate (where clause condition) for criteria query
   */
  public static Predicate getValuePredicate(
      CriteriaBuilder builder,
      String fieldName,
      Path valueExpression,
      KeyValueQuery keyValueQuery,
      boolean stringColumn) {

    var value = keyValueQuery.getValue();
    var operator = keyValueQuery.getOperator();
    switch (value.getKindCase()) {
      case NUMBER_VALUE:
        LOGGER.debug("Called switch case : number_value");
        //        return getOperatorPredicate(
        //            builder,
        //            builder.function("CAST", BigDecimal.class, valueExpression,
        //            		builder.function("DECIMAL", BigDecimal.class,
        // builder.literal(10),builder.literal(10))),
        //            operator, value.getNumberValue());
        if (App.getInstance().mdbConfig.getDatabase().getRdbConfiguration().isPostgres()) {
          if (stringColumn) {

            return getOperatorPredicate(
                builder,
                builder
                    .trim(Trimspec.BOTH, Character.valueOf('"'), valueExpression)
                    .as(Double.class),
                operator,
                value.getNumberValue());
          } else {
            return getOperatorPredicate(
                builder, valueExpression.as(Double.class), operator, value.getNumberValue());
          }
        } else {
          return getOperatorPredicate(
              builder, builder.toBigDecimal(valueExpression), operator, value.getNumberValue());
        }
      case STRING_VALUE:
        LOGGER.debug("Called switch case : string_value");
        if (!value.getStringValue().isEmpty()) {
          LOGGER.debug("Called switch case : string value exist");
          if (fieldName.equals(ModelDBConstants.ATTRIBUTES)
              && !(operator.equals(Operator.CONTAIN) || operator.equals(Operator.NOT_CONTAIN))) {
            return getOperatorPredicate(
                builder, valueExpression, operator, ModelDBUtils.getStringFromProtoObject(value));
          } else if (keyValueQuery.getKey().equals(ModelDBConstants.VISIBILITY)) {
            return getOperatorPredicate(
                builder,
                valueExpression,
                operator,
                ResourceVisibility.valueOf(value.getStringValue()).ordinal());
          } else {
            return getOperatorPredicate(builder, valueExpression, operator, value.getStringValue());
          }
        } else {
          throw new InvalidArgumentException("Predicate does not contain string value in request");
        }
      case BOOL_VALUE:
        LOGGER.debug("Called switch case : bool_value");
        return getOperatorPredicate(builder, valueExpression, operator, value.getBoolValue());
      case LIST_VALUE:
        List<Object> valueList = new ArrayList<>();
        var listValue = value.getListValue();
        for (Value item : listValue.getValuesList()) {
          if (item.getKindCase().ordinal() == Value.KindCase.STRING_VALUE.ordinal()) {
            var stringValue = item.getStringValue();
            valueList.add(stringValue);
          } else if (item.getKindCase().ordinal() == Value.KindCase.NUMBER_VALUE.ordinal()) {
            Double doubleValue = item.getNumberValue();
            valueList.add(doubleValue);
          }
        }
        return getOperatorPredicate(builder, valueExpression, operator, valueList);
      default:
        throw new UnimplementedException(
            "Unknown 'Value' type recognized, valid 'Value' type are NUMBER_VALUE, STRING_VALUE, BOOL_VALUE");
    }
  }

  /**
   * Method add the where clause base on the sort key switch cases and create the condition and add
   * it in final where clause predicate list and create the hibernate criteria Order for query
   *
   * @param sortBy : sort key for sorted list (default sortKey=DATE_UPDATED)
   * @param isAscending : sort order for sorted list
   * @param builder : Hibernate criteria builder
   * @param root : entity root which is further used for getting sub filed path from it and set in
   *     criteria where clause. Ex: Root<DatasetEntity> datasetRoot =
   *     criteriaQuery.from(DatasetEntity.class);
   * @param parentFieldName : entity has multi level field hierarchy so here mention the parent
   *     field name like attributes, artifacts, tags, metrics etc.
   * @return {@link Order} : return hibernate order base on the parameters
   */
  public static Order getOrderBasedOnSortKey(
      String sortBy,
      Boolean isAscending,
      CriteriaBuilder builder,
      Root<?> root,
      String parentFieldName) {
    if (sortBy == null || sortBy.isEmpty()) {
      sortBy = ModelDBConstants.DATE_UPDATED;
    }

    Order orderBy;

    String[] keys = sortBy.split("\\.");
    root.get(ModelDBConstants.ID);
    Expression<?> orderByExpression;
    switch (keys[0]) {
      case ModelDBConstants.ARTIFACTS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_ARTIFACTS_DEBUG);
        Join<ExperimentRunEntity, ArtifactEntity> artifactEntityJoin =
            root.join(ModelDBConstants.ARTIFACT_MAPPING, JoinType.LEFT);
        artifactEntityJoin.alias(parentFieldName + "_art");
        artifactEntityJoin.on(
            builder.and(
                builder.equal(
                    artifactEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    artifactEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                    ModelDBConstants.ARTIFACTS),
                builder.equal(
                    artifactEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpression = artifactEntityJoin.get(ModelDBConstants.PATH);
        break;
      case ModelDBConstants.DATASETS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_DATASETS_DEBUG);
        Join<ExperimentRunEntity, ArtifactEntity> datasetEntityJoin =
            root.join(ModelDBConstants.ARTIFACT_MAPPING, JoinType.LEFT);
        datasetEntityJoin.alias(parentFieldName + "_dts");
        datasetEntityJoin.on(
            builder.and(
                builder.equal(
                    datasetEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    datasetEntityJoin.get(ModelDBConstants.FEILD_TYPE), ModelDBConstants.DATASETS),
                builder.equal(datasetEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpression = datasetEntityJoin.get(ModelDBConstants.PATH);
        break;
      case ModelDBConstants.ATTRIBUTES:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_ATTRIBUTES_DEBUG);
        Join<ExperimentRunEntity, AttributeEntity> attributeEntityJoin =
            root.join(ModelDBConstants.ATTRIBUTE_MAPPING, JoinType.LEFT);
        attributeEntityJoin.alias(parentFieldName + "_attr");
        attributeEntityJoin.on(
            builder.and(
                builder.equal(
                    attributeEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    attributeEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                    ModelDBConstants.ATTRIBUTES),
                builder.equal(
                    attributeEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpression = attributeEntityJoin.get(ModelDBConstants.VALUE);
        break;
      case ModelDBConstants.HYPERPARAMETERS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_HYPERPARAMETERS_DEBUG);
        Join<ExperimentRunEntity, KeyValueEntity> hyperparameterEntityJoin =
            root.join(ModelDBConstants.KEY_VALUE_MAPPING, JoinType.LEFT);
        hyperparameterEntityJoin.alias(parentFieldName + "_hypr");
        hyperparameterEntityJoin.on(
            builder.and(
                builder.equal(
                    hyperparameterEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    hyperparameterEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                    ModelDBConstants.HYPERPARAMETERS),
                builder.equal(
                    hyperparameterEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpression = hyperparameterEntityJoin.get(ModelDBConstants.VALUE);
        break;
      case ModelDBConstants.METRICS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_METRICS_DEBUG);
        Join<ExperimentRunEntity, KeyValueEntity> metricsEntityJoin =
            root.join(ModelDBConstants.KEY_VALUE_MAPPING, JoinType.LEFT);
        metricsEntityJoin.alias(parentFieldName + "_mtr");
        metricsEntityJoin.on(
            builder.and(
                builder.equal(
                    metricsEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    metricsEntityJoin.get(ModelDBConstants.FEILD_TYPE), ModelDBConstants.METRICS),
                builder.equal(metricsEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpression = metricsEntityJoin.get(ModelDBConstants.VALUE);
        break;
      case ModelDBConstants.OBSERVATIONS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_OBSERVATION_DEBUG);
        if (keys.length > 2) {
          // If getting third level key like observation.attribute.attr_1 then it is not supported
          throw new InvalidArgumentException("Third level of sorting not supported");
          /*TODO: Below code for supporting the third level (ex: experimentRun.attributes.att_1) ordering data but right now Mongo doesn't support the third level ordering so commented below code to maintain the functionality.
          switch (keys[1]) {
              case ModelDBConstants.ATTRIBUTES:
                  LOGGER.debug("switch case : Observation --> Attributes");
                  String objAttrParentFieldName = "observationEntity";
                  Path obsAttrExpression = criteriaQuery.from(KeyValueEntity.class);
                  obsAttrExpression.alias(objAttrParentFieldName + "_kv");
                  finalPredicatesList.addAll(
                          getKeyValueTypePredicates(
                                  obserExpression,
                                  objAttrParentFieldName,
                                  obsAttrExpression,
                                  builder,
                                  ModelDBConstants.ATTRIBUTES,
                                  keys[keys.length - 1],
                                  null));
                  orderByExpression = obsAttrExpression.get(ModelDBConstants.VALUE);
                  break;
              case ModelDBConstants.ARTIFACTS:
                  LOGGER.debug("switch case : Observation --> Artifact");
                  String objArtParentFieldName = "observationEntity";
                  Path obrArtExpression = criteriaQuery.from(ArtifactEntity.class);
                  obrArtExpression.alias(objArtParentFieldName + "_art");
                  finalPredicatesList.addAll(
                          getKeyValueTypePredicates(
                                  obserExpression,
                                  objArtParentFieldName,
                                  obrArtExpression,
                                  builder,
                                  ModelDBConstants.ARTIFACTS,
                                  keys[keys.length - 1],
                                  null));
                  orderByExpression = obrArtExpression.get(ModelDBConstants.PATH);
                  break;

              default:
                  break;
          }*/
        } else {
          Join<ExperimentRunEntity, ObservationEntity> observationEntityJoin =
              root.join(ModelDBConstants.OBSERVATION_MAPPING, JoinType.LEFT);
          observationEntityJoin.alias(parentFieldName + "_obser");
          observationEntityJoin.on(
              builder.and(
                  builder.equal(
                      observationEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                      root.get(ModelDBConstants.ID)),
                  builder.equal(
                      observationEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                      ModelDBConstants.OBSERVATIONS),
                  builder.equal(
                      observationEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

          orderByExpression = observationEntityJoin.get(keys[1]);
        }
        break;
      case ModelDBConstants.FEATURES:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_FEATURE_DEBUG);
        Join<ExperimentRunEntity, FeatureEntity> featureEntityJoin =
            root.join(ModelDBConstants.FEATURES, JoinType.LEFT);
        featureEntityJoin.alias(parentFieldName + "_feature");
        featureEntityJoin.on(
            builder.and(
                builder.equal(
                    featureEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID))));

        orderByExpression = featureEntityJoin.get(ModelDBConstants.NAME);
        break;
      case ModelDBConstants.TAGS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_TAGS_DEBUG);
        Join<ExperimentRunEntity, TagsMapping> tagsEntityJoin =
            root.join(ModelDBConstants.TAGS, JoinType.LEFT);
        tagsEntityJoin.alias(parentFieldName + "_tags");
        tagsEntityJoin.on(
            builder.and(
                builder.equal(
                    tagsEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID))));

        orderByExpression = tagsEntityJoin.get(ModelDBConstants.TAGS);
        break;
      default:
        orderByExpression = root.get(sortBy);
    }
    orderBy = isAscending ? builder.asc(orderByExpression) : builder.desc(orderByExpression);

    return orderBy;
  }

  /**
   * Method add the where clause base on the sort key switch cases and create the condition and add
   * it in final where clause predicate list and create the hibernate criteria Order for query
   *
   * @param sortBy : sort key for sorted list (default sortKey=DATE_UPDATED)
   * @param isAscending : sort order for sorted list
   * @param builder : Hibernate criteria builder
   * @param root : entity root which is further used for getting sub filed path from it and set in
   *     criteria where clause. Ex: Root<DatasetEntity> datasetRoot =
   *     criteriaQuery.from(DatasetEntity.class);
   * @param parentFieldName : entity has multi level field hierarchy so here mention the parent
   *     field name like attributes, artifacts, tags, metrics etc.
   * @return {@link Order} : return hibernate order base on the parameters
   */
  public static Order[] getOrderArrBasedOnSortKey(
      String sortBy,
      Boolean isAscending,
      CriteriaBuilder builder,
      Root<?> root,
      String parentFieldName) {
    if (sortBy == null || sortBy.isEmpty()) {
      sortBy = ModelDBConstants.DATE_UPDATED;
    }

    String[] keys = sortBy.split("\\.");
    root.get(ModelDBConstants.ID);
    List<Expression<?>> orderByExpressionList = new ArrayList<>();
    switch (keys[0]) {
      case ModelDBConstants.ARTIFACTS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_ARTIFACTS_DEBUG);
        Join<ExperimentRunEntity, ArtifactEntity> artifactEntityJoin =
            root.join(ModelDBConstants.ARTIFACT_MAPPING, JoinType.LEFT);
        artifactEntityJoin.alias(parentFieldName + "_art");
        artifactEntityJoin.on(
            builder.and(
                builder.equal(
                    artifactEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    artifactEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                    ModelDBConstants.ARTIFACTS),
                builder.equal(
                    artifactEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpressionList.add(artifactEntityJoin.get(ModelDBConstants.PATH));
        break;
      case ModelDBConstants.DATASETS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_DATASETS_DEBUG);
        Join<ExperimentRunEntity, ArtifactEntity> datasetEntityJoin =
            root.join(ModelDBConstants.ARTIFACT_MAPPING, JoinType.LEFT);
        datasetEntityJoin.alias(parentFieldName + "_dts");
        datasetEntityJoin.on(
            builder.and(
                builder.equal(
                    datasetEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    datasetEntityJoin.get(ModelDBConstants.FEILD_TYPE), ModelDBConstants.DATASETS),
                builder.equal(datasetEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpressionList.add(datasetEntityJoin.get(ModelDBConstants.PATH));
        break;
      case ModelDBConstants.ATTRIBUTES:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_ATTRIBUTES_DEBUG);
        Join<ExperimentRunEntity, AttributeEntity> attributeEntityJoin =
            root.join(ModelDBConstants.ATTRIBUTE_MAPPING, JoinType.LEFT);
        attributeEntityJoin.alias(parentFieldName + "_attr");
        attributeEntityJoin.on(
            builder.and(
                builder.equal(
                    attributeEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    attributeEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                    ModelDBConstants.ATTRIBUTES),
                builder.equal(
                    attributeEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpressionList.add(attributeEntityJoin.get(ModelDBConstants.VALUE));
        break;
      case ModelDBConstants.HYPERPARAMETERS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_HYPERPARAMETERS_DEBUG);
        Join<ExperimentRunEntity, KeyValueEntity> hyperparameterEntityJoin =
            root.join(ModelDBConstants.KEY_VALUE_MAPPING, JoinType.LEFT);
        hyperparameterEntityJoin.alias(parentFieldName + "_hypr");
        hyperparameterEntityJoin.on(
            builder.and(
                builder.equal(
                    hyperparameterEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    hyperparameterEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                    ModelDBConstants.HYPERPARAMETERS),
                builder.equal(
                    hyperparameterEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpressionList.add(hyperparameterEntityJoin.get(ModelDBConstants.VALUE));

        if (parentFieldName.equals("experimentRunEntity")) {
          Join<ExperimentRunEntity, HyperparameterElementMappingEntity> elementMappingEntityJoin =
              root.join(ModelDBConstants.HYPERPARAMETER_ELEMENT_MAPPINGS, JoinType.LEFT);
          elementMappingEntityJoin.alias(parentFieldName + "_hyper_elem_mapping");
          elementMappingEntityJoin.on(
              builder.and(
                  builder.equal(
                      elementMappingEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                      root.get(ModelDBConstants.ID))),
              builder.equal(
                  elementMappingEntityJoin.get(ModelDBConstants.NAME), keys[keys.length - 1]));

          orderByExpressionList.add(elementMappingEntityJoin.get("int_value"));
          orderByExpressionList.add(elementMappingEntityJoin.get("float_value"));
          orderByExpressionList.add(elementMappingEntityJoin.get("string_value"));
        }
        break;
      case ModelDBConstants.METRICS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_METRICS_DEBUG);
        Join<ExperimentRunEntity, KeyValueEntity> metricsEntityJoin =
            root.join(ModelDBConstants.KEY_VALUE_MAPPING, JoinType.LEFT);
        metricsEntityJoin.alias(parentFieldName + "_mtr");
        metricsEntityJoin.on(
            builder.and(
                builder.equal(
                    metricsEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID)),
                builder.equal(
                    metricsEntityJoin.get(ModelDBConstants.FEILD_TYPE), ModelDBConstants.METRICS),
                builder.equal(metricsEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

        orderByExpressionList.add(metricsEntityJoin.get(ModelDBConstants.VALUE));
        break;
      case ModelDBConstants.OBSERVATIONS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_OBSERVATION_DEBUG);
        if (keys.length > 2) {
          // If getting third level key like observation.attribute.attr_1 then it is not supported
          throw new InvalidArgumentException("Third level of sorting not supported");
          /*TODO: Below code for supporting the third level (ex: experimentRun.attributes.att_1) ordering data but right now Mongo doesn't support the third level ordering so commented below code to maintain the functionality.
          switch (keys[1]) {
              case ModelDBConstants.ATTRIBUTES:
                  LOGGER.debug("switch case : Observation --> Attributes");
                  String objAttrParentFieldName = "observationEntity";
                  Path obsAttrExpression = criteriaQuery.from(KeyValueEntity.class);
                  obsAttrExpression.alias(objAttrParentFieldName + "_kv");
                  finalPredicatesList.addAll(
                          getKeyValueTypePredicates(
                                  obserExpression,
                                  objAttrParentFieldName,
                                  obsAttrExpression,
                                  builder,
                                  ModelDBConstants.ATTRIBUTES,
                                  keys[keys.length - 1],
                                  null));
                  orderByExpression = obsAttrExpression.get(ModelDBConstants.VALUE);
                  break;
              case ModelDBConstants.ARTIFACTS:
                  LOGGER.debug("switch case : Observation --> Artifact");
                  String objArtParentFieldName = "observationEntity";
                  Path obrArtExpression = criteriaQuery.from(ArtifactEntity.class);
                  obrArtExpression.alias(objArtParentFieldName + "_art");
                  finalPredicatesList.addAll(
                          getKeyValueTypePredicates(
                                  obserExpression,
                                  objArtParentFieldName,
                                  obrArtExpression,
                                  builder,
                                  ModelDBConstants.ARTIFACTS,
                                  keys[keys.length - 1],
                                  null));
                  orderByExpression = obrArtExpression.get(ModelDBConstants.PATH);
                  break;

              default:
                  break;
          }*/
        } else {
          Join<ExperimentRunEntity, ObservationEntity> observationEntityJoin =
              root.join(ModelDBConstants.OBSERVATION_MAPPING, JoinType.LEFT);
          observationEntityJoin.alias(parentFieldName + "_obser");
          observationEntityJoin.on(
              builder.and(
                  builder.equal(
                      observationEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                      root.get(ModelDBConstants.ID)),
                  builder.equal(
                      observationEntityJoin.get(ModelDBConstants.FEILD_TYPE),
                      ModelDBConstants.OBSERVATIONS),
                  builder.equal(
                      observationEntityJoin.get(ModelDBConstants.KEY), keys[keys.length - 1])));

          orderByExpressionList.add(observationEntityJoin.get(keys[1]));
        }
        break;
      case ModelDBConstants.FEATURES:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_FEATURE_DEBUG);
        Join<ExperimentRunEntity, FeatureEntity> featureEntityJoin =
            root.join(ModelDBConstants.FEATURES, JoinType.LEFT);
        featureEntityJoin.alias(parentFieldName + "_feature");
        featureEntityJoin.on(
            builder.and(
                builder.equal(
                    featureEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID))));

        orderByExpressionList.add(featureEntityJoin.get(ModelDBConstants.NAME));
        break;
      case ModelDBConstants.TAGS:
        LOGGER.debug(ModelDBMessages.SWITCH_CASE_TAGS_DEBUG);
        Join<ExperimentRunEntity, TagsMapping> tagsEntityJoin =
            root.join(ModelDBConstants.TAGS, JoinType.LEFT);
        tagsEntityJoin.alias(parentFieldName + "_tags");
        tagsEntityJoin.on(
            builder.and(
                builder.equal(
                    tagsEntityJoin.get(parentFieldName).get(ModelDBConstants.ID),
                    root.get(ModelDBConstants.ID))));

        orderByExpressionList.add(tagsEntityJoin.get(ModelDBConstants.TAGS));
        break;
      default:
        orderByExpressionList.add(root.get(sortBy));
    }
    var orderByArr = new Order[orderByExpressionList.size()];
    for (var index = 0; index < orderByExpressionList.size(); index++) {
      Expression<?> orderByExpression = orderByExpressionList.get(index);
      orderByArr[index] =
          isAscending ? builder.asc(orderByExpression) : builder.desc(orderByExpression);
    }

    return orderByArr;
  }

  /**
   * @param expression : entity has multi level field this field name from parent root path
   * @param builder : Hibernate criteria builder
   * @param fieldType : For example, artifact has the single table but type is different like
   *     dataset(artifact), artifact(artifact) etc.
   * @param key : artifact.key
   * @param predicate : field contain the keyValue and operator for query which is set by frontend
   * @return {@link List<Predicate>} : which contain the where clause condition(predicate) created
   *     from KeyValueQuery base on artifact
   */
  private static List<Predicate> getArtifactTypePredicates(
      Path expression,
      CriteriaBuilder builder,
      String fieldType,
      String key,
      KeyValueQuery predicate) {
    List<Predicate> fieldPredicates = new ArrayList<>();
    var fieldTypePredicate = builder.equal(expression.get(ModelDBConstants.FEILD_TYPE), fieldType);
    fieldPredicates.add(fieldTypePredicate);

    Path valueExpression;
    if (key.equals(ModelDBConstants.LINKED_ARTIFACT_ID)) {
      valueExpression = expression.get(key);
    } else {
      var keyPredicate = builder.equal(expression.get(ModelDBConstants.KEY), key);
      fieldPredicates.add(keyPredicate);
      valueExpression = expression.get(ModelDBConstants.PATH);
    }

    if (predicate != null) {
      var valuePredicate =
          getValuePredicate(builder, ModelDBConstants.ARTIFACTS, valueExpression, predicate, true);
      fieldPredicates.add(valuePredicate);
    }

    return fieldPredicates;
  }

  /**
   * @param expression : entity has multi level field this field name from parent root path
   * @param builder : Hibernate criteria builder
   * @param fieldType : For example, attribute has the single table but type is different like
   *     metrics(attribute), attribute(attribute), hyperparameter(attribute) etc.
   * @param key : attribute.key
   * @param predicate : field contain the keyValue and operator for query which is set by frontend
   * @return {@link List<Predicate>} : which contain the where clause condition(predicate) created
   *     from KeyValueQuery base on attributes
   */
  private static List<Predicate> getKeyValueTypePredicates(
      Path expression,
      CriteriaBuilder builder,
      String fieldType,
      String key,
      KeyValueQuery predicate) {
    List<Predicate> fieldPredicates = new ArrayList<>();
    var fieldTypePredicate = builder.equal(expression.get(ModelDBConstants.FEILD_TYPE), fieldType);
    fieldPredicates.add(fieldTypePredicate);
    var keyPredicate = builder.equal(expression.get(ModelDBConstants.KEY), key);
    fieldPredicates.add(keyPredicate);

    if (predicate != null) {
      var valuePredicate =
          getValuePredicate(
              builder,
              ModelDBConstants.ATTRIBUTES,
              expression.get(ModelDBConstants.VALUE),
              predicate,
              true);
      fieldPredicates.add(valuePredicate);
    }

    return fieldPredicates;
  }

  /**
   * Return the data count base on the criteria query
   *
   * @param session : hibernate session
   * @param root : entity root which is further used for getting sub filed path from it and set in
   *     criteria where clause. Ex: Root<ProjectEntity> projectRoot =
   *     criteriaQuery.from(ProjectEntity.class);
   * @param criteria : Hibernate criteria query reference for further process
   * @param <T> : T = entity name like ProjectEntity, DatasetEntity, ExperimentEntity etc.
   * @return {@link Long} : total records count
   */
  public static <T> long count(Session session, Root<T> root, CriteriaQuery<T> criteria) {
    final var builder = session.getCriteriaBuilder();
    final CriteriaQuery<Long> countCriteria = builder.createQuery(Long.class);

    countCriteria.select(builder.count(root));
    countCriteria.getRoots().addAll(criteria.getRoots());

    final Predicate whereRestriction = criteria.getRestriction();
    if (whereRestriction != null) {
      countCriteria.where(whereRestriction);
    }

    final Predicate groupRestriction = criteria.getGroupRestriction();
    if (groupRestriction != null) {
      countCriteria.having(groupRestriction);
    }

    countCriteria.groupBy(criteria.getGroupList());
    countCriteria.distinct(criteria.isDistinct());
    return session.createQuery(countCriteria).getSingleResult();
  }

  /**
   * @param entityName : entity name like ProjectEntity, DatasetEntity etc. Here we used the
   *     hibernate so this entity name is the same name of java class name
   * @param predicates : field contain the keyValue and operator for query which is set by frontend
   * @param builder : Hibernate criteria builder
   * @param criteriaQuery : Hibernate criteria query reference for further process
   * @param entityRootPath : entity root which is further used for getting sub filed path from it
   *     and set in criteria where clause. Ex: Root<ProjectEntity> projectRoot =
   *     criteriaQuery.from(ProjectEntity.class);
   * @return {@link List<Predicate>} : list for where clause condition for criteria query
   */
  public static List<Predicate> getQueryPredicatesFromPredicateList(
      String entityName,
      List<KeyValueQuery> predicates,
      CriteriaBuilder builder,
      CriteriaQuery<?> criteriaQuery,
      Root<?> entityRootPath,
      AuthService authService,
      MDBRoleService mdbRoleService,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes)
      throws ModelDBException {
    List<Predicate> finalPredicatesList = new ArrayList<>();
    if (!predicates.isEmpty()) {
      List<Predicate> keyValuePredicates = new ArrayList<>();
      for (var index = 0; index < predicates.size(); index++) {
        KeyValueQuery predicate = predicates.get(index);
        try {
          String errorMessage = null;
          String key = predicate.getKey();
          if (key.isEmpty()) {
            errorMessage = "Key not found in predicate";
          } else if (!key.contains(ModelDBConstants.LINKED_ARTIFACT_ID)
              && predicate.getOperator().equals(Operator.IN)) {
            errorMessage = "Operator `IN` supported only with the linked_artifact_id as a key";
          }

          if (errorMessage != null) {
            throw new InvalidArgumentException(errorMessage);
          }

          LOGGER.debug("Set predicate : \n{}", predicate);
          Path expression;
          String[] names = key.split("\\.");

          var operator = predicate.getOperator();
          if (operator.equals(Operator.NOT_CONTAIN)) {
            predicate = predicate.toBuilder().setOperator(Operator.CONTAIN).build();
          } else if (operator.equals(Operator.NE)) {
            predicate = predicate.toBuilder().setOperator(Operator.EQ).build();
          }
          Subquery<String> subquery = criteriaQuery.subquery(String.class);

          Expression<String> parentPathFromChild;
          switch (names[0]) {
            case ModelDBConstants.ARTIFACTS:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_ARTIFACTS_DEBUG);
              Root<ArtifactEntity> artifactEntityRoot = subquery.from(ArtifactEntity.class);
              artifactEntityRoot.alias(entityName + "_" + ModelDBConstants.ARTIFACT_ALIAS + index);
              List<Predicate> artifactValuePredicates =
                  RdbmsUtils.getArtifactTypePredicates(
                      artifactEntityRoot,
                      builder,
                      ModelDBConstants.ARTIFACTS,
                      names[names.length - 1],
                      predicate);

              parentPathFromChild = artifactEntityRoot.get(entityName).get(ModelDBConstants.ID);
              subquery.select(parentPathFromChild);
              artifactValuePredicates.add(builder.isNotNull(parentPathFromChild));

              var artifactPredicatesOne = new Predicate[artifactValuePredicates.size()];
              for (var indexJ = 0; indexJ < artifactValuePredicates.size(); indexJ++) {
                artifactPredicatesOne[indexJ] = artifactValuePredicates.get(indexJ);
              }
              subquery.where(builder.and(artifactPredicatesOne));
              keyValuePredicates.add(
                  getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              break;
            case ModelDBConstants.DATASETS:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_DATASETS_DEBUG);
              Root<ArtifactEntity> datasetEntityRoot = subquery.from(ArtifactEntity.class);
              datasetEntityRoot.alias(entityName + "_" + ModelDBConstants.DATASET_ALIAS + index);
              List<Predicate> datasetValuePredicates =
                  RdbmsUtils.getArtifactTypePredicates(
                      datasetEntityRoot,
                      builder,
                      ModelDBConstants.DATASETS,
                      names[names.length - 1],
                      predicate);

              parentPathFromChild = datasetEntityRoot.get(entityName).get(ModelDBConstants.ID);
              subquery.select(parentPathFromChild);
              datasetValuePredicates.add(builder.isNotNull(parentPathFromChild));

              var datasetPredicatesOne = new Predicate[datasetValuePredicates.size()];
              for (var indexJ = 0; indexJ < datasetValuePredicates.size(); indexJ++) {
                datasetPredicatesOne[indexJ] = datasetValuePredicates.get(indexJ);
              }
              subquery.where(builder.and(datasetPredicatesOne));
              keyValuePredicates.add(
                  getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              break;
            case ModelDBConstants.ATTRIBUTES:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_ATTRIBUTES_DEBUG);
              Root<AttributeEntity> attributeEntityRoot = subquery.from(AttributeEntity.class);
              attributeEntityRoot.alias(
                  entityName + "_" + ModelDBConstants.ATTRIBUTE_ALIAS + index);
              List<Predicate> attributeValuePredicates =
                  RdbmsUtils.getKeyValueTypePredicates(
                      attributeEntityRoot,
                      builder,
                      ModelDBConstants.ATTRIBUTES,
                      names[names.length - 1],
                      predicate);

              parentPathFromChild = attributeEntityRoot.get(entityName).get(ModelDBConstants.ID);
              subquery.select(parentPathFromChild);
              attributeValuePredicates.add(builder.isNotNull(parentPathFromChild));

              var attributePredicatesOne = new Predicate[attributeValuePredicates.size()];
              for (var indexJ = 0; indexJ < attributeValuePredicates.size(); indexJ++) {
                attributePredicatesOne[indexJ] = attributeValuePredicates.get(indexJ);
              }
              subquery.where(builder.and(attributePredicatesOne));
              keyValuePredicates.add(
                  getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              break;
            case ModelDBConstants.HYPERPARAMETERS:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_HYPERPARAMETERS_DEBUG);
              Subquery<String> subqueryMDB = criteriaQuery.subquery(String.class);
              Root<KeyValueEntity> hyperparameterEntityRoot =
                  subqueryMDB.from(KeyValueEntity.class);
              hyperparameterEntityRoot.alias(
                  entityName + "_" + ModelDBConstants.HYPERPARAMETER_ALIAS + index);
              List<Predicate> hyperparameterValuePredicates =
                  RdbmsUtils.getKeyValueTypePredicates(
                      hyperparameterEntityRoot,
                      builder,
                      ModelDBConstants.HYPERPARAMETERS,
                      names[names.length - 1],
                      predicate);
              parentPathFromChild =
                  hyperparameterEntityRoot.get(entityName).get(ModelDBConstants.ID);
              subqueryMDB.select(parentPathFromChild);
              hyperparameterValuePredicates.add(builder.isNotNull(parentPathFromChild));

              var hyperparameterPredicatesOne = new Predicate[hyperparameterValuePredicates.size()];
              for (var indexJ = 0; indexJ < hyperparameterValuePredicates.size(); indexJ++) {
                hyperparameterPredicatesOne[indexJ] = hyperparameterValuePredicates.get(indexJ);
              }
              subqueryMDB.where(builder.and(hyperparameterPredicatesOne));
              // This subquery should become one part of union
              var predicatesArr = new Predicate[2];
              var oldHyperparameterPredicate =
                  getPredicateFromSubquery(builder, entityRootPath, operator, subqueryMDB);
              predicatesArr[0] = oldHyperparameterPredicate;
              Subquery<String> subqueryVersion = criteriaQuery.subquery(String.class);
              // Hyperparameter from new design
              var newHyperparameterPredicate =
                  getVersionedInputHyperparameterPredicate(
                      entityName,
                      builder,
                      entityRootPath,
                      index,
                      predicate,
                      names,
                      operator,
                      subqueryVersion);
              predicatesArr[1] = newHyperparameterPredicate;
              if (operator.equals(Operator.NOT_CONTAIN) || operator.equals(Operator.NE)) {
                keyValuePredicates.add(builder.and(predicatesArr));
              } else {
                keyValuePredicates.add(builder.or(predicatesArr));
              }
              break;
            case ModelDBConstants.METRICS:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_METRICS_DEBUG);
              Root<KeyValueEntity> metricsEntityRoot = subquery.from(KeyValueEntity.class);
              metricsEntityRoot.alias(entityName + "_" + ModelDBConstants.METRICS_ALIAS + index);
              List<Predicate> metricsValuePredicates =
                  RdbmsUtils.getKeyValueTypePredicates(
                      metricsEntityRoot,
                      builder,
                      ModelDBConstants.METRICS,
                      names[names.length - 1],
                      predicate);

              parentPathFromChild = metricsEntityRoot.get(entityName).get(ModelDBConstants.ID);
              subquery.select(parentPathFromChild);
              metricsValuePredicates.add(builder.isNotNull(parentPathFromChild));

              var metricsPredicatesOne = new Predicate[metricsValuePredicates.size()];
              for (var indexJ = 0; indexJ < metricsValuePredicates.size(); indexJ++) {
                metricsPredicatesOne[indexJ] = metricsValuePredicates.get(indexJ);
              }
              subquery.where(builder.and(metricsPredicatesOne));
              keyValuePredicates.add(
                  getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              break;
            case ModelDBConstants.OBSERVATIONS:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_OBSERVATION_DEBUG);

              if (names.length > 2) {
                switch (names[1]) {
                  case ModelDBConstants.ATTRIBUTES:
                    LOGGER.debug("switch case : Observation --> Attributes");
                    Root<KeyValueEntity> obrAttrEntityRoot = subquery.from(KeyValueEntity.class);
                    obrAttrEntityRoot.alias(
                        entityName
                            + "_observationEntity_"
                            + ModelDBConstants.ATTRIBUTE_ALIAS
                            + index);
                    List<Predicate> obrAttrValuePredicates =
                        RdbmsUtils.getKeyValueTypePredicates(
                            obrAttrEntityRoot,
                            builder,
                            ModelDBConstants.ATTRIBUTES,
                            names[names.length - 1],
                            predicate);

                    parentPathFromChild =
                        obrAttrEntityRoot.get(entityName).get(ModelDBConstants.ID);
                    subquery.select(parentPathFromChild);
                    obrAttrValuePredicates.add(builder.isNotNull(parentPathFromChild));

                    var obrAttrPredicatesOne = new Predicate[obrAttrValuePredicates.size()];
                    for (var indexJ = 0; indexJ < obrAttrValuePredicates.size(); indexJ++) {
                      obrAttrPredicatesOne[indexJ] = obrAttrValuePredicates.get(indexJ);
                    }
                    subquery.where(builder.and(obrAttrPredicatesOne));
                    keyValuePredicates.add(
                        getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
                    break;
                  case ModelDBConstants.ARTIFACTS:
                    LOGGER.debug("switch case : Observation --> Artifact");
                    Root<ArtifactEntity> obrArtEntityRoot = subquery.from(ArtifactEntity.class);
                    obrArtEntityRoot.alias(
                        entityName
                            + "_observationEntity_"
                            + ModelDBConstants.ARTIFACT_ALIAS
                            + index);
                    List<Predicate> obrArtValuePredicates =
                        RdbmsUtils.getArtifactTypePredicates(
                            obrArtEntityRoot,
                            builder,
                            ModelDBConstants.ARTIFACTS,
                            names[names.length - 1],
                            predicate);

                    parentPathFromChild = obrArtEntityRoot.get(entityName).get(ModelDBConstants.ID);
                    subquery.select(parentPathFromChild);
                    obrArtValuePredicates.add(builder.isNotNull(parentPathFromChild));
                    var obrArtPredicatesOne = new Predicate[obrArtValuePredicates.size()];
                    for (var indexJ = 0; indexJ < obrArtValuePredicates.size(); indexJ++) {
                      obrArtPredicatesOne[indexJ] = obrArtValuePredicates.get(indexJ);
                    }
                    subquery.where(builder.and(obrArtPredicatesOne));
                    keyValuePredicates.add(
                        getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
                    break;

                  default:
                    break;
                }
              } else {
                Root<ObservationEntity> observationEntityRootEntityRoot =
                    subquery.from(ObservationEntity.class);
                observationEntityRootEntityRoot.alias(
                    entityName + "_" + ModelDBConstants.OBSERVATION_ALIAS + index);
                var observationValuePredicate =
                    RdbmsUtils.getValuePredicate(
                        builder,
                        ModelDBConstants.OBSERVATIONS,
                        observationEntityRootEntityRoot.get(names[1]),
                        predicate,
                        true);

                parentPathFromChild =
                    observationEntityRootEntityRoot.get(entityName).get(ModelDBConstants.ID);
                subquery.select(parentPathFromChild);
                subquery.where(
                    builder.and(observationValuePredicate, builder.isNotNull(parentPathFromChild)));
                keyValuePredicates.add(
                    getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              }
              break;
            case ModelDBConstants.FEATURES:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_FEATURE_DEBUG);
              Root<FeatureEntity> featureEntityRoot = subquery.from(FeatureEntity.class);
              featureEntityRoot.alias(entityName + "_" + ModelDBConstants.FEATURE_ALIAS + index);
              var featureValuePredicate =
                  RdbmsUtils.getValuePredicate(
                      builder,
                      ModelDBConstants.FEATURES,
                      featureEntityRoot.get(names[names.length - 1]),
                      predicate,
                      true);

              parentPathFromChild = featureEntityRoot.get(entityName).get(ModelDBConstants.ID);
              subquery.select(parentPathFromChild);
              subquery.where(
                  builder.and(featureValuePredicate, builder.isNotNull(parentPathFromChild)));
              keyValuePredicates.add(
                  getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              break;
            case ModelDBConstants.TAGS:
              LOGGER.debug(ModelDBMessages.SWITCH_CASE_TAGS_DEBUG);
              Predicate tagValuePredicate;
              if (entityName.equals(ModelDBConstants.REPOSITORY_ENTITY)) {
                Subquery<Long> longSubquery = criteriaQuery.subquery(Long.class);
                Root<LabelsMappingEntity> tagsMappingRoot =
                    longSubquery.from(LabelsMappingEntity.class);
                tagsMappingRoot.alias(entityName + "_" + ModelDBConstants.TAGS_ALIAS + index);
                var predicate1 =
                    builder.equal(
                        tagsMappingRoot.get("id").get("entity_type"),
                        IDTypeEnum.IDType.VERSIONING_REPOSITORY_VALUE);
                var predicate2 =
                    RdbmsUtils.getValuePredicate(
                        builder,
                        "id.label",
                        tagsMappingRoot.get("id").get("label"),
                        predicate,
                        true);
                tagValuePredicate = builder.and(predicate1, predicate2);
                parentPathFromChild = tagsMappingRoot.get("id").get("entity_hash");
                longSubquery.select(parentPathFromChild.as(Long.class));
                longSubquery.where(
                    builder.and(tagValuePredicate, builder.isNotNull(parentPathFromChild)));
                Expression<String> exp = entityRootPath.get(ModelDBConstants.ID);
                if (operator.equals(Operator.NOT_CONTAIN) || operator.equals(Operator.NE)) {
                  keyValuePredicates.add(builder.not(exp.in(longSubquery)));
                } else {
                  keyValuePredicates.add(exp.in(longSubquery));
                }
              } else {
                Root<TagsMapping> tagsMappingRoot = subquery.from(TagsMapping.class);
                tagsMappingRoot.alias(entityName + "_" + ModelDBConstants.TAGS_ALIAS + index);
                tagValuePredicate =
                    RdbmsUtils.getValuePredicate(
                        builder,
                        ModelDBConstants.TAGS,
                        tagsMappingRoot.get(ModelDBConstants.TAGS),
                        predicate,
                        true);

                parentPathFromChild = tagsMappingRoot.get(entityName).get(ModelDBConstants.ID);
                subquery.select(parentPathFromChild);
                subquery.where(
                    builder.and(tagValuePredicate, builder.isNotNull(parentPathFromChild)));
                keyValuePredicates.add(
                    getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              }
              break;
            case ModelDBConstants.VERSIONED_INPUTS:
              LOGGER.debug("switch case : versioned_inputs");
              Root<VersioningModeldbEntityMapping> versioningEntityRoot =
                  subquery.from(VersioningModeldbEntityMapping.class);
              versioningEntityRoot.alias(
                  entityName + "_" + ModelDBConstants.VERSIONED_ALIAS + index);
              var keyValuePredicate =
                  RdbmsUtils.getValuePredicate(
                      builder, names[1], versioningEntityRoot.get(names[1]), predicate, false);

              List<Predicate> versioningValuePredicates = new ArrayList<>();
              versioningValuePredicates.add(keyValuePredicate);

              parentPathFromChild = versioningEntityRoot.get(entityName).get(ModelDBConstants.ID);
              subquery.select(parentPathFromChild);

              versioningValuePredicates.add(builder.isNotNull(parentPathFromChild));
              var versioningPredicatesOne = new Predicate[versioningValuePredicates.size()];
              for (var indexJ = 0; indexJ < versioningValuePredicates.size(); indexJ++) {
                versioningPredicatesOne[indexJ] = versioningValuePredicates.get(indexJ);
              }
              subquery.where(builder.and(versioningPredicatesOne));
              keyValuePredicates.add(
                  getPredicateFromSubquery(builder, entityRootPath, operator, subquery));
              break;
            default:
              if (entityName.equals(ModelDBConstants.REPOSITORY_ENTITY)) {
                if (key.equals(ModelDBConstants.TIME_UPDATED)) {
                  key = ModelDBConstants.DATE_UPDATED;
                }
                if (key.equals(ModelDBConstants.TIME_CREATED)) {
                  key = ModelDBConstants.DATE_CREATED;
                }
              }
              predicate = predicate.toBuilder().setOperator(operator).build();
              if (key.equalsIgnoreCase("owner")) {
                if (modelDBServiceResourceTypes.equals(ModelDBServiceResourceTypes.PROJECT)
                    || modelDBServiceResourceTypes.equals(ModelDBServiceResourceTypes.REPOSITORY)
                    || modelDBServiceResourceTypes.equals(ModelDBServiceResourceTypes.DATASET)) {
                  setFuzzyOwnerPredicateBasedOnUACWorkspace(
                      builder,
                      entityRootPath,
                      authService,
                      modelDBServiceResourceTypes,
                      keyValuePredicates,
                      predicate,
                      operator,
                      mdbRoleService);
                } else {
                  setFuzzySearchPredicateBasedOnMDBOwner(
                      builder,
                      criteriaQuery,
                      entityRootPath,
                      authService,
                      keyValuePredicates,
                      predicate,
                      key,
                      operator);
                }
              } else if (key.equalsIgnoreCase("dataset_visibility")) {
                expression = entityRootPath.get("repository_visibility");
                var queryPredicate =
                    RdbmsUtils.getValuePredicate(builder, key, expression, predicate, false);
                keyValuePredicates.add(queryPredicate);
                criteriaQuery.multiselect(entityRootPath, expression);
              } else {
                expression = entityRootPath.get(key);
                var queryPredicate =
                    RdbmsUtils.getValuePredicate(builder, key, expression, predicate, false);
                keyValuePredicates.add(queryPredicate);
                criteriaQuery.multiselect(entityRootPath, expression);
              }
          }
        } catch (IllegalArgumentException ex) {
          LOGGER.error(ex.getMessage(), ex);
          if (ex.getMessage()
              .contains(
                  "Unable to locate Attribute  with the the given name ["
                      + predicate.getKey()
                      + "] on this ManagedType ["
                      + entityRootPath.getJavaType().getName()
                      + "]")) {
            throw new InvalidArgumentException(
                "Predicate key '" + predicate.getKey() + "' mapping not found in the MDB entity");
          } else {
            throw ex;
          }
        }
      }
      if (!keyValuePredicates.isEmpty()) {
        var finalKeyValuePredicates = new Predicate[keyValuePredicates.size()];
        for (var indexJ = 0; indexJ < keyValuePredicates.size(); indexJ++) {
          finalKeyValuePredicates[indexJ] = keyValuePredicates.get(indexJ);
        }
        finalPredicatesList.add(builder.and(finalKeyValuePredicates));
      }
    }
    return finalPredicatesList;
  }

  private static void setFuzzyOwnerPredicateBasedOnUACWorkspace(
      CriteriaBuilder builder,
      Root<?> entityRootPath,
      AuthService authService,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      List<Predicate> keyValuePredicates,
      KeyValueQuery predicate,
      Operator operator,
      MDBRoleService mdbRoleService) {
    if ((operator.equals(Operator.CONTAIN) || operator.equals(Operator.NOT_CONTAIN))) {
      var fuzzySearchPredicate =
          getFuzzyUsersQueryPredicate(
              authService,
              mdbRoleService,
              builder,
              entityRootPath,
              predicate,
              modelDBServiceResourceTypes);
      if (fuzzySearchPredicate != null) {
        keyValuePredicates.add(fuzzySearchPredicate);
      } else {
        throw new ModelDBException(
            ModelDBConstants.INTERNAL_MSG_USERS_NOT_FOUND, Code.FAILED_PRECONDITION);
      }
    } else {
      var ownerId = predicate.getValue().getStringValue();
      Map<String, UserInfo> userInfoMap =
          authService.getUserInfoFromAuthServer(
              new HashSet<>(Collections.singleton(ownerId)),
              Collections.emptySet(),
              Collections.emptyList(),
              false);
      Set<String> resourceIdSet =
          getResourceIdsFromUserWorkspaces(
              authService,
              mdbRoleService,
              modelDBServiceResourceTypes,
              Collections.singletonList(userInfoMap.get(ownerId)));
      Expression<String> exp = entityRootPath.get(ModelDBConstants.ID);
      keyValuePredicates.add(exp.in(resourceIdSet));
    }
  }

  private static void setFuzzySearchPredicateBasedOnMDBOwner(
      CriteriaBuilder builder,
      CriteriaQuery<?> criteriaQuery,
      Root<?> entityRootPath,
      AuthService authService,
      List<Predicate> keyValuePredicates,
      KeyValueQuery predicate,
      String key,
      Operator operator) {
    Path expression;
    if (operator.equals(Operator.CONTAIN) || operator.equals(Operator.NOT_CONTAIN)) {
      var fuzzySearchPredicate =
          getFuzzyUsersQueryPredicate(authService, builder, entityRootPath, predicate);
      if (fuzzySearchPredicate != null) {
        keyValuePredicates.add(fuzzySearchPredicate);
      } else {
        throw new ModelDBException(
            ModelDBConstants.INTERNAL_MSG_USERS_NOT_FOUND, Code.FAILED_PRECONDITION);
      }
    } else {
      expression = entityRootPath.get(key);
      var queryPredicate = RdbmsUtils.getValuePredicate(builder, key, expression, predicate, false);
      keyValuePredicates.add(queryPredicate);
      criteriaQuery.multiselect(entityRootPath, expression);
    }
  }

  /**
   * This logic is for the other entities excluding project, repository logic where still owner is
   * at the MDB. above removed code with projectIds is added with this logic in subsequence PR.
   * where added two function one for old logic with owner Id for other entities excluding project,
   * repository
   *
   * @param authService: authService
   * @param builder: CriteriaBuilder
   * @param entityRootPath: entityRootPath
   * @param requestedPredicate: requestedPredicate
   * @return {@link Predicate}: criteria query predicate
   */
  private static Predicate getFuzzyUsersQueryPredicate(
      AuthService authService,
      CriteriaBuilder builder,
      Root<?> entityRootPath,
      KeyValueQuery requestedPredicate) {
    if (requestedPredicate.getValue().getKindCase().equals(Value.KindCase.STRING_VALUE)) {
      var operator = requestedPredicate.getOperator();
      List<UserInfo> userInfoList = getFuzzyUserInfos(authService, requestedPredicate);
      if (userInfoList != null && !userInfoList.isEmpty()) {
        Expression<String> exp = entityRootPath.get(requestedPredicate.getKey());
        List<String> vertaIds =
            userInfoList.stream()
                .map(authService::getVertaIdFromUserInfo)
                .collect(Collectors.toList());
        if (operator.equals(Operator.NOT_CONTAIN) || operator.equals(Operator.NE)) {
          return builder.not(exp.in(vertaIds));
        } else {
          return exp.in(vertaIds);
        }
      }
    } else {
      throw new InvalidArgumentException(
          "Predicate for the owner search only supporting 'String' value");
    }
    return null;
  }

  private static Predicate getFuzzyUsersQueryPredicate(
      AuthService authService,
      MDBRoleService mdbRoleService,
      CriteriaBuilder builder,
      Root<?> entityRootPath,
      KeyValueQuery requestedPredicate,
      ModelDBResourceEnum.ModelDBServiceResourceTypes modelDBServiceResourceTypes) {
    if (requestedPredicate.getValue().getKindCase().equals(Value.KindCase.STRING_VALUE)) {
      var operator = requestedPredicate.getOperator();
      List<UserInfo> userInfoList = getFuzzyUserInfos(authService, requestedPredicate);
      if (userInfoList != null && !userInfoList.isEmpty()) {
        Set<String> projectIdSet =
            getResourceIdsFromUserWorkspaces(
                authService, mdbRoleService, modelDBServiceResourceTypes, userInfoList);
        Expression<String> exp = entityRootPath.get(ModelDBConstants.ID);
        if (operator.equals(Operator.NOT_CONTAIN) || operator.equals(Operator.NE)) {
          return builder.not(exp.in(projectIdSet));
        } else {
          return exp.in(projectIdSet);
        }
      }
    } else {
      throw new InvalidArgumentException(
          "Predicate for the owner search only supporting 'String' value");
    }
    return null;
  }

  public static Set<String> getResourceIdsFromUserWorkspaces(
      AuthService authService,
      MDBRoleService mdbRoleService,
      ModelDBServiceResourceTypes modelDBServiceResourceTypes,
      List<UserInfo> userInfoList) {
    Set<String> resourceIdsSet = new HashSet<>();
    for (UserInfo userInfo : userInfoList) {
      List<GetResourcesResponseItem> accessibleAllWorkspaceItems =
          mdbRoleService.getResourceItems(
              Workspace.newBuilder()
                  .setId(authService.getWorkspaceIdFromUserInfo(userInfo))
                  .build(),
              Collections.emptySet(),
              modelDBServiceResourceTypes,
              false);
      resourceIdsSet.addAll(
          accessibleAllWorkspaceItems.stream()
              .map(GetResourcesResponseItem::getResourceId)
              .collect(Collectors.toSet()));
    }
    return resourceIdsSet;
  }

  public static List<UserInfo> getFuzzyUserInfos(
      AuthService authService, KeyValueQuery requestedPredicate) {
    var userInfoPaginationDTO =
        authService.getFuzzyUserInfoList(requestedPredicate.getValue().getStringValue());
    return userInfoPaginationDTO.getUserInfoList();
  }

  private static Predicate getVersionedInputHyperparameterPredicate(
      String entityName,
      CriteriaBuilder builder,
      Root<?> entityRootPath,
      int index,
      KeyValueQuery predicate,
      String[] names,
      Operator operator,
      Subquery<String> subquery) {
    Root<HyperparameterElementMappingEntity> elementMappingEntityRoot =
        subquery.from(HyperparameterElementMappingEntity.class);
    elementMappingEntityRoot.alias(
        entityName + "_" + ModelDBConstants.HYPERPARAMETER_ALIAS + "elem_mapping_" + index);
    List<Predicate> configBlobEntityRootPredicates = new ArrayList<>();
    var idPredicate =
        builder.equal(
            elementMappingEntityRoot.get(entityName).get(ModelDBConstants.ID),
            entityRootPath.get(ModelDBConstants.ID));
    configBlobEntityRootPredicates.add(idPredicate);

    var keyPredicate =
        builder.equal(elementMappingEntityRoot.get(ModelDBConstants.NAME), names[names.length - 1]);
    configBlobEntityRootPredicates.add(keyPredicate);

    List<Predicate> orPredicates = new ArrayList<>();
    if (predicate.getValue().getKindCase().equals(KindCase.NUMBER_VALUE)) {
      try {
        var intValuePredicate =
            getValuePredicate(
                builder,
                ModelDBConstants.HYPERPARAMETERS,
                elementMappingEntityRoot.get("int_value"),
                predicate,
                false);
        orPredicates.add(intValuePredicate);
      } catch (Exception e) {
        LOGGER.debug("Value could not be cast to int");
      }
      try {
        var floatValuePredicate =
            getValuePredicate(
                builder,
                ModelDBConstants.HYPERPARAMETERS,
                elementMappingEntityRoot.get("float_value"),
                predicate,
                false);
        orPredicates.add(floatValuePredicate);
      } catch (Exception e) {
        LOGGER.debug("Value could not be cast to float");
      }
    }
    var stringValuePredicate =
        getValuePredicate(
            builder,
            ModelDBConstants.HYPERPARAMETERS,
            elementMappingEntityRoot.get("string_value"),
            predicate,
            true);
    orPredicates.add(stringValuePredicate);
    configBlobEntityRootPredicates.add(builder.or(orPredicates.toArray(new Predicate[0])));

    Expression<String> parentPathFromChild =
        elementMappingEntityRoot.get(entityName).get(ModelDBConstants.ID);
    configBlobEntityRootPredicates.add(builder.isNotNull(parentPathFromChild));

    var hyprElemmappingRootPredicatesOne = new Predicate[configBlobEntityRootPredicates.size()];
    for (var indexJ = 0; indexJ < configBlobEntityRootPredicates.size(); indexJ++) {
      hyprElemmappingRootPredicatesOne[indexJ] = configBlobEntityRootPredicates.get(indexJ);
    }
    subquery.select(parentPathFromChild);
    subquery.where(builder.and(hyprElemmappingRootPredicatesOne));

    return getPredicateFromSubquery(builder, entityRootPath, operator, subquery);
  }

  private static Predicate getPredicateFromSubquery(
      CriteriaBuilder builder,
      Root<?> entityRootPath,
      Operator operator,
      Subquery<String> subquery) {
    return getPredicateFromSubquery(
        builder, entityRootPath, operator, subquery, ModelDBConstants.ID);
  }

  private static Predicate getPredicateFromSubquery(
      CriteriaBuilder builder,
      Root<?> entityRootPath,
      Operator operator,
      Subquery<String> subquery,
      String fieldName) {
    Expression<String> exp = entityRootPath.get(fieldName);
    if (operator.equals(Operator.NOT_CONTAIN) || operator.equals(Operator.NE)) {
      return builder.not(exp.in(subquery));
    } else {
      return exp.in(subquery);
    }
  }

  public static List<VersioningModeldbEntityMapping> getVersioningMappingFromVersioningInput(
      Session session,
      VersioningEntry versioningEntry,
      Map<String, Map.Entry<BlobExpanded, String>> locationBlobWithHashMap,
      Object entity) {
    List<VersioningModeldbEntityMapping> versioningModeldbEntityMappings = new ArrayList<>();
    if (versioningEntry.getKeyLocationMapMap().isEmpty()) {
      versioningModeldbEntityMappings.add(
          new VersioningModeldbEntityMapping(
              versioningEntry.getRepositoryId(),
              versioningEntry.getCommit(),
              CommonConstants.EMPTY_STRING,
              null,
              null,
              null,
              entity));
    } else {
      for (Map.Entry<String, Location> locationEntry :
          versioningEntry.getKeyLocationMapMap().entrySet()) {
        var locationKey = String.join("#", locationEntry.getValue().getLocationList());
        Map.Entry<BlobExpanded, String> blobExpandedWithHashMap =
            locationBlobWithHashMap.get(locationKey);

        var blob = blobExpandedWithHashMap.getKey().getBlob();

        var vmem =
            new VersioningModeldbEntityMapping(
                versioningEntry.getRepositoryId(),
                versioningEntry.getCommit(),
                locationEntry.getKey(),
                ModelDBUtils.getStringFromProtoObject(locationEntry.getValue()),
                blob.getContentCase().getNumber(),
                blobExpandedWithHashMap.getValue(),
                entity);
        if (blob.getContentCase().equals(Blob.ContentCase.CONFIG)) {
          var query =
              session.createQuery("FROM ConfigBlobEntity cb WHERE cb.blob_hash = :blobHash");
          query.setParameter("blobHash", blobExpandedWithHashMap.getValue());
          List<ConfigBlobEntity> configBlobEntities = query.list();
          vmem.setConfig_blob_entities(new HashSet<>(configBlobEntities));
        }
        versioningModeldbEntityMappings.add(vmem);
      }
    }
    return versioningModeldbEntityMappings;
  }

  public static VersioningEntry getVersioningEntryFromList(
      List<VersioningModeldbEntityMapping> versioningModeldbEntityMappings) {
    var versioningEntry = VersioningEntry.newBuilder();
    for (VersioningModeldbEntityMapping versioningModeldbEntityMapping :
        versioningModeldbEntityMappings) {
      versioningEntry.setRepositoryId(versioningModeldbEntityMapping.getRepository_id());
      versioningEntry.setCommit(versioningModeldbEntityMapping.getCommit());
      if (versioningModeldbEntityMapping.getVersioning_location() != null
          && !versioningModeldbEntityMapping.getVersioning_location().isEmpty()) {
        var locationBuilder = Location.newBuilder();
        CommonUtils.getProtoObjectFromString(
            versioningModeldbEntityMapping.getVersioning_location(), locationBuilder);
        versioningEntry.putKeyLocationMap(
            versioningModeldbEntityMapping.getVersioning_key(), locationBuilder.build());
      }
    }
    return versioningEntry.build();
  }

  /**
   * Validating predicate with following condition
   *
   * <p>Validate if current user has access to the entity or not where predicate key has an id with
   * only allowed 'EQ' operators
   *
   * <p>Validate predicate id is in accessible ids or not.
   *
   * <p>Only allow 'EQ' operator for predicate key has an id.
   *
   * <p>Workspace & workspace type should not allowed in the predicate
   *
   * @param entityName : dataset, project etc.
   * @param accessibleEntityIds : accessible entity ids like project.ids, dataset.ids etc.
   * @param predicate : predicate request
   * @param mdbRoleService : role service
   */
  public static void validatePredicates(
      String entityName,
      List<String> accessibleEntityIds,
      KeyValueQuery predicate,
      MDBRoleService mdbRoleService) {
    if (predicate.getKey().equals(ModelDBConstants.ID)) {
      if (!predicate.getOperator().equals(OperatorEnum.Operator.EQ)) {
        throw new InvalidArgumentException(ModelDBConstants.NON_EQ_ID_PRED_ERROR_MESSAGE);
      }
      var entityId = predicate.getValue().getStringValue();
      if ((accessibleEntityIds.isEmpty() || !accessibleEntityIds.contains(entityId))
          && mdbRoleService.IsImplemented()) {
        throw new PermissionDeniedException(
            "Access is denied. User is unauthorized for given "
                + entityName
                + " entity ID : "
                + entityId);
      }
    }

    if (predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE)
        || predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE_ID)
        || predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE_NAME)
        || predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE_TYPE)) {
      throw new InvalidArgumentException("Workspace name OR type not supported as predicate");
    }
  }

  /**
   * @param index0 : predicate index for unique query parameter identity
   * @param queryBuilder : query builder
   * @param operator : query operator like GE, LE, EQ, GTE etc.
   * @param value : query parameter value like repoId(123), repoName(xyz) etc.
   * @param parametersMap : query parameter identity map.
   */
  public static void setValueWithOperatorInQuery(
      int index0,
      StringBuilder queryBuilder,
      OperatorEnum.Operator operator,
      Object value,
      Map<String, Object> parametersMap) {
    @SuppressWarnings("squid:S2245")
    long timestamp =
        (long) index0 + (long) new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE);
    String key;
    switch (operator.ordinal()) {
      case OperatorEnum.Operator.GT_VALUE:
        key = "GT_VALUE_" + timestamp;
        queryBuilder.append(" > :").append(key);
        parametersMap.put(key, value);
        break;
      case OperatorEnum.Operator.GTE_VALUE:
        key = "GTE_VALUE_" + timestamp;
        queryBuilder.append(" >= :").append(key);
        parametersMap.put(key, value);
        break;
      case OperatorEnum.Operator.LT_VALUE:
        key = "LT_VALUE_" + timestamp;
        queryBuilder.append(" < :").append(key);
        parametersMap.put(key, value);
        break;
      case OperatorEnum.Operator.LTE_VALUE:
        key = "LTE_VALUE_" + timestamp;
        queryBuilder.append(" <= :").append(key);
        parametersMap.put(key, value);
        break;
      case OperatorEnum.Operator.NE_VALUE:
        key = "NE_VALUE_" + timestamp;
        queryBuilder.append(" <> :").append(key);
        parametersMap.put(key, value);
        break;
      case OperatorEnum.Operator.CONTAIN_VALUE:
        queryBuilder
            .append(" LIKE ")
            .append(("'%" + Pattern.compile((String) value) + "%'").toLowerCase());
        break;
      case OperatorEnum.Operator.NOT_CONTAIN_VALUE:
        queryBuilder
            .append(" NOT LIKE ")
            .append(("'%" + Pattern.compile((String) value) + "%'").toLowerCase());
        break;
      case OperatorEnum.Operator.IN_VALUE:
        key = "IN_VALUE_" + timestamp;
        queryBuilder.append(" IN (:").append(key).append(")");
        parametersMap.put(key, value);
        break;
      default:
        key = "default_" + timestamp;
        queryBuilder.append(" = :").append(key);
        parametersMap.put(key, value);
    }
    queryBuilder.append(" ");
  }

  public static void setParameterInQuery(Query query, Map<String, Object> parametersMap) {
    if (parametersMap.size() > 0) {
      parametersMap.forEach(
          (key, value) -> {
            if (value instanceof List) {
              List<Object> objectList = (List<Object>) value;
              query.setParameterList(key, objectList);
            } else {
              query.setParameter(key, value);
            }
          });
    }
  }
}
