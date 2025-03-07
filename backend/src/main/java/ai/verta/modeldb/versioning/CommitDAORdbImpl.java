package ai.verta.modeldb.versioning;

import ai.verta.common.KeyValueQuery;
import ai.verta.common.ModelDBResourceEnum.ModelDBServiceResourceTypes;
import ai.verta.common.OperatorEnum;
import ai.verta.modeldb.DatasetPartInfo;
import ai.verta.modeldb.DatasetVersion;
import ai.verta.modeldb.ModelDBConstants;
import ai.verta.modeldb.PathLocationTypeEnum.PathLocationType;
import ai.verta.modeldb.authservice.MDBRoleService;
import ai.verta.modeldb.common.authservice.AuthService;
import ai.verta.modeldb.common.collaborator.CollaboratorUser;
import ai.verta.modeldb.common.exceptions.ModelDBException;
import ai.verta.modeldb.cron_jobs.DeleteEntitiesCron;
import ai.verta.modeldb.dto.CommitPaginationDTO;
import ai.verta.modeldb.entities.AttributeEntity;
import ai.verta.modeldb.entities.metadata.LabelsMappingEntity;
import ai.verta.modeldb.entities.metadata.MetadataPropertyMappingEntity;
import ai.verta.modeldb.entities.versioning.*;
import ai.verta.modeldb.exceptions.InvalidArgumentException;
import ai.verta.modeldb.metadata.IDTypeEnum;
import ai.verta.modeldb.metadata.IdentificationType;
import ai.verta.modeldb.metadata.MetadataDAO;
import ai.verta.modeldb.utils.ModelDBHibernateUtil;
import ai.verta.modeldb.utils.ModelDBUtils;
import ai.verta.modeldb.utils.RdbmsUtils;
import ai.verta.modeldb.versioning.blob.container.BlobContainer;
import ai.verta.uac.UserInfo;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.Value;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Session;
import org.hibernate.query.Query;

public class CommitDAORdbImpl implements CommitDAO {

  private static final Logger LOGGER = LogManager.getLogger(CommitDAORdbImpl.class);
  private static final ModelDBHibernateUtil modelDBHibernateUtil =
      ModelDBHibernateUtil.getInstance();
  private static final String REPO_ID_QUERY_PARAM = "repoId";
  private static final String COMMIT_HASHES_QUERY_PARAM = "commitHashes";
  private static final String REPOSITORY_ID_QUERY_PARAM = "repositoryId";
  private final AuthService authService;
  private final MDBRoleService mdbRoleService;

  public CommitDAORdbImpl(AuthService authService, MDBRoleService mdbRoleService) {
    this.authService = authService;
    this.mdbRoleService = mdbRoleService;
  }

  private static final long CACHE_SIZE = 1000;
  private static final int DURATION = 10;

  private LoadingCache<String, ReadWriteLock> locks =
      CacheBuilder.newBuilder()
          .maximumSize(CACHE_SIZE)
          .expireAfterWrite(DURATION, TimeUnit.MINUTES)
          .build(
              new CacheLoader<String, ReadWriteLock>() {
                public ReadWriteLock load(String lockKey) {
                  return new ReentrantReadWriteLock() {};
                }
              });

  @SuppressWarnings({"squid:S2222"})
  protected AutoCloseable acquireWriteLock(String lockKey) throws ExecutionException {
    LOGGER.debug("acquireWriteLock for key: {}", lockKey);
    ReadWriteLock lock = locks.get(lockKey);
    var writeLock = lock.writeLock();
    writeLock.lock();
    return writeLock::unlock;
  }

  /**
   * commit : details of the commit and the blobs to be added setBlobs : recursively creates trees
   * and blobs in top down fashion and generates SHAs in bottom up fashion getRepository : fetches
   * the repository the commit is made on
   */
  public CreateCommitRequest.Response setCommit(
      String author,
      Commit commit,
      BlobFunction setBlobs,
      BlobFunction.BlobFunctionAttribute setBlobsAttributes,
      RepositoryFunction getRepository)
      throws ModelDBException, NoSuchAlgorithmException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession();
        AutoCloseable ignored = acquireWriteLock(commit.getCommitSha())) {
      session.beginTransaction();
      final String rootSha = setBlobs.apply(session);
      var repositoryEntity = getRepository.apply(session);

      var commitEntity = saveCommitEntity(session, commit, rootSha, author, repositoryEntity);
      setBlobsAttributes.apply(session, repositoryEntity.getId(), commitEntity.getCommit_hash());
      session.getTransaction().commit();
      return CreateCommitRequest.Response.newBuilder()
          .setCommit(commitEntity.toCommitProto())
          .build();
    } catch (ModelDBException ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return setCommit(author, commit, setBlobs, setBlobsAttributes, getRepository);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      throw new ModelDBException(ex);
    }
  }

  @Override
  public CreateCommitRequest.Response setCommitFromDatasetVersion(
      DatasetVersion datasetVersion,
      RepositoryDAO repositoryDAO,
      BlobDAO blobDAO,
      MetadataDAO metadataDAO,
      RepositoryEntity repositoryEntity)
      throws ModelDBException, NoSuchAlgorithmException {
    var repositoryIdentification =
        RepositoryIdentification.newBuilder().setRepoId(repositoryEntity.getId()).build();
    // Set parent datasetVersion
    var getBranchResponse =
        repositoryDAO.getBranch(
            GetBranchRequest.newBuilder()
                .setRepositoryId(repositoryIdentification)
                .setBranch(ModelDBConstants.MASTER_BRANCH)
                .build(),
            false,
            RepositoryEnums.RepositoryTypeEnum.DATASET);
    datasetVersion =
        datasetVersion
            .toBuilder()
            .setParentId(getBranchResponse.getCommit().getCommitSha())
            .build();

    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      var blobBuilder = Blob.newBuilder();
      if (datasetVersion.hasDatasetBlob()) {
        blobBuilder.setDataset(datasetVersion.getDatasetBlob());
      } else {
        var datasetBlobBuilder = DatasetBlob.newBuilder();
        switch (datasetVersion.getDatasetVersionInfoCase()) {
          case PATH_DATASET_VERSION_INFO:
            var pathDatasetVersionInfo = datasetVersion.getPathDatasetVersionInfo();
            List<DatasetPartInfo> partInfos = pathDatasetVersionInfo.getDatasetPartInfosList();
            Stream<PathDatasetComponentBlob> result =
                partInfos.stream()
                    .map(
                        datasetPartInfo ->
                            componentFromPart(
                                datasetPartInfo, pathDatasetVersionInfo.getBasePath()));
            if (pathDatasetVersionInfo.getLocationType() == PathLocationType.S3_FILE_SYSTEM) {
              datasetBlobBuilder.setS3(
                  S3DatasetBlob.newBuilder()
                      .addAllComponents(
                          result
                              .map(
                                  path -> S3DatasetComponentBlob.newBuilder().setPath(path).build())
                              .collect(Collectors.toList())));
            } else {
              datasetBlobBuilder.setPath(
                  PathDatasetBlob.newBuilder()
                      .addAllComponents(result.collect(Collectors.toList())));
            }
            break;
          case DATASETVERSIONINFO_NOT_SET:
          default:
            throw new ModelDBException("Wrong dataset version type", Code.INVALID_ARGUMENT);
        }
        blobBuilder.setDataset(datasetBlobBuilder);
      }
      List<String> location =
          Collections.singletonList(ModelDBConstants.DEFAULT_VERSIONING_BLOB_LOCATION);
      List<BlobContainer> blobList =
          Collections.singletonList(
              BlobContainer.create(
                  BlobExpanded.newBuilder()
                      .addAllLocation(location)
                      .setBlob(blobBuilder.build())
                      .addAllAttributes(datasetVersion.getAttributesList())
                      .build()));

      session.beginTransaction();
      final String rootSha = blobDAO.setBlobs(session, blobList, new FileHasher());

      var builder = Commit.newBuilder();
      if (!datasetVersion.getParentId().isEmpty()) {
        builder.addParentShas(datasetVersion.getParentId());
      }
      builder.setDateCreated(datasetVersion.getTimeLogged());
      builder.setDateUpdated(datasetVersion.getTimeUpdated());
      builder.setVersionNumber(datasetVersion.getVersionNumber());
      var commit = builder.build();

      if (!repositoryEntity.isDataset()) {
        throw new ModelDBException(
            "Repository should be created from Dataset to add Dataset Version to it",
            Status.Code.INVALID_ARGUMENT);
      }

      var commitPaginationDTO =
          findCommits(
              session,
              FindRepositoriesBlobs.newBuilder()
                  .setPageNumber(1)
                  .setPageLimit(1)
                  .addRepoIds(repositoryEntity.getId())
                  .build(),
              authService.getCurrentLoginUserInfo(),
              false,
              false,
              true,
              null,
              false);

      var commitEntity =
          saveCommitEntity(session, commit, rootSha, datasetVersion.getOwner(), repositoryEntity);
      blobDAO.setBlobsAttributes(
          session, repositoryEntity.getId(), commitEntity.getCommit_hash(), blobList, true);
      String compositeId =
          VersioningUtils.getVersioningCompositeId(
              repositoryEntity.getId(), commitEntity.getCommit_hash(), location);
      metadataDAO.addProperty(
          session,
          IdentificationType.newBuilder()
              .setIdType(IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB)
              .setStringId(compositeId)
              .build(),
          "description",
          datasetVersion.getDescription());
      metadataDAO.addLabels(
          session,
          IdentificationType.newBuilder()
              .setStringId(compositeId)
              .setIdType(IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB)
              .build(),
          datasetVersion.getTagsList());

      long version = datasetVersion.getVersion();
      if (commitPaginationDTO.getCommitEntities() != null
          && !commitPaginationDTO.getCommitEntities().isEmpty()
          && version == 0) {
        CommitEntity parentEntity = commitPaginationDTO.getCommitEntities().get(0);
        String parentCompositeId =
            VersioningUtils.getVersioningCompositeId(
                repositoryEntity.getId(), parentEntity.getCommit_hash(), location);
        String parentVersion =
            metadataDAO.getProperty(
                session,
                IdentificationType.newBuilder()
                    .setIdType(IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB)
                    .setStringId(parentCompositeId)
                    .build(),
                ModelDBConstants.VERSION);
        if (parentVersion != null && !parentVersion.isEmpty()) {
          version = Long.parseLong(parentVersion) + 1L;
        }
      }
      if (version == 0) {
        version = 1;
      }
      metadataDAO.addProperty(
          session,
          IdentificationType.newBuilder()
              .setIdType(IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB)
              .setStringId(compositeId)
              .build(),
          ModelDBConstants.VERSION,
          String.valueOf(version));
      metadataDAO.addProperty(
          session,
          IdentificationType.newBuilder()
              .setIdType(IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB)
              .setStringId(compositeId)
              .build(),
          "version_number",
          String.valueOf(datasetVersion.getVersionNumber()));
      session.getTransaction().commit();

      repositoryDAO.setBranch(
          SetBranchRequest.newBuilder()
              .setRepositoryId(
                  RepositoryIdentification.newBuilder().setRepoId(repositoryEntity.getId()).build())
              .setBranch(ModelDBConstants.MASTER_BRANCH)
              .setCommitSha(commitEntity.getCommit_hash())
              .build(),
          false,
          RepositoryEnums.RepositoryTypeEnum.DATASET);

      return CreateCommitRequest.Response.newBuilder()
          .setCommit(commitEntity.toCommitProto())
          .build();
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return setCommitFromDatasetVersion(
            datasetVersion, repositoryDAO, blobDAO, metadataDAO, repositoryEntity);
      } else {
        throw ex;
      }
    }
  }

  private PathDatasetComponentBlob componentFromPart(DatasetPartInfo part, String basePath) {
    return PathDatasetComponentBlob.newBuilder()
        .setPath(part.getPath())
        .setSize(part.getSize())
        .setLastModifiedAtSource(part.getLastModifiedAtSource())
        .setMd5(part.getChecksum())
        .setBasePath(basePath)
        .build();
  }

  @Override
  public CommitEntity saveCommitEntity(
      Session session,
      Commit commit,
      String rootSha,
      String author,
      RepositoryEntity repositoryEntity)
      throws ModelDBException, NoSuchAlgorithmException {
    long timeCreated = new Date().getTime();
    if (commit.getDateCreated() != 0L) {
      timeCreated = commit.getDateCreated();
    }

    Map<String, CommitEntity> parentCommitEntities = new HashMap<>();
    if (!commit.getParentShasList().isEmpty()) {
      parentCommitEntities =
          getCommits(session, repositoryEntity.getId(), commit.getParentShasList());
      if (parentCommitEntities.size() != commit.getParentShasCount()) {
        for (String parentSHA : commit.getParentShasList()) {
          if (!parentCommitEntities.containsKey(parentSHA)) {
            throw new ModelDBException(
                "Parent commit '" + parentSHA + "' not found in DB", Code.INVALID_ARGUMENT);
          }
        }
      }
    }
    Map<Integer, CommitEntity> parentOrderMap = new HashMap<>();
    for (var index = 0; index < commit.getParentShasCount(); index++) {
      parentOrderMap.put(index, parentCommitEntities.get(commit.getParentShas(index)));
    }

    var internalCommit =
        Commit.newBuilder()
            .setDateCreated(timeCreated)
            .setDateUpdated(timeCreated)
            .setAuthor(author)
            .setMessage(commit.getMessage())
            .setCommitSha(generateCommitSHA(rootSha, commit, timeCreated))
            .setVersionNumber(commit.getVersionNumber())
            .build();
    var commitEntity = new CommitEntity(repositoryEntity, parentOrderMap, internalCommit, rootSha);
    session.saveOrUpdate(commitEntity);
    return commitEntity;
  }

  public CommitPaginationDTO fetchCommitEntityList(
      Session session, ListCommitsRequest request, Long repoId, boolean ascending)
      throws ModelDBException {
    var commitQueryBuilder =
        new StringBuilder(
            " FROM "
                + CommitEntity.class.getSimpleName()
                + " cm INNER JOIN cm.repository repo WHERE repo.id = :repoId ");
    Map<String, Long> parameterMap = new HashMap<>();
    if (!request.getCommitBase().isEmpty()) {
      var baseCommitEntity =
          Optional.ofNullable(session.get(CommitEntity.class, request.getCommitBase()))
              .orElseThrow(
                  () ->
                      new ModelDBException(
                          "Couldn't find base commit by sha : " + request.getCommitBase(),
                          Code.NOT_FOUND));
      Long baseTime = baseCommitEntity.getDate_created();
      commitQueryBuilder.append(" AND cm.date_created >= :date_created_baseTime ");
      parameterMap.put("date_created_baseTime", baseTime);
    }

    if (!request.getCommitHead().isEmpty()) {
      var headCommitEntity =
          Optional.ofNullable(session.get(CommitEntity.class, request.getCommitHead()))
              .orElseThrow(
                  () ->
                      new ModelDBException(
                          "Couldn't find head commit by sha : " + request.getCommitHead(),
                          Code.NOT_FOUND));
      Long headTime = headCommitEntity.getDate_created();
      commitQueryBuilder.append(" AND cm.date_created <= :date_created_headTime ");
      parameterMap.put("date_created_headTime", headTime);
    }

    String order = ascending ? " ASC " : " DESC ";

    StringBuilder finalQueryBuilder =
        new StringBuilder("SELECT cm ")
            .append(commitQueryBuilder.toString())
            .append(" ORDER BY cm.date_updated ")
            .append(order);
    Query<CommitEntity> commitEntityQuery = session.createQuery(finalQueryBuilder.toString());
    StringBuilder finalCountBuilder =
        new StringBuilder("SELECT count(cm) ").append(commitQueryBuilder);
    var countQuery = session.createQuery(finalCountBuilder.toString());

    commitEntityQuery.setParameter(REPO_ID_QUERY_PARAM, repoId);
    countQuery.setParameter(REPO_ID_QUERY_PARAM, repoId);
    if (!parameterMap.isEmpty()) {
      parameterMap.forEach(
          (key, value) -> {
            commitEntityQuery.setParameter(key, value);
            countQuery.setParameter(key, value);
          });
    }
    if (request.hasPagination()) {
      int pageLimit = request.getPagination().getPageLimit();
      final int startPosition = (request.getPagination().getPageNumber() - 1) * pageLimit;
      commitEntityQuery.setFirstResult(startPosition);
      commitEntityQuery.setMaxResults(pageLimit);
    }
    List<CommitEntity> commitEntities = commitEntityQuery.list();
    Long totalRecords = (long) countQuery.uniqueResult();

    var commitPaginationDTO = new CommitPaginationDTO();
    commitPaginationDTO.setCommitEntities(commitEntities);
    commitPaginationDTO.setTotalRecords(totalRecords);
    return commitPaginationDTO;
  }

  @Override
  public ListCommitsRequest.Response listCommits(
      ListCommitsRequest request, RepositoryFunction getRepository, boolean ascending)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      RepositoryEntity repository = getRepository.apply(session);

      var commitPaginationDTO =
          fetchCommitEntityList(session, request, repository.getId(), ascending);
      List<Commit> commits =
          commitPaginationDTO.getCommitEntities().stream()
              .map(CommitEntity::toCommitProto)
              .collect(Collectors.toList());
      return ListCommitsRequest.Response.newBuilder()
          .addAllCommits(commits)
          .setTotalRecords(commitPaginationDTO.getTotalRecords())
          .build();
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return listCommits(request, getRepository, ascending);
      } else {
        throw ex;
      }
    }
  }

  private String generateCommitSHA(String blobSHA, Commit commit, long timeCreated)
      throws NoSuchAlgorithmException {
    return VersioningUtils.generateCommitSHA(
        commit.getParentShasList(), commit.getMessage(), timeCreated, commit.getAuthor(), blobSHA);
  }
  /**
   * @param session session
   * @param parentShaList : a list of sha for which the function returns commits
   * @return {@link Map<String, CommitEntity>}
   */
  private Map<String, CommitEntity> getCommits(
      Session session, Long repoId, ProtocolStringList parentShaList) {
    var commitQueryBuilder =
        new StringBuilder(
            "SELECT cm FROM "
                + CommitEntity.class.getSimpleName()
                + " cm LEFT JOIN cm.repository repo WHERE repo.id = :repoId AND cm.commit_hash IN (:commitHashes)");

    Query<CommitEntity> commitEntityQuery =
        session.createQuery(commitQueryBuilder.append(" ORDER BY cm.date_created DESC").toString());
    commitEntityQuery.setParameter(REPO_ID_QUERY_PARAM, repoId);
    commitEntityQuery.setParameter(COMMIT_HASHES_QUERY_PARAM, parentShaList);
    return commitEntityQuery.list().stream()
        .collect(Collectors.toMap(CommitEntity::getCommit_hash, commitEntity -> commitEntity));
  }

  @Override
  public Commit getCommit(String commitHash, RepositoryFunction getRepository)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      var commitEntity = getCommitEntity(session, commitHash, getRepository);

      return commitEntity.toCommitProto();
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return getCommit(commitHash, getRepository);
      } else {
        throw ex;
      }
    }
  }

  @Override
  public CommitEntity getCommitEntity(
      Session session, String commitHash, RepositoryFunction getRepositoryFunction)
      throws ModelDBException {
    var repositoryEntity = getRepositoryFunction.apply(session);
    boolean exists =
        VersioningUtils.commitRepositoryMappingExists(
            session, commitHash, repositoryEntity.getId());
    if (!exists) {
      throw new ModelDBException("Commit_hash and repository_id mapping not found", Code.NOT_FOUND);
    }

    return session.load(CommitEntity.class, commitHash);
  }

  @Override
  public String getDatasetIdByDatasetVersion(RepositoryDAO repositoryDAO, String commitHash)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      var commitEntity = session.get(CommitEntity.class, commitHash);

      if (commitEntity == null) {
        throw new ModelDBException("DatasetVersion not found", Code.NOT_FOUND);
      }

      if (commitEntity.getRepository() != null && commitEntity.getRepository().size() > 1) {
        throw new ModelDBException(
            String.format(
                "DatasetVersion '%s' associated with multiple datasets",
                commitEntity.getCommit_hash()),
            Code.INTERNAL);
      }
      return String.valueOf(new ArrayList<>(commitEntity.getRepository()).get(0).getId());
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return getDatasetIdByDatasetVersion(repositoryDAO, commitHash);
      } else {
        throw ex;
      }
    }
  }
  /**
   * Deleting dataversiosn stored as commits
   *
   * <p>1. get Repo
   *
   * <p>2. Iterate through each dataset version 2.1 get Commit
   *
   * <p>2.2 commit since representing datasetversion verify commit belongs to a single repo and
   * match with repo id from 1.
   *
   * <p>2.3 get parent commit , since it is dataset assume just single commit
   *
   * <p>2.4 if commit to be deleted is pointed to by a branch , move branch to parent.
   *
   * <p>2.5 if commit has children move them to parent 2.5 delete label , tag , attributes for the
   * commit
   */
  @Override
  public synchronized void deleteDatasetVersions(
      RepositoryIdentification repositoryIdentification,
      List<String> datasetVersionIds,
      RepositoryDAO repositoryDAO)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      RepositoryEntity repositoryEntity = null;
      if (repositoryIdentification != null) {
        repositoryEntity =
            repositoryDAO.getRepositoryById(
                session,
                repositoryIdentification,
                true,
                false,
                RepositoryEnums.RepositoryTypeEnum.REGULAR);
      }

      for (String datasetVersionId : datasetVersionIds) {
        Query<CommitEntity> getCommitQuery =
            session.createQuery(
                "From CommitEntity c WHERE c.commit_hash = :commitHash", CommitEntity.class);
        getCommitQuery.setParameter("commitHash", datasetVersionId);
        var commitEntity = getCommitQuery.uniqueResult();
        if (commitEntity == null || commitEntity.getParent_commits().isEmpty()) {
          LOGGER.warn(
              "skipping deleting commit corresponding to dataset version {}", datasetVersionId);
          continue;
        }

        if (commitEntity.getRepository() != null && commitEntity.getRepository().size() > 1) {
          throw new ModelDBException(
              String.format(
                  "DatasetVersion '%s' associated with multiple datasets",
                  commitEntity.getCommit_hash()),
              Code.INTERNAL);
        } else if (commitEntity.getRepository() == null) {
          throw new ModelDBException("DatasetVersion not associated with datasets", Code.INTERNAL);
        }
        Long newRepoId = new ArrayList<>(commitEntity.getRepository()).get(0).getId();
        if (repositoryIdentification == null) {
          repositoryIdentification =
              RepositoryIdentification.newBuilder().setRepoId(newRepoId).build();
        } else {
          if (repositoryIdentification.getRepoId() != newRepoId) {
            throw new ModelDBException(
                String.format(
                    "DatasetVersion '%s' associated with multiple datasets",
                    commitEntity.getCommit_hash()),
                Code.INTERNAL);
          }
        }

        if (repositoryEntity == null) {
          repositoryEntity =
              repositoryDAO.getRepositoryById(
                  session,
                  repositoryIdentification,
                  true,
                  false,
                  RepositoryEnums.RepositoryTypeEnum.REGULAR);
        }

        var query = session.createQuery(RepositoryDAORdbImpl.CHECK_BRANCH_IN_REPOSITORY_HQL);
        query.setParameter(REPOSITORY_ID_QUERY_PARAM, repositoryEntity.getId());
        query.setParameter("branch", ModelDBConstants.MASTER_BRANCH);
        var branchEntity = (BranchEntity) query.uniqueResult();

        CommitEntity parentDatasetVersion = commitEntity.getParent_commits().get(0);

        if (branchEntity != null
            && branchEntity.getCommit_hash().equals(commitEntity.getCommit_hash())) {
          repositoryDAO.setBranch(
              SetBranchRequest.newBuilder()
                  .setRepositoryId(repositoryIdentification)
                  .setBranch(ModelDBConstants.MASTER_BRANCH)
                  .setCommitSha(parentDatasetVersion.getCommit_hash())
                  .build(),
              false,
              RepositoryEnums.RepositoryTypeEnum.DATASET);
        }

        session.beginTransaction();
        session.lock(commitEntity, LockMode.PESSIMISTIC_WRITE);
        if (!commitEntity.getChild_commits().isEmpty()) {
          CommitEntity childCommit = new ArrayList<>(commitEntity.getChild_commits()).get(0);
          session.lock(childCommit, LockMode.PESSIMISTIC_WRITE);
          var updateChildEntity =
              "UPDATE commit_parent SET parent_hash = :parentHash WHERE child_hash = :childHash";
          Query updateChildQuery =
              session
                  .createSQLQuery(updateChildEntity)
                  .setLockOptions(new LockOptions().setLockMode(LockMode.PESSIMISTIC_WRITE));
          updateChildQuery.setParameter("parentHash", parentDatasetVersion.getCommit_hash());
          updateChildQuery.setParameter("childHash", childCommit.getCommit_hash());
          updateChildQuery.executeUpdate();
        }

        String compositeId =
            VersioningUtils.getVersioningCompositeId(
                repositoryEntity.getId(),
                commitEntity.getCommit_hash(),
                Collections.singletonList(ModelDBConstants.DEFAULT_VERSIONING_BLOB_LOCATION));
        DeleteEntitiesCron.deleteLabels(
            session, compositeId, IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB);
        DeleteEntitiesCron.deleteAttribute(session, compositeId);
        session.delete(commitEntity);
        session.getTransaction().commit();
        session.clear();
      }
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        deleteDatasetVersions(repositoryIdentification, datasetVersionIds, repositoryDAO);
      } else {
        throw ex;
      }
    }
  }

  @Override
  public boolean deleteCommits(
      RepositoryIdentification repositoryIdentification,
      List<String> commitShas,
      RepositoryDAO repositoryDAO)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      Query<CommitEntity> getCommitQuery =
          session.createQuery(
              "From CommitEntity c WHERE c.commit_hash IN (:commitHashes)", CommitEntity.class);
      getCommitQuery.setParameter(COMMIT_HASHES_QUERY_PARAM, commitShas);
      List<CommitEntity> commitEntities = getCommitQuery.getResultList();
      if (commitEntities.isEmpty()) {
        throw new ModelDBException("Commits not found for the ids: " + commitShas, Code.NOT_FOUND);
      }

      for (CommitEntity commitEntity : commitEntities) {
        if (!commitEntity.getChild_commits().isEmpty()) {
          throw new ModelDBException(
              "Commit '"
                  + commitEntity.getCommit_hash()
                  + "' has the child, please delete child commit first",
              Code.FAILED_PRECONDITION);
        }
      }

      var repositoryEntity =
          repositoryDAO.getRepositoryById(
              session,
              repositoryIdentification,
              true,
              true,
              RepositoryEnums.RepositoryTypeEnum.REGULAR);

      String getBranchByCommitHQLBuilder =
          "FROM BranchEntity br where br.id.repository_id = :repositoryId "
              + " AND br.commit_hash IN (:commitHashes) ";
      Query<BranchEntity> getBranchByCommitQuery =
          session.createQuery(getBranchByCommitHQLBuilder, BranchEntity.class);
      getBranchByCommitQuery.setParameter(REPOSITORY_ID_QUERY_PARAM, repositoryEntity.getId());
      getBranchByCommitQuery.setParameter(COMMIT_HASHES_QUERY_PARAM, commitShas);
      List<BranchEntity> branchEntities = getBranchByCommitQuery.list();

      if (branchEntities != null && !branchEntities.isEmpty()) {
        var errorMessage = new StringBuilder("Commits are associated with branch name : ");
        var count = 0;
        for (BranchEntity branchEntity : branchEntities) {
          errorMessage.append(branchEntity.getId().getBranch());
          if (count < branchEntities.size() - 1) {
            errorMessage.append(", ");
          }
          count++;
        }
        throw new ModelDBException(errorMessage.toString(), Code.FAILED_PRECONDITION);
      }

      String getTagsHql =
          "From TagsEntity te where te.id."
              + ModelDBConstants.REPOSITORY_ID
              + " = :repoId "
              + " AND te.commit_hash"
              + " IN (:commitHashes)";
      Query<TagsEntity> getTagsQuery = session.createQuery(getTagsHql, TagsEntity.class);
      getTagsQuery.setParameter(REPO_ID_QUERY_PARAM, repositoryEntity.getId());
      getTagsQuery.setParameter(COMMIT_HASHES_QUERY_PARAM, commitShas);
      List<TagsEntity> tagsEntities = getTagsQuery.list();
      if (tagsEntities.size() > 0) {
        throw new ModelDBException(
            "Commit is associated with Tags : "
                + tagsEntities.stream()
                    .map(tagsEntity -> tagsEntity.getId().getTag())
                    .collect(Collectors.joining(",")),
            Code.FAILED_PRECONDITION);
      }

      session.beginTransaction();
      String getLabelsHql =
          "From LabelsMappingEntity lm where lm.id."
              + ModelDBConstants.ENTITY_HASH
              + " IN (:entityHashes) "
              + " AND lm.id."
              + ModelDBConstants.ENTITY_TYPE
              + " = :entityType";
      Query<LabelsMappingEntity> query =
          session
              .createQuery(getLabelsHql, LabelsMappingEntity.class)
              .setLockOptions(new LockOptions().setLockMode(LockMode.PESSIMISTIC_WRITE));
      query.setParameter("entityHashes", commitShas);
      query.setParameter("entityType", IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_VALUE);
      List<LabelsMappingEntity> labelsMappingEntities = query.list();
      for (LabelsMappingEntity labelsMappingEntity : labelsMappingEntities) {
        session.delete(labelsMappingEntity);
      }

      commitEntities.forEach(
          (commitEntity) -> {
            session.lock(commitEntity, LockMode.PESSIMISTIC_WRITE);
            if (commitEntity.getRepository().size() == 1) {
              String compositeId =
                  VersioningUtils.getVersioningCompositeId(
                      repositoryEntity.getId(),
                      commitEntity.getCommit_hash(),
                      Collections.singletonList(ModelDBConstants.DEFAULT_VERSIONING_BLOB_LOCATION));
              DeleteEntitiesCron.deleteLabels(
                  session, compositeId, IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB);
              DeleteEntitiesCron.deleteAttribute(session, compositeId);
              session.delete(commitEntity);
            } else {
              commitEntity.getRepository().remove(repositoryEntity);
              session.update(commitEntity);
            }
          });
      session.getTransaction().commit();
      return true;
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return deleteCommits(repositoryIdentification, commitShas, repositoryDAO);
      } else {
        throw ex;
      }
    }
  }

  /**
   * This add deletes a label on a commit. the commit being a datasetversion allows us to assume
   * that it will belong to a single repo.
   */
  @Override
  public DatasetVersion addDeleteDatasetVersionTags(
      RepositoryDAO repositoryDAO,
      BlobDAO blobDAO,
      MetadataDAO metadataDAO,
      boolean addTags,
      String datasetId,
      String datasetVersionId,
      List<String> tagsList,
      boolean deleteAll)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      var repositoryEntity =
          VersioningUtils.getDatasetRepositoryEntity(
              session, repositoryDAO, datasetId, datasetVersionId, true);
      addDeleteCommitLabels(
          repositoryEntity, datasetVersionId, metadataDAO, addTags, tagsList, deleteAll);
      return blobDAO.convertToDatasetVersion(
          repositoryDAO, metadataDAO, repositoryEntity, datasetVersionId, true);
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return addDeleteDatasetVersionTags(
            repositoryDAO,
            blobDAO,
            metadataDAO,
            addTags,
            datasetId,
            datasetVersionId,
            tagsList,
            deleteAll);
      } else {
        throw ex;
      }
    }
  }

  @Override
  public void addDeleteCommitLabels(
      RepositoryEntity repositoryEntity,
      String commitHash,
      MetadataDAO metadataDAO,
      boolean addLabels,
      List<String> labelsList,
      boolean deleteAll)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      String compositeId =
          VersioningUtils.getVersioningCompositeId(
              repositoryEntity.getId(),
              commitHash,
              Collections.singletonList(ModelDBConstants.DEFAULT_VERSIONING_BLOB_LOCATION));

      session.beginTransaction();
      var identificationType =
          IdentificationType.newBuilder()
              .setIdType(IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB)
              .setStringId(compositeId)
              .build();
      if (addLabels) {
        metadataDAO.addLabels(identificationType, ModelDBUtils.checkEntityTagsLength(labelsList));
      } else {
        metadataDAO.deleteLabels(
            identificationType, ModelDBUtils.checkEntityTagsLength(labelsList), deleteAll);
      }
      var commitEntity = getCommitEntity(session, commitHash, (session1 -> repositoryEntity));
      commitEntity.setDate_updated(new Date().getTime());
      commitEntity.increaseVersionNumber();
      session.update(commitEntity);
      session.getTransaction().commit();
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        addDeleteCommitLabels(
            repositoryEntity, commitHash, metadataDAO, addLabels, labelsList, deleteAll);
      } else {
        throw ex;
      }
    }
  }

  @Override
  public CommitPaginationDTO findCommits(
      FindRepositoriesBlobs request,
      UserInfo currentLoginUserInfo,
      boolean idsOnly,
      boolean rootSHAOnly,
      boolean isDatasetVersion,
      String sortKey,
      boolean ascending)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      var commitPaginationDTO =
          findCommits(
              session,
              request,
              currentLoginUserInfo,
              idsOnly,
              rootSHAOnly,
              isDatasetVersion,
              sortKey,
              ascending);
      commitPaginationDTO.setCommits(
          commitPaginationDTO.getCommitEntities().stream()
              .map(CommitEntity::toCommitProto)
              .collect(Collectors.toList()));
      return commitPaginationDTO;
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return findCommits(
            request,
            currentLoginUserInfo,
            idsOnly,
            rootSHAOnly,
            isDatasetVersion,
            sortKey,
            ascending);
      } else {
        throw ex;
      }
    }
  }

  /**
   * This method find the blobs supported based on the following conditions
   *
   * <p>commit.author, commit.label, tags, repoIds, commitHashList
   *
   * @param session :hibernate session
   * @param request : FindRepositoriesBlobs request
   * @param currentLoginUserInfo : current login userInfo
   * @return {@link CommitPaginationDTO} : "result", "count" as a key
   */
  @Override
  public CommitPaginationDTO findCommits(
      Session session,
      FindRepositoriesBlobs request,
      UserInfo currentLoginUserInfo,
      boolean idsOnly,
      boolean rootSHAOnly,
      boolean isDatasetVersion,
      String sortKey,
      boolean ascending)
      throws ModelDBException {
    List<KeyValueQuery> predicates = new ArrayList<>(request.getPredicatesList());
    for (KeyValueQuery predicate : predicates) {
      var predicateCase = predicate.getValue().getKindCase();
      if (predicate.getKey().equals(ModelDBConstants.ID)) {
        throw new ModelDBException(
            "predicates with ids not supported", Status.Code.INVALID_ARGUMENT);
      }
      if (predicate.getKey().isEmpty()) {
        throw new ModelDBException(
            "predicates with empty key not supported", Status.Code.INVALID_ARGUMENT);
      }
      if (predicateCase.equals(Value.KindCase.STRING_VALUE)
          && predicate.getValue().getStringValue().isEmpty()) {
        throw new ModelDBException(
            "Predicate does not contain string value in request", Status.Code.INVALID_ARGUMENT);
      }
      if (!predicateCase.equals(Value.KindCase.STRING_VALUE)
          && !predicateCase.equals(Value.KindCase.NUMBER_VALUE)
          && !predicateCase.equals(Value.KindCase.BOOL_VALUE)) {
        throw new ModelDBException(
            "Unknown 'Value' type recognized, valid 'Value' type are NUMBER_VALUE, STRING_VALUE, BOOL_VALUE",
            Status.Code.UNIMPLEMENTED);
      }

      if (predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE)
          || predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE_ID)
          || predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE_NAME)
          || predicate.getKey().equalsIgnoreCase(ModelDBConstants.WORKSPACE_TYPE)) {
        throw new ModelDBException(
            "Workspace name OR type not supported as predicate", Status.Code.INVALID_ARGUMENT);
      }
    }

    var modelDBServiceResourceTypes = ModelDBServiceResourceTypes.REPOSITORY;
    if (isDatasetVersion) {
      modelDBServiceResourceTypes = ModelDBServiceResourceTypes.DATASET;
    }

    Set<String> accessibleResourceIds =
        new HashSet<>(
            mdbRoleService.getAccessibleResourceIds(
                null,
                new CollaboratorUser(authService, currentLoginUserInfo),
                modelDBServiceResourceTypes,
                request.getRepoIdsList().stream()
                    .map(String::valueOf)
                    .collect(Collectors.toList())));

    String workspaceName = request.getWorkspaceName();
    LOGGER.debug("Workspace is : '{}'", workspaceName);
    if (!workspaceName.isEmpty()) {
      accessibleResourceIds =
          ModelDBUtils.filterWorkspaceOnlyAccessibleIds(
              mdbRoleService,
              accessibleResourceIds,
              workspaceName,
              currentLoginUserInfo,
              modelDBServiceResourceTypes);
    }

    if (accessibleResourceIds.isEmpty() && mdbRoleService.IsImplemented()) {
      LOGGER.debug("Accessible Repository Ids not found, size 0");
      var commitPaginationDTO = new CommitPaginationDTO();
      commitPaginationDTO.setCommitEntities(Collections.emptyList());
      commitPaginationDTO.setTotalRecords(0L);
      return commitPaginationDTO;
    }

    Set<String> commitHashList = new HashSet<>(request.getCommitsList());

    Map<String, Object> parametersMap = new HashMap<>();

    var alias = "cm";
    var rootQueryStringBuilder =
        new StringBuilder(" FROM ")
            .append(CommitEntity.class.getSimpleName())
            .append(" ")
            .append(alias)
            .append(" ");

    var joinClause = new StringBuilder();
    var repoAlias = "repo";
    joinClause.append(" INNER JOIN ").append(alias).append(".repository ").append(repoAlias);
    joinClause
        .append(" INNER JOIN ")
        .append(InternalFolderElementEntity.class.getSimpleName())
        .append(" folderElm ")
        .append(" ON ");
    joinClause.append("folderElm.folder_hash = ").append(alias).append(".rootSha ");

    List<String> whereClauseList = new ArrayList<>();

    if (!accessibleResourceIds.isEmpty()) {
      whereClauseList.add(repoAlias + ".id IN (:repoIds) ");
      parametersMap.put(
          "repoIds",
          accessibleResourceIds.stream().map(Long::valueOf).collect(Collectors.toList()));
    }

    if (!predicates.isEmpty()) {
      for (var index = 0; index < predicates.size(); index++) {
        KeyValueQuery predicate = predicates.get(index);
        String[] names = predicate.getKey().split("\\.");
        switch (names[0].toLowerCase()) {
          case ModelDBConstants.COMMIT:
            LOGGER.debug("switch case : commit");
            if (names[1].contains(ModelDBConstants.LABEL)) {
              StringBuilder subQueryBuilder =
                  new StringBuilder("SELECT lb.id.entity_hash FROM ")
                      .append(LabelsMappingEntity.class.getSimpleName())
                      .append(" lb WHERE ")
                      .append(" lb.id.entity_type ");
              RdbmsUtils.setValueWithOperatorInQuery(
                  index,
                  subQueryBuilder,
                  OperatorEnum.Operator.EQ,
                  IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_VALUE,
                  parametersMap);
              subQueryBuilder.append(" AND lb.id.label ");
              RdbmsUtils.setValueWithOperatorInQuery(
                  index,
                  subQueryBuilder,
                  OperatorEnum.Operator.EQ,
                  predicate.getValue().getStringValue(),
                  parametersMap);
              whereClauseList.add(alias + ".commit_hash IN (" + subQueryBuilder.toString() + ") ");
            } else if (names[1].toLowerCase().equals("author")) {
              var authorBuilder = new StringBuilder(alias + "." + names[1]);
              var operator = predicate.getOperator();
              if ((operator.equals(OperatorEnum.Operator.CONTAIN)
                  || operator.equals(OperatorEnum.Operator.NOT_CONTAIN))) {
                List<UserInfo> userInfoList = RdbmsUtils.getFuzzyUserInfos(authService, predicate);
                if (userInfoList != null && !userInfoList.isEmpty()) {
                  List<String> vertaIds =
                      userInfoList.stream()
                          .map(authService::getVertaIdFromUserInfo)
                          .collect(Collectors.toList());
                  String key = "fuzzy_owners_" + index;
                  if (operator.equals(OperatorEnum.Operator.NOT_CONTAIN)) {
                    authorBuilder.append(" NOT IN (:").append(key).append(") ");
                  } else {
                    authorBuilder.append(" IN (:").append(key).append(") ");
                  }
                  parametersMap.put(key, vertaIds);
                  whereClauseList.add(authorBuilder.toString());
                } else {
                  var commitPaginationDTO = new CommitPaginationDTO();
                  commitPaginationDTO.setCommitEntities(Collections.emptyList());
                  commitPaginationDTO.setTotalRecords(0L);
                  return commitPaginationDTO;
                }
              } else {
                VersioningUtils.setQueryParameters(index, authorBuilder, predicate, parametersMap);
                whereClauseList.add(authorBuilder.toString());
              }
            } else {
              throw new ModelDBException(
                  "Given predicate not supported yet : " + predicate, Code.UNIMPLEMENTED);
            }
            break;
          case ModelDBConstants.ATTRIBUTES:
            Map<String, Object> attrQueryParametersMap = new HashMap<>();
            StringBuilder attrQueryBuilder =
                new StringBuilder(
                        "SELECT attr.entity_hash From "
                            + AttributeEntity.class.getSimpleName()
                            + " attr where attr.")
                    .append(ModelDBConstants.KEY);
            RdbmsUtils.setValueWithOperatorInQuery(
                index,
                attrQueryBuilder,
                OperatorEnum.Operator.EQ,
                names[1],
                attrQueryParametersMap);
            attrQueryBuilder.append("AND attr.value ");
            RdbmsUtils.setValueWithOperatorInQuery(
                index,
                attrQueryBuilder,
                predicate.getOperator(),
                ModelDBUtils.getStringFromProtoObject(predicate.getValue()),
                attrQueryParametersMap);
            attrQueryBuilder.append("AND attr.field_type ");
            RdbmsUtils.setValueWithOperatorInQuery(
                index,
                attrQueryBuilder,
                OperatorEnum.Operator.EQ,
                ModelDBConstants.ATTRIBUTES,
                attrQueryParametersMap);
            attrQueryBuilder.append("AND attr.entity_name ");
            RdbmsUtils.setValueWithOperatorInQuery(
                index,
                attrQueryBuilder,
                OperatorEnum.Operator.EQ,
                ModelDBConstants.BLOB,
                attrQueryParametersMap);

            var attrQuery = session.createQuery(attrQueryBuilder.toString());
            attrQueryParametersMap.forEach(attrQuery::setParameter);
            LOGGER.debug(
                "Find attributes in datasetVersion final query : {}", attrQuery.getQueryString());
            List<String> attrEntityHashes = attrQuery.list();
            LOGGER.debug("Attributes in datasetVersion count: {}", attrEntityHashes.size());
            Set<String> attrCommitHashes = new HashSet<>();
            for (String blobHash : attrEntityHashes) {
              String[] compositeIdArr =
                  VersioningUtils.getDatasetVersionBlobCompositeIdString(blobHash);
              attrCommitHashes.add(compositeIdArr[1]);
            }
            if (!attrCommitHashes.isEmpty()) {
              whereClauseList.add(
                  String.format("%s.commit_hash IN (:attr_%s_CommitHashes)", alias, index));
              parametersMap.put(String.format("attr_%s_CommitHashes", index), attrCommitHashes);
            } else {
              var commitPaginationDTO = new CommitPaginationDTO();
              commitPaginationDTO.setCommitEntities(Collections.emptyList());
              commitPaginationDTO.setTotalRecords(0L);
              return commitPaginationDTO;
            }
            break;
          case ModelDBConstants.TAGS:
          case ModelDBConstants.BLOB:
            LOGGER.debug("switch case : Blob");
            StringBuilder subQueryBuilder =
                new StringBuilder("SELECT lb.id.entity_hash FROM ")
                    .append(LabelsMappingEntity.class.getSimpleName())
                    .append(" lb WHERE ")
                    .append(" lb.id.entity_type = :entityType");
            subQueryBuilder.append(" AND lower(lb.id.label) ");

            Map<String, Object> innerQueryParametersMap = new HashMap<>();
            if (predicate.getOperator().equals(OperatorEnum.Operator.NE)
                || predicate.getOperator().equals(OperatorEnum.Operator.NOT_CONTAIN)) {
              RdbmsUtils.setValueWithOperatorInQuery(
                  index,
                  subQueryBuilder,
                  predicate.getOperator().equals(OperatorEnum.Operator.NOT_CONTAIN)
                      ? OperatorEnum.Operator.CONTAIN
                      : OperatorEnum.Operator.EQ,
                  predicate.getValue().getStringValue().toLowerCase(),
                  innerQueryParametersMap);
            } else {
              RdbmsUtils.setValueWithOperatorInQuery(
                  index,
                  subQueryBuilder,
                  predicate.getOperator(),
                  predicate.getValue().getStringValue().toLowerCase(),
                  innerQueryParametersMap);
            }
            subQueryBuilder.append(" GROUP BY lb.id.entity_hash");
            var labelQuery = session.createQuery(subQueryBuilder.toString());
            labelQuery.setParameter(
                "entityType", IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB_VALUE);
            innerQueryParametersMap.forEach(labelQuery::setParameter);
            LOGGER.debug("Find tags OR blob final query : {}", labelQuery.getQueryString());
            List<String> blobHashes = labelQuery.list();
            LOGGER.debug("tags OR blob count : {}", blobHashes.size());
            Set<String> commitHashes = new HashSet<>();
            blobHashes.forEach(
                blobHash -> {
                  String[] compositeIdArr =
                      VersioningUtils.getDatasetVersionBlobCompositeIdString(blobHash);
                  commitHashes.add(compositeIdArr[1]);
                });
            LOGGER.debug(
                "tags OR blob in commit count : {}, commitHashes : {}",
                commitHashes.size(),
                commitHashes);
            if (!commitHashes.isEmpty()) {
              if (predicate.getOperator().equals(OperatorEnum.Operator.NE)
                  || predicate.getOperator().equals(OperatorEnum.Operator.NOT_CONTAIN)) {
                whereClauseList.add(
                    String.format("%s.commit_hash NOT IN (:label_%s_CommitHashes)", alias, index));
              } else {
                whereClauseList.add(
                    String.format("%s.commit_hash IN (:label_%s_CommitHashes)", alias, index));
              }
              parametersMap.put("label_" + index + "_CommitHashes", commitHashes);
            } else {
              var commitPaginationDTO = new CommitPaginationDTO();
              commitPaginationDTO.setCommitEntities(Collections.emptyList());
              commitPaginationDTO.setTotalRecords(0L);
              return commitPaginationDTO;
            }
            break;
          case ModelDBConstants.TIME_LOGGED:
          case ModelDBConstants.TIME_UPDATED:
          case ModelDBConstants.DATE_CREATED:
          case ModelDBConstants.DATE_UPDATED:
            String key = predicate.getKey();
            if (key.equals(ModelDBConstants.TIME_LOGGED)) {
              key = ModelDBConstants.DATE_CREATED;
            } else if (key.equals(ModelDBConstants.TIME_UPDATED)) {
              key = ModelDBConstants.DATE_UPDATED;
            }

            Double value = predicate.getValue().getNumberValue();
            var dateQueryBuilder = new StringBuilder(alias);
            dateQueryBuilder.append(".").append(key);
            RdbmsUtils.setValueWithOperatorInQuery(
                index, dateQueryBuilder, predicate.getOperator(), value.longValue(), parametersMap);
            whereClauseList.add(dateQueryBuilder.toString());
            break;
          case ModelDBConstants.VERSION:
            LOGGER.debug("switch case : Version");
            StringBuilder versionQueryBuilder =
                new StringBuilder("SELECT mpm.id.commitSha FROM ")
                    .append(MetadataPropertyMappingEntity.class.getSimpleName())
                    .append(" mpm WHERE ")
                    .append(" mpm.id.key = :key")
                    .append(" AND mpm.id.repositoryId IN (:repositoryId) ");
            versionQueryBuilder.append(" AND mpm.value ");

            Map<String, Object> versionInnerQueryParametersMap = new HashMap<>();
            versionInnerQueryParametersMap.put("key", ModelDBConstants.VERSION);
            versionInnerQueryParametersMap.put(
                REPOSITORY_ID_QUERY_PARAM,
                accessibleResourceIds.stream().map(Long::parseLong).collect(Collectors.toList()));
            Double version = predicate.getValue().getNumberValue();
            if (predicate.getOperator().equals(OperatorEnum.Operator.EQ)) {
              RdbmsUtils.setValueWithOperatorInQuery(
                  index,
                  versionQueryBuilder,
                  predicate.getOperator(),
                  String.valueOf(version.longValue()),
                  versionInnerQueryParametersMap);
            } else {
              throw new InvalidArgumentException(
                  "Operator EQ is only supported in predicate with `version` key");
            }
            versionQueryBuilder.append(" GROUP BY mpm.id.commitSha");
            var versionQuery = session.createQuery(versionQueryBuilder.toString());
            versionInnerQueryParametersMap.forEach(versionQuery::setParameter);
            LOGGER.debug("Find version blob final query : {}", versionQuery.getQueryString());
            List<String> versionCommitHashes = versionQuery.list();
            LOGGER.debug("version blob count : {}", versionCommitHashes.size());
            if (!versionCommitHashes.isEmpty()) {
              whereClauseList.add(
                  String.format("%s.commit_hash IN (:label_%s_CommitHashes)", alias, index));
              parametersMap.put(String.format("label_%s_CommitHashes", index), versionCommitHashes);
            } else {
              var commitPaginationDTO = new CommitPaginationDTO();
              commitPaginationDTO.setCommitEntities(Collections.emptyList());
              commitPaginationDTO.setTotalRecords(0L);
              return commitPaginationDTO;
            }
            break;
          default:
            throw new ModelDBException(
                "Invalid predicate found : " + predicate, Code.INVALID_ARGUMENT);
        }
      }
    }

    if (!commitHashList.isEmpty()) {
      whereClauseList.add(alias + ".commit_hash IN (:commitHashList)");
      parametersMap.put("commitHashList", commitHashList);
    }

    var whereClause = new StringBuilder();
    whereClause.append(
        VersioningUtils.setPredicatesWithQueryOperator(
            " AND ", whereClauseList.toArray(new String[0])));

    // Order by clause
    if (sortKey != null && !sortKey.isEmpty()) {
      if (sortKey.equals(ModelDBConstants.TIME_LOGGED)) {
        sortKey = ModelDBConstants.DATE_CREATED;
      } else if (sortKey.equals(ModelDBConstants.TIME_UPDATED)) {
        sortKey = ModelDBConstants.DATE_UPDATED;
      }
    } else {
      if (isDatasetVersion) {
        sortKey = ModelDBConstants.DATE_CREATED;
      } else {
        sortKey = ModelDBConstants.DATE_UPDATED;
      }
    }

    StringBuilder orderClause =
        new StringBuilder(" ORDER BY ")
            .append(alias)
            .append(".")
            .append(sortKey)
            .append(" ")
            .append(ascending ? "ASC" : "DESC");

    var finalQueryBuilder = new StringBuilder("SELECT ");
    if (idsOnly) {
      finalQueryBuilder.append(alias).append(".commit_hash ");
    } else if (rootSHAOnly) {
      finalQueryBuilder.append(alias).append(".rootSha ");
    } else {
      finalQueryBuilder.append(alias).append(" ");
    }
    finalQueryBuilder.append(rootQueryStringBuilder);
    finalQueryBuilder.append(joinClause);
    if (!whereClause.toString().isEmpty()) {
      finalQueryBuilder.append(" WHERE ").append(whereClause);
    }
    finalQueryBuilder.append(orderClause);

    // Build count query
    var countQueryBuilder = new StringBuilder();
    if (!joinClause.toString().isEmpty()) {
      countQueryBuilder.append("SELECT COUNT(").append(alias).append(") ");
    } else {
      countQueryBuilder.append("SELECT COUNT(*) ");
    }
    countQueryBuilder.append(rootQueryStringBuilder);
    countQueryBuilder.append(joinClause);
    if (!whereClause.toString().isEmpty()) {
      countQueryBuilder.append(" WHERE ").append(whereClause);
    }

    var query = session.createQuery(finalQueryBuilder.toString());
    LOGGER.debug("Find commits final query : {}", query.getQueryString());
    var countQuery = session.createQuery(countQueryBuilder.toString());
    if (!parametersMap.isEmpty()) {
      parametersMap.forEach(
          (key, value) -> {
            if (value instanceof List) {
              List<Object> objectList = (List<Object>) value;
              query.setParameterList(key, objectList);
              countQuery.setParameterList(key, objectList);
            } else {
              query.setParameter(key, value);
              countQuery.setParameter(key, value);
            }
          });
    }

    if (request.getPageNumber() != 0 && request.getPageLimit() != 0) {
      // Calculate number of documents to skip
      int skips = request.getPageLimit() * (request.getPageNumber() - 1);
      query.setFirstResult(skips);
      query.setMaxResults(request.getPageLimit());
    }

    List<CommitEntity> commitEntities;
    if (idsOnly || rootSHAOnly) {
      List<String> resultSet = query.list();
      commitEntities =
          resultSet.stream()
              .map(
                  selectedField -> {
                    var commitEntity = new CommitEntity();
                    if (idsOnly) {
                      commitEntity.setCommit_hash(selectedField);
                    } else if (rootSHAOnly) {
                      commitEntity.setRootSha(selectedField);
                    }
                    return commitEntity;
                  })
              .collect(Collectors.toList());
    } else {
      commitEntities = query.list();
    }
    LOGGER.debug("Final find commit count: {}", commitEntities.size());

    Long totalCount = (Long) countQuery.uniqueResult();
    LOGGER.debug("Find commit totalCount: {}", totalCount);
    var commitPaginationDTO = new CommitPaginationDTO();
    commitPaginationDTO.setCommitEntities(commitEntities);
    commitPaginationDTO.setTotalRecords(totalCount);
    return commitPaginationDTO;
  }

  /**
   * Check commit exists in the commit table by commitHash irrespective to repository
   *
   * @param session : session
   * @param commitHash : commit.commit_hash
   * @return {@link Boolean} : exists status
   */
  @Override
  public boolean isCommitExists(Session session, String commitHash) {
    var checkDatasetVersionExistsByIdHql =
        new StringBuilder("Select count(cm.commit_hash) From CommitEntity cm where ")
            .append(" cm.commit_hash = :commitHash ")
            .toString();
    var query = session.createQuery(checkDatasetVersionExistsByIdHql);
    query.setParameter("commitHash", commitHash);
    Long count = (Long) query.uniqueResult();
    return count > 0;
  }

  @Override
  public DatasetVersion updateDatasetVersionDescription(
      RepositoryDAO repositoryDAO,
      BlobDAO blobDAO,
      MetadataDAO metadataDAO,
      String datasetId,
      String datasetVersionId,
      String description)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      RepositoryEntity repositoryEntity;
      CommitEntity commitEntity = null;

      var repositoryIdentification = RepositoryIdentification.newBuilder();
      if (datasetId == null || datasetId.isEmpty()) {
        commitEntity =
            session.get(CommitEntity.class, datasetVersionId, LockMode.PESSIMISTIC_WRITE);

        if (commitEntity == null) {
          throw new ModelDBException("DatasetVersion not found", Code.NOT_FOUND);
        }

        if (commitEntity.getRepository() != null && commitEntity.getRepository().size() > 1) {
          throw new ModelDBException(
              String.format(
                  "DatasetVersion '%s' associated with multiple datasets",
                  commitEntity.getCommit_hash()),
              Code.INTERNAL);
        }
        Long newRepoId = new ArrayList<>(commitEntity.getRepository()).get(0).getId();
        repositoryIdentification.setRepoId(newRepoId);
      } else {
        repositoryIdentification.setRepoId(Long.parseLong(datasetId));
      }
      repositoryEntity =
          repositoryDAO.getProtectedRepositoryById(repositoryIdentification.build(), true);

      if (commitEntity == null) {
        commitEntity = getCommitEntity(session, datasetVersionId, (session1 -> repositoryEntity));
        session.lock(commitEntity, LockMode.PESSIMISTIC_WRITE);
      }
      String compositeId =
          VersioningUtils.getVersioningCompositeId(
              repositoryEntity.getId(),
              commitEntity.getCommit_hash(),
              Collections.singletonList(ModelDBConstants.DEFAULT_VERSIONING_BLOB_LOCATION));

      session.beginTransaction();
      metadataDAO.addProperty(
          session,
          IdentificationType.newBuilder()
              .setIdType(IDTypeEnum.IDType.VERSIONING_REPO_COMMIT_BLOB)
              .setStringId(compositeId)
              .build(),
          "description",
          description);
      commitEntity.setDate_updated(new Date().getTime());
      commitEntity.increaseVersionNumber();
      session.update(commitEntity);
      session.getTransaction().commit();
      return blobDAO.convertToDatasetVersion(
          repositoryDAO, metadataDAO, repositoryEntity, datasetVersionId, true);
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return updateDatasetVersionDescription(
            repositoryDAO, blobDAO, metadataDAO, datasetId, datasetVersionId, description);
      } else {
        throw ex;
      }
    }
  }

  @Override
  public DatasetVersion getDatasetVersionById(
      RepositoryDAO repositoryDAO,
      BlobDAO blobDAO,
      MetadataDAO metadataDAO,
      String datasetVersionId)
      throws ModelDBException {
    try (var session = modelDBHibernateUtil.getSessionFactory().openSession()) {
      return blobDAO.convertToDatasetVersion(
          repositoryDAO, metadataDAO, null, datasetVersionId, false);
    } catch (Exception ex) {
      if (ModelDBUtils.needToRetry(ex)) {
        return getDatasetVersionById(repositoryDAO, blobDAO, metadataDAO, datasetVersionId);
      } else {
        throw ex;
      }
    }
  }
}
