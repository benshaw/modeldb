package ai.verta.modeldb.reconcilers;

import ai.verta.modeldb.common.futures.FutureJdbi;
import ai.verta.modeldb.common.reconcilers.ReconcileResult;
import ai.verta.modeldb.common.reconcilers.Reconciler;
import ai.verta.modeldb.common.reconcilers.ReconcilerConfig;
import java.util.AbstractMap;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;

public class UpdateExperimentTimestampReconcile
    extends Reconciler<AbstractMap.SimpleEntry<String, Long>> {

  public UpdateExperimentTimestampReconcile(
      ReconcilerConfig config, FutureJdbi futureJdbi, Executor executor) {
    super(
        config,
        LogManager.getLogger(UpdateExperimentTimestampReconcile.class),
        futureJdbi,
        executor,
        false);
  }

  @Override
  public void resync() {
    var fetchUpdatedExperimentIds =
        new StringBuilder("SELECT expr.experiment_id, MAX(expr.date_updated) AS max_date ")
            .append(" FROM experiment_run expr INNER JOIN experiment e ")
            .append(" ON e.id = expr.experiment_id AND e.date_updated < expr.date_updated ")
            .append(" GROUP BY expr.experiment_id")
            .toString();

    futureJdbi.useHandle(
        handle -> {
          handle
              .createQuery(fetchUpdatedExperimentIds)
              .setFetchSize(config.getMaxSync())
              .map(
                  (rs, ctx) -> {
                    var experimentId = rs.getString("expr.experiment_id");
                    var maxUpdatedDate = rs.getLong("max_date");
                    this.insert(new AbstractMap.SimpleEntry<>(experimentId, maxUpdatedDate));
                    return rs;
                  })
              .list();
        });
  }

  @Override
  protected ReconcileResult reconcile(
      Set<AbstractMap.SimpleEntry<String, Long>> updatedMaxDateMap) {
    logger.debug(
        "Reconciling update timestamp for experiments: "
            + updatedMaxDateMap.stream()
                .map(AbstractMap.SimpleEntry::getKey)
                .collect(Collectors.toList()));
    return futureJdbi
        .useHandle(
            handle -> {
              var updateExperimentTimestampQuery =
                  "UPDATE experiment SET date_updated = :updatedDate, version_number=(version_number + 1) WHERE id = :id";

              final var batch = handle.prepareBatch(updateExperimentTimestampQuery);
              for (AbstractMap.SimpleEntry<String, Long> updatedRecord : updatedMaxDateMap) {
                var id = updatedRecord.getKey();
                long updatedDate = updatedRecord.getValue();
                batch.bind("id", id).bind("updatedDate", updatedDate).add();
              }

              batch.execute();
            })
        .thenApply(unused -> new ReconcileResult(), executor)
        .get();
  }
}
