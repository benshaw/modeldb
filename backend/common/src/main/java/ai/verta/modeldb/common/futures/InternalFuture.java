package ai.verta.modeldb.common.futures;

import ai.verta.modeldb.common.exceptions.ModelDBException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.*;
import java.util.stream.Collectors;

public class InternalFuture<T> {
  private CompletionStage<T> stage;

  private InternalFuture() {}

  public static <T> InternalFuture<T> trace(
      Supplier<InternalFuture<T>> supplier,
      String operationName,
      Map<String, String> tags,
      Executor executor) {
    return supplier.get();
    //
    //        if (!GlobalTracer.isRegistered())
    //            return supplier.get();
    //
    //        final var tracer = GlobalTracer.get();
    //
    //        final var currentSpan = tracer.scopeManager().activeSpan();
    //        final var spanContext = TraceSupport.getActiveSpanContext(tracer);
    //        final var spanCreator = TraceSupport.createSpanFromParent(tracer, spanContext,
    // operationName, tags);
    //        final var scopeCreator = tracer.scopeManager().activate(spanCreator);
    //
    //        final var promise = new CompletableFuture<T>();
    //
    //        executor.execute(() -> {
    //            Context.current()
    //                    .withValue(OpenTracingContextKey.getKey(), spanCreator)
    //                    .withValue(OpenTracingContextKey.getSpanContextKey(),
    // spanCreator.context())
    //                    .attach();
    //            supplier.get().stage.whenCompleteAsync(
    //                (v, t) -> {
    //                  scopeCreator.close();
    //                  spanCreator.finish();
    //                  if (t != null) {
    //                    promise.completeExceptionally(t);
    //                  } else {
    //                    promise.complete(v);
    //                  }
    //                },
    //                executor);
    //
    //        });
    //
    //        tracer.scopeManager().activate(currentSpan);
    //
    //        return InternalFuture.from(promise);
  }

  // Convert a list of futures to a future of a list
  public static <T> InternalFuture<List<T>> sequence(
      final List<InternalFuture<T>> futures, Executor executor) {
    if (futures.isEmpty()) {
      return InternalFuture.completedInternalFuture(new LinkedList<>());
    }

    final var promise = new CompletableFuture<List<T>>();
    final var values = new ArrayList<T>(futures.size());
    final CompletableFuture<T>[] futuresArray =
        futures.stream()
            .map(x -> x.toCompletionStage().toCompletableFuture())
            .collect(Collectors.toList())
            .toArray(new CompletableFuture[futures.size()]);

    CompletableFuture.allOf(futuresArray)
        .whenCompleteAsync(
            (ignored, throwable) -> {
              if (throwable != null) {
                promise.completeExceptionally(throwable);
                return;
              }

              try {
                for (final var future : futuresArray) {
                  values.add(future.get());
                }
                promise.complete(values);
              } catch (RuntimeException | InterruptedException | ExecutionException t) {
                if (t instanceof InterruptedException) {
                  // Restore interrupted state...
                  Thread.currentThread().interrupt();
                }
                promise.completeExceptionally(t);
              }
            },
            executor);

    return InternalFuture.from(promise);
  }

  public static <R> InternalFuture<R> from(CompletionStage<R> other) {
    var ret = new InternalFuture<R>();
    ret.stage = other;
    return ret;
  }

  public static <R> InternalFuture<R> completedInternalFuture(R value) {
    var ret = new InternalFuture<R>();
    ret.stage = CompletableFuture.completedFuture(value);
    return ret;
  }

  public <U> InternalFuture<U> thenCompose(
      Function<? super T, InternalFuture<U>> fn, Executor executor) {
    return from(
        stage.thenComposeAsync(fn.andThen(internalFuture -> internalFuture.stage), executor));
  }

  public <U> InternalFuture<U> thenApply(Function<? super T, ? extends U> fn, Executor executor) {
    return from(stage.thenApplyAsync(fn, executor));
  }

  public <U> InternalFuture<U> handle(
      BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
    return from(stage.handleAsync(fn, executor));
  }

  public <U, V> InternalFuture<V> thenCombine(
      InternalFuture<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn,
      Executor executor) {
    return from(stage.thenCombineAsync(other.stage, fn, executor));
  }

  public InternalFuture<Void> thenAccept(Consumer<? super T> action, Executor executor) {
    return from(stage.thenAcceptAsync(action, executor));
  }

  public <U> InternalFuture<Void> thenRun(Runnable runnable, Executor executor) {
    return from(stage.thenRunAsync(runnable, executor));
  }

  public static <U> InternalFuture<Void> runAsync(Runnable runnable, Executor executor) {
    return from(CompletableFuture.runAsync(runnable, executor));
  }

  public static <U> InternalFuture<U> supplyAsync(Supplier<U> supplier, Executor executor) {
    return from(CompletableFuture.supplyAsync(supplier, executor));
  }

  public static <U> InternalFuture<U> failedStage(Throwable ex) {
    return from(CompletableFuture.failedFuture(ex));
  }

  public static <U> InternalFuture<U> retriableStage(
      Supplier<InternalFuture<U>> supplier,
      Function<Throwable, Boolean> retryChecker,
      Executor executor) {
    final var promise = new CompletableFuture<U>();

    supplier
        .get()
        .whenComplete(
            (value, throwable) -> {
              boolean retryCheckerFlag = retryChecker.apply(throwable);
              if (throwable == null) {
                promise.complete(value);
              } else if (retryCheckerFlag) {
                // If we should retry, then perform the retry and map the result of the future to
                // the current promise
                // This build up a chain of promises, which can consume memory. I couldn't figure
                // out how to do better
                // with Java constraints of final vars in lambdas and not using uninitialized
                // variables.
                retriableStage(supplier, retryChecker, executor)
                    .whenComplete(
                        (v, t) -> {
                          if (t == null) {
                            promise.complete(v);
                          } else {
                            promise.completeExceptionally(t);
                          }
                        },
                        executor);
              } else {
                promise.completeExceptionally(throwable);
              }
            },
            executor);

    return InternalFuture.from(promise);
  }

  public InternalFuture<T> whenComplete(
      BiConsumer<? super T, ? super Throwable> action, Executor executor) {
    return from(stage.whenCompleteAsync(action, executor));
  }

  public T get() {
    try {
      return stage.toCompletableFuture().get();
    } catch (ExecutionException ex) {
      throw new ModelDBException(ex);
    } catch (InterruptedException ex) {
      // Restore interrupted state...
      Thread.currentThread().interrupt();
      throw new ModelDBException(ex);
    }
  }

  public CompletionStage<T> toCompletionStage() {
    return stage;
  }
}
