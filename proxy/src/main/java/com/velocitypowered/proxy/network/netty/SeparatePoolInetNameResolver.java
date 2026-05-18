/*
 * Copyright (C) 2020-2023 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.network.netty;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.InetNameResolver;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@code InetNameResolver} that performs blocking DNS name lookups
 * in a separate thread, avoiding blocking the Netty threads for an extended period of time
 * and without the downsides of Netty's native DNS resolver.
 */
public final class SeparatePoolInetNameResolver extends InetNameResolver {

  private final ExecutorService resolveExecutor;
  private final InetNameResolver delegate;
  private final Cache<String, List<InetAddress>> cache;
  private final ConcurrentHashMap<String, CompletableFuture<InetAddress>> pendingSingleLookups;
  private final ConcurrentHashMap<String, CompletableFuture<List<InetAddress>>> pendingAllLookups;
  private AddressResolverGroup<InetSocketAddress> resolverGroup;

  /**
   * Creates a new instance of {@code SeparatePoolInetNameResolver}.
   *
   * @param executor the {@link EventExecutor} which is used to notify the listeners of the
   *                 {@link Future} returned by {@link #resolve(String)}
   */
  public SeparatePoolInetNameResolver(EventExecutor executor) {
    super(executor);
    this.resolveExecutor = Executors.newFixedThreadPool(
        Math.max(4, Runtime.getRuntime().availableProcessors()),
        new ThreadFactoryBuilder()
            .setNameFormat("Velocity DNS Resolver")
            .setDaemon(true)
            .build());
    this.delegate = new DefaultNameResolver(executor);
    this.cache = Caffeine.newBuilder()
        .expireAfterWrite(30, TimeUnit.SECONDS)
        .build();
    this.pendingSingleLookups = new ConcurrentHashMap<>();
    this.pendingAllLookups = new ConcurrentHashMap<>();
  }

  @Override
  protected void doResolve(String inetHost, Promise<InetAddress> promise) throws Exception {
    List<InetAddress> cached = cache.getIfPresent(inetHost);
    if (cached != null) {
      promise.trySuccess(cached.getFirst());
      return;
    }

    pendingSingleLookups.computeIfAbsent(inetHost, key -> {
      CompletableFuture<InetAddress> future = new CompletableFuture<>();
      try {
        resolveExecutor.execute(() -> {
          Promise<InetAddress> delegatePromise = executor().newPromise();
          delegatePromise.addListener(f -> {
            pendingSingleLookups.remove(key);
            if (f.isSuccess()) {
              InetAddress result = (InetAddress) f.getNow();
              cache.put(key, ImmutableList.of(result));
              future.complete(result);
            } else {
              future.completeExceptionally(f.cause());
            }
          });
          this.delegate.resolve(key, delegatePromise);
        });
      } catch (RejectedExecutionException e) {
        pendingSingleLookups.remove(key);
        future.completeExceptionally(e);
      }
      return future;
    }).whenComplete((result, ex) -> {
      if (ex != null) {
        promise.tryFailure(ex);
      } else {
        promise.trySuccess(result);
      }
    });
  }

  @Override
  protected void doResolveAll(String inetHost, Promise<List<InetAddress>> promise) throws Exception {
    List<InetAddress> cached = cache.getIfPresent(inetHost);
    if (cached != null) {
      promise.trySuccess(cached);
      return;
    }

    pendingAllLookups.computeIfAbsent(inetHost, key -> {
      CompletableFuture<List<InetAddress>> future = new CompletableFuture<>();
      try {
        resolveExecutor.execute(() -> {
          Promise<List<InetAddress>> delegatePromise = executor().newPromise();
          delegatePromise.addListener(f -> {
            pendingAllLookups.remove(key);
            if (f.isSuccess()) {
              List<InetAddress> result = (List<InetAddress>) f.getNow();
              cache.put(key, result);
              future.complete(result);
            } else {
              future.completeExceptionally(f.cause());
            }
          });
          this.delegate.resolveAll(key, delegatePromise);
        });
      } catch (RejectedExecutionException e) {
        pendingAllLookups.remove(key);
        future.completeExceptionally(e);
      }
      return future;
    }).whenComplete((result, ex) -> {
      if (ex != null) {
        promise.tryFailure(ex);
      } else {
        promise.trySuccess(result);
      }
    });
  }

  public void shutdown() {
    this.resolveExecutor.shutdown();
  }

  /**
   * Returns a view of this resolver as a AddressResolverGroup.
   *
   * @return a view of this resolver as a AddressResolverGroup
   */
  public AddressResolverGroup<InetSocketAddress> asGroup() {
    if (this.resolverGroup == null) {
      this.resolverGroup = new AddressResolverGroup<InetSocketAddress>() {
        @Override
        protected AddressResolver<InetSocketAddress> newResolver(EventExecutor executor) {
          return asAddressResolver();
        }
      };
    }
    return this.resolverGroup;
  }
}
