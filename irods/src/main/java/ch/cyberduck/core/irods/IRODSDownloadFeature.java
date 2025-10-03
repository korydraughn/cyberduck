package ch.cyberduck.core.irods;

/*
 * Copyright (c) 2002-2025 iterate GmbH. All rights reserved.
 * https://cyberduck.io/
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

import ch.cyberduck.core.ConnectionCallback;
import ch.cyberduck.core.Local;
import ch.cyberduck.core.Path;
import ch.cyberduck.core.exception.BackgroundException;
import ch.cyberduck.core.exception.NotfoundException;
import ch.cyberduck.core.features.Download;
import ch.cyberduck.core.features.Read;
import ch.cyberduck.core.io.BandwidthThrottle;
import ch.cyberduck.core.io.StreamListener;
import ch.cyberduck.core.preferences.HostPreferencesFactory;
import ch.cyberduck.core.preferences.PreferencesReader;
import ch.cyberduck.core.transfer.TransferStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.IRODSConnectionPool;
import org.irods.irods4j.high_level.connection.IRODSConnectionPool.PoolConnection;
import org.irods.irods4j.high_level.io.IRODSDataObjectInputStream;
import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IRODSDownloadFeature implements Download {

    private static final Logger log = LogManager.getLogger(IRODSDownloadFeature.class);

    private final IRODSSession session;

    public IRODSDownloadFeature(final IRODSSession session) {
        this.session = session;
    }

    @Override
    public void download(final Read read, final Path file, final Local local, final BandwidthThrottle throttle,
                         final StreamListener listener, final TransferStatus status,
                         final ConnectionCallback callback) throws BackgroundException {
        try {
            final PreferencesReader preferences = HostPreferencesFactory.get(session.getHost());

            final RcComm primaryConn = session.getClient().getRcComm();
            final String logicalPath = file.getAbsolute();

            if(!IRODSFilesystem.exists(primaryConn, logicalPath)) {
                throw new NotfoundException(logicalPath);
            }

            final long dataObjectSize = IRODSFilesystem.dataObjectSize(primaryConn, logicalPath);

            log.info("status.getLength() = [{}]", status.getLength());
            log.info("local file         = [{}]", local.getAbsolute());
            log.info("logicalPath        = [{}]", logicalPath);

            // Transfer the bytes over multiple connections if the size of the local file
            // exceeds a certain threshold - e.g. 32MB.
            // TODO Consider making this configurable.
            if(dataObjectSize < 32L * TransferStatus.MEGA) { //preferences.getInteger("irods.parallel_transfer.size_threshold")) {
                log.info("data object is smaller than 32MB. performing single-threaded transfer.");

                byte[] buffer = new byte[(int) (4L * TransferStatus.MEGA)]; //preferences.getInteger("irods.parallel_transfer.rbuffer_size")];

                try(IRODSConnection conn = IRODSConnectionUtils.newConnection(session);
                    IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(conn.getRcComm(), local.getAbsolute());
                    FileOutputStream out = new FileOutputStream(logicalPath)) {
                    while(true) {
                        status.validate(); // Throws if transfer is cancelled.
                        int bytesRead = in.read(buffer);
                        if(bytesRead == -1) {
                            return;
                        }
                        listener.recv(bytesRead);
                        out.write(buffer, 0, bytesRead);
                        listener.sent(bytesRead);
                    }
                }
            }

            //
            // The data object is larger than the threshold so use parallel transfer.
            //

            log.info("local file is larger than 32MB. performing multi-threaded transfer.");

            // TODO Clamp the value so that users do not specify something ridiculous.
            final int threadCount = 3; //preferences.getInteger("irods.parallel_transfer.thread_count");
            log.info("thread count = [{}]; starting thread pool.", threadCount);
            final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            final long chunkSize = dataObjectSize / threadCount;
            final long remainingBytes = dataObjectSize % threadCount;
            log.info("chunk size      = [{}]", chunkSize);
            log.info("remaining bytes = [{}]", remainingBytes);

            final List<OutputStream> localFileStreams = new ArrayList<>();
            final List<IRODSDataObjectInputStream> irodsStreams = new ArrayList<>();

            log.info("launching connection pool with [{}] connections.", threadCount);
            try(IRODSConnectionPool pool = new IRODSConnectionPool(threadCount)) {
                status.validate(); // Throws if transfer is cancelled.

                IRODSConnectionUtils.startIRODSConnectionPool(session, pool);
                log.info("connection pool started.");

                try {
                    // Initialize streams.
                    for(int i = 0; i < threadCount; ++i) {
                        localFileStreams.add(new FileOutputStream(local.getAbsolute()));

                        // The pooled connection will never be returned to the pool. This is
                        // okay because after the transfer, no connection is reused.
                        PoolConnection conn = pool.getConnection();
                        irodsStreams.add(new IRODSDataObjectInputStream(conn.getRcComm(), logicalPath));
                    }

                    status.validate(); // Throws if transfer is cancelled.

                    // Holds handles to tasks running on the thread pool. This allows us to wait for
                    // all tasks to complete before shutting down everything.
                    List<Future<?>> tasks = new ArrayList<>();

                    // Launch remaining IO tasks.
                    log.info("launch parallel IO tasks.");
                    for(int i = 0; i < threadCount; ++i) {
                        tasks.add(executor.submit(new IRODSChunkWorker(
                                status,
                                listener,
                                irodsStreams.get(i),
                                localFileStreams.get(i),
                                i * chunkSize,
                                (threadCount - 1 == i) ? chunkSize + remainingBytes : chunkSize,
                                4 * 1024 * 1024//preferences.getInteger("irods.parallel_transfer.rbuffer_size")
                        )));
                    }

                    waitForTasksToComplete(tasks);
                }
                finally {
                    closeInputStreams(irodsStreams);
                    closeOutputStreams(localFileStreams);
                }
            }

            log.info("shutting down thread pool executor.");
            executor.shutdown();
            // TODO Make this configurable.
            executor.awaitTermination(5, TimeUnit.SECONDS);
            log.info("done.");
        }
        catch(Exception e) {
            throw new IRODSExceptionMappingService().map(e);
        }
    }

    @Override
    public boolean offset(final Path file) {
        return false;
    }

    @Override
    public Download withReader(final Read reader) {
        return this;
    }

    private static void closeOutputStreams(List<OutputStream> streams) {
        log.info("closing output streams.");

        streams.forEach(out -> {
            try {
                out.close();
            }
            catch(Exception e) { /* Ignored */ }
        });
    }

    private static void closeInputStreams(List<IRODSDataObjectInputStream> streams) {
        log.info("closing input streams.");

        streams.forEach(in -> {
            try {
                in.close();
            }
            catch(Exception e) { /* Ignored */ }
        });
    }

    private static void waitForTasksToComplete(List<Future<?>> tasks) {
        log.info("waiting for parallel IO tasks to finish.");
        for(Future<?> task : tasks) {
            try {
                task.get();
            }
            catch(Exception e) {
                log.error(e.getMessage());
            }
        }
        log.info("parallel IO tasks have finished.");
    }
}
