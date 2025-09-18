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
import ch.cyberduck.core.ProgressListener;
import ch.cyberduck.core.exception.BackgroundException;
import ch.cyberduck.core.features.Upload;
import ch.cyberduck.core.features.Write;
import ch.cyberduck.core.io.BandwidthThrottle;
import ch.cyberduck.core.io.Checksum;
import ch.cyberduck.core.io.StreamListener;
import ch.cyberduck.core.preferences.HostPreferencesFactory;
import ch.cyberduck.core.preferences.PreferencesReader;
import ch.cyberduck.core.transfer.TransferStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.high_level.connection.IRODSConnectionPool;
import org.irods.irods4j.high_level.connection.IRODSConnectionPool.PoolConnection;
import org.irods.irods4j.high_level.io.IRODSDataObjectOutputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectStream;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IRODSUploadFeature implements Upload<Checksum> {

    private static final Logger log = LogManager.getLogger(IRODSUploadFeature.class);

    private final IRODSSession session;

    public IRODSUploadFeature(final IRODSSession session) {
        this.session = session;
    }

    @Override
    public Checksum upload(final Path file, final Local local, final BandwidthThrottle throttle,
                           final ProgressListener progress, final StreamListener streamListener, final TransferStatus status,
                           final ConnectionCallback callback) throws BackgroundException {
        try {
            final PreferencesReader preferences = HostPreferencesFactory.get(session.getHost());

            final long fileSize = local.attributes().getSize();
            final String logicalPath = file.getAbsolute();

            log.info("status.getLength() = [{}]", status.getLength());
            log.info("fileSize           = [{}]", fileSize);
            log.info("local file         = [{}]", local.getAbsolute());
            log.info("logicalPath        = [{}]", logicalPath);

            // Transfer the bytes over multiple connections if the size of the local file
            // exceeds a certain threshold - e.g. 32MB.
            // TODO Consider making this configurable.
            if(fileSize < 32 * TransferStatus.MEGA) { //preferences.getInteger("irods.parallel_transfer.size_threshold")) {
                log.info("local file is smaller than 32MB. performing single-threaded transfer.");

                byte[] buffer = new byte[(int) (4 * TransferStatus.MEGA)]; //preferences.getInteger("irods.parallel_transfer.rbuffer_size")];
                boolean truncate = true;
                boolean append = false;

                try(FileInputStream in = new FileInputStream(local.getAbsolute());
                    // TODO Consider using a different iRODS connection for data transfers.
                    // We need to think about asynchronous operations. iRODS does not support
                    // simultaneous use of connections.
                    IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(
                            session.getClient().getRcComm(), logicalPath, truncate, append)) {
                    while(true) {
                        int bytesRead = in.read(buffer);
                        if(bytesRead == -1) {
                            break;
                        }
                        out.write(buffer, 0, bytesRead);
                    }
                }

                return null;
            }

            //
            // The data object is larger than the threshold so use parallel transfer.
            //

            log.info("local file is larger than 32MB. performing multi-threaded transfer.");

            // TODO Clamp the value so that users do not specify something ridiculous.
            final int threadCount = preferences.getInteger("irods.parallel_transfer.thread_count");
            log.info("thread count = [{}]; starting thread pool.", threadCount);
            final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            final long chunkSize = fileSize / threadCount;
            final long remainingBytes = fileSize % threadCount;
            log.info("chunk size      = [{}]", chunkSize);
            log.info("remaining bytes = [{}]", remainingBytes);

            final List<InputStream> localFileStreams = new ArrayList<>();
            final List<IRODSDataObjectOutputStream> irodsStreams = new ArrayList<>();

            log.info("launching connection pool with [{}] connections.", threadCount);
            try(IRODSConnectionPool pool = new IRODSConnectionPool(threadCount)) {
                IRODSConnectionUtils.startIRODSConnectionPool(session, pool);
                log.info("connection pool started.");

                try {
                    // Initialize streams.
                    String replicaToken = null;
                    long replicaNumber = -1;

                    for(int i = 0; i < threadCount; ++i) {
                        localFileStreams.add(new FileInputStream(local.getAbsolute()));

                        // The pooled connection will never be returned to the pool. This is
                        // okay because after the transfer, no connection is reused.
                        PoolConnection conn = pool.getConnection();

                        if(0 == i) {
                            log.info("opened primary iRODS stream.");
                            // The first iRODS output stream is the primary stream. The opened
                            // replica is always truncated upon success.
                            irodsStreams.add(new IRODSDataObjectOutputStream(
                                    conn.getRcComm(), logicalPath, true, false));
                            replicaToken = irodsStreams.get(0).getReplicaToken();
                            replicaNumber = irodsStreams.get(0).getReplicaNumber();
                            log.info("replica token  = [{}]", replicaToken);
                            log.info("replica number = [{}]", replicaNumber);
                        }
                        else {
                            log.info("opened secondary iRODS stream.");
                            irodsStreams.add(new IRODSDataObjectOutputStream(
                                    conn.getRcComm(), replicaToken, logicalPath, replicaNumber, false, false));
                        }
                    }

                    // Holds handles to tasks running on the thread pool. This allows us to wait for
                    // all tasks to complete before shutting down everything.
                    List<Future<?>> tasks = new ArrayList<>();

                    // Launch remaining IO tasks.
                    log.info("launch parallel IO tasks.");
                    for(int i = 0; i < threadCount; ++i) {
                        tasks.add(executor.submit(new IRODSChunkWorker(
                                localFileStreams.get(i),
                                irodsStreams.get(i),
                                i * chunkSize,
                                (threadCount - 1 == i) ? chunkSize + remainingBytes : chunkSize,
                                4 * 1024 * 1024//preferences.getInteger("irods.parallel_transfer.rbuffer_size")
                        )));
                    }

                    waitForTasksToComplete(tasks);
                }
                finally {
                    closeOutputStreams(irodsStreams);
                    closeInputStreams(localFileStreams);
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

        // TODO Make this configurable.
//      final String fingerprintValue = IRODSFilesystem.dataObjectChecksum(primaryConn, logicalPath);
//      return Checksum.parse(fingerprintValue);
        return null;
    }

//    @Override
//    public Write.Append append(final Path file, final TransferStatus status) throws BackgroundException {
//        return new Write.Append(status.isExists()).withStatus(status);
//    }

    @Override
    public Upload<Checksum> withWriter(final Write<Checksum> writer) {
        return this;
    }

    private static void closeOutputStreams(List<IRODSDataObjectOutputStream> streams) {
        log.info("closing output streams.");

        final IRODSDataObjectStream.OnCloseSuccess closeInstructions = new IRODSDataObjectStream.OnCloseSuccess();
        closeInstructions.updateSize = false;
        closeInstructions.updateStatus = false;
        closeInstructions.computeChecksum = false;
        closeInstructions.sendNotifications = false;

        for (int i = 1; i < streams.size(); ++i) {
            try {
                streams.get(i).close(closeInstructions);
            }
            catch(Exception e) { /* Ignored */ }
        }

        try {
            streams.get(0).close();
        }
        catch(Exception e) { /* Ignored */ }
    }

    private static void closeInputStreams(List<InputStream> streams) {
        log.info("closing input streams.");

        streams.forEach(out -> {
            try {
                out.close();
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
