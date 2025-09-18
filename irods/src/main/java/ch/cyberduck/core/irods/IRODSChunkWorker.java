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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.high_level.io.IRODSDataObjectInputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectOutputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectStream;
import org.irods.irods4j.low_level.api.IRODSException;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

public class IRODSChunkWorker implements Runnable {

    private static final Logger log = LogManager.getLogger(IRODSChunkWorker.class);

    private final InputStream in;
    private final OutputStream out;
    private final long offset;
    private final long chunkSize;
    private final byte[] buffer;

    public IRODSChunkWorker(InputStream in, OutputStream out, long offset, long chunkSize, int bufferSize) {
        log.info("constructing iRODS chunk worker.");
        log.info("offset      = [{}]", offset);
        log.info("chunk size  = [{}]", chunkSize);
        log.info("buffer size = [{}]", bufferSize);
        this.in = in;
        this.out = out;
        this.offset = offset;
        this.chunkSize = chunkSize;
        this.buffer = new byte[bufferSize];
        log.info("iRODS chunk worker constructed.");
    }

    @Override
    public void run() {
        try {
            seek(in);
            seek(out);

            long remaining = chunkSize;
            while(remaining > 0) {
                int count = (int) Math.min(buffer.length, remaining);

                int bytesRead = in.read(buffer, 0, count);
                log.info("read [{}] of [{}] requested bytes from input stream.", bytesRead, count);
                if(-1 == bytesRead) {
                    break;
                }

                out.write(buffer, 0, bytesRead);
                log.info("wrote [{}] bytes to output stream.", bytesRead);
                remaining -= bytesRead;
            }

            log.info("total bytes remaining = [{}]", remaining);
            log.info("done. wrote [{}] of [{}] bytes to the replica.", chunkSize - remaining, chunkSize);
        }
        catch(IOException | IRODSException e) {
            log.error(e.getMessage());
        }
    }

    private void seek(InputStream in) throws IRODSException, IOException {
        if(in instanceof IRODSDataObjectInputStream) {
            IRODSDataObjectInputStream stream = (IRODSDataObjectInputStream) in;
            long totalOffset = offset;
            log.info("input stream: total offset = [{}]", totalOffset);
            while(totalOffset > 0) {
                long intermediateOffset = Math.min(totalOffset, Integer.MAX_VALUE);
                totalOffset -= intermediateOffset;
                log.info("input stream: offsetting by [{}]. remaining offset = [{}]", intermediateOffset, totalOffset);
                stream.seek((int) intermediateOffset, IRODSDataObjectStream.SeekDirection.CURRENT);
            }
        }
        else if(in instanceof FileInputStream) {
            log.info("input stream: seeking to position [{}]", offset);
            FileChannel fc = ((FileInputStream) in).getChannel().position(offset);
            log.info("input stream: position = [{}]", fc.position());
        }
    }

    private void seek(OutputStream out) throws IRODSException, IOException {
        if(out instanceof IRODSDataObjectOutputStream) {
            IRODSDataObjectOutputStream stream = (IRODSDataObjectOutputStream) out;
            long totalOffset = offset;
            log.info("output stream: total offset = [{}]", totalOffset);
            while(totalOffset > 0) {
                long intermediateOffset = Math.min(totalOffset, Integer.MAX_VALUE);
                totalOffset -= intermediateOffset;
                log.info("output stream: offsetting by [{}]. remaining offset = [{}]", intermediateOffset, totalOffset);
                stream.seek((int) intermediateOffset, IRODSDataObjectStream.SeekDirection.CURRENT);
            }
        }
        else if(out instanceof FileOutputStream) {
            log.info("output stream: seeking to position [{}]", offset);
            FileChannel fc = ((FileOutputStream) out).getChannel().position(offset);
            log.info("output stream: position = [{}]", fc.position());
        }
    }
}
