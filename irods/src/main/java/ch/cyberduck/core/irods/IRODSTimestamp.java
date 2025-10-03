package ch.cyberduck.core.irods;

/*
 * Copyright (c) 2002-2025 iterate GmbH. All rights reserved.
 * https://cyberduck.io/
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

import ch.cyberduck.core.Path;
import ch.cyberduck.core.exception.BackgroundException;
import ch.cyberduck.core.features.Timestamp;
import ch.cyberduck.core.transfer.TransferStatus;

import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.low_level.api.IRODSException;

import java.io.IOException;

public class IRODSTimestamp implements Timestamp {

    private IRODSSession session;

    public IRODSTimestamp(IRODSSession session) {
        this.session = session;
    }

    @Override
    public void setTimestamp(final Path file, final TransferStatus status) throws BackgroundException {
        if(status.getModified() != null) {
            long seconds = Timestamp.toSeconds(status.getModified());
            try {
                IRODSFilesystem.lastWriteTime(session.getClient().getRcComm(), file.getAbsolute(), seconds);
            }
            catch(IOException | IRODSException e) {
                throw new IRODSExceptionMappingService().map(e);
            }
        }
    }

}
