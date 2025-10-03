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

import ch.cyberduck.core.ListProgressListener;
import ch.cyberduck.core.Path;
import ch.cyberduck.core.PathAttributes;
import ch.cyberduck.core.exception.BackgroundException;
import ch.cyberduck.core.exception.NotfoundException;
import ch.cyberduck.core.features.AttributesAdapter;
import ch.cyberduck.core.features.AttributesFinder;
import ch.cyberduck.core.io.Checksum;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.high_level.catalog.IRODSQuery;
import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.low_level.api.IRODSException;

import java.io.IOException;
import java.util.List;

public class IRODSAttributesFinderFeature implements AttributesFinder, AttributesAdapter<List<String>> {

    private static final Logger log = LogManager.getLogger(IRODSAttributesFinderFeature.class);

    private final IRODSSession session;

    public IRODSAttributesFinderFeature(final IRODSSession session) {
        this.session = session;
    }

    @Override
    public PathAttributes find(final Path file, final ListProgressListener listener) throws BackgroundException {
        try {
            log.debug("looking up path attributes.");

            final IRODSConnection conn = session.getClient();
            final String logicalPath = file.getAbsolute();
            if(!IRODSFilesystem.exists(session.getClient().getRcComm(), logicalPath)) {
                throw new NotfoundException(file.getAbsolute());
            }

            log.debug("data object exists in iRODS. fetching data using GenQuery2.");
            String query = String.format(
                    "select DATA_CREATE_TIME, DATA_MODIFY_TIME, DATA_SIZE, DATA_CHECKSUM, DATA_REPL_STATUS where COLL_NAME = '%s' and DATA_NAME = '%s' order by DATA_REPL_STATUS desc, DATA_MODIFY_TIME desc",
                    FilenameUtils.getFullPathNoEndSeparator(logicalPath),
                    FilenameUtils.getName(logicalPath));
            List<List<String>> rows = IRODSQuery.executeGenQuery2(conn.getRcComm(), query);

            PathAttributes attrs = new PathAttributes();
            setAttributes(attrs, rows.get(0));
            return attrs;
        }
        catch(IOException | IRODSException e) {
            throw new IRODSExceptionMappingService().map("Failure to read attributes of {0}", e, file);
        }
    }

    @Override
    public PathAttributes toAttributes(final List<String> row) {
        PathAttributes attrs = new PathAttributes();
        setAttributes(attrs, row);
        return attrs;
    }

    private static void setAttributes(final PathAttributes attrs, final List<String> row) {
        log.debug("path attribute info: created at [{}], modified at [{}], data size = [{}], checksum = [{}]",
                row.get(0), row.get(1), row.get(2), row.get(3));

        attrs.setCreationDate(Long.parseLong(row.get(0)) * 1000); // seconds to ms
        attrs.setModificationDate(Long.parseLong(row.get(1)) * 1000);
        attrs.setSize(Long.parseLong(row.get(2)));

        String checksum = row.get(3);
        if(!StringUtils.isEmpty(checksum)) {
            log.debug("checksum from iRODS server is [{}].", checksum);
            checksum = checksum.substring(checksum.indexOf(':') + 1);
            attrs.setChecksum(Checksum.parse(checksum));
        }
    }
}
