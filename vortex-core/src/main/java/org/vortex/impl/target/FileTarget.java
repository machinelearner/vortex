package org.vortex.impl.target;

import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.domain.VTarget;
import org.vortex.help.Maps;
import org.vortex.help.Pair;
import org.vortex.query.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class FileTarget extends VTarget {

    public FileTarget(Settings settings) {
        super(settings);
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public Callable<Result> delete(BulkDeleteTaskQuery bulkDeleteTaskQuery) {
        return null;
    }

    @Override
    public Callable<Result> list(ListQuery listQuery) {
        return null;
    }

    @Override
    public Callable<Result> create(final CreateQuery createQuery) {
        return () -> {
            Map<String, Object> result = createQuery.object();
            String serializedObject = Maps.pairs(result).stream().map(Pair::toTupleString).collect(Collectors.joining("\n"));
            try {
                File fileToBeWritten = new File(createQuery.into());
                FileOutputStream streamToBeWritten = new FileOutputStream(fileToBeWritten);
                OutputStreamWriter streamWriter = new OutputStreamWriter(streamToBeWritten);
                streamWriter.write(serializedObject);
                Result success = Result.success("File create Query", Maps.<String, Object>map("query", createQuery.toJson()));
                LOGGER.info("File create successful for file name: {}", createQuery.into());
                LOGGER.debug("File create successful with result: {}", success.toJson());
                return success;
            } catch (Exception e) {
                Result failure = Result.failure("File Create", Maps.map("query", createQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                LOGGER.error("File create failed with error", e);
                LOGGER.debug("File create failed with result: {}", failure.toJson());
                return failure;
            }
        };
    }

    @Override
    public Callable<Result> update(UpdateQuery updateQuery) {
        return () -> {
            Map<String, Object> result = updateQuery.object();
            String serializedObject = Maps.pairs(result).stream().map(Pair::toTupleString).collect(Collectors.joining("\n"));
            try {
                File fileToBeWritten = new File(updateQuery.into());
                FileOutputStream streamToBeWritten = new FileOutputStream(fileToBeWritten, true);
                OutputStreamWriter streamWriter = new OutputStreamWriter(streamToBeWritten);
                streamWriter.append(serializedObject).append("\n");
                streamWriter.close();
                Result success = Result.success("File update Query", Maps.<String, Object>map("query", updateQuery.toJson()));
                LOGGER.info("File update successful for file name: {}", updateQuery.into());
                LOGGER.debug("File update successful with result: {}", success.toJson());
                return success;
            } catch (Exception e) {
                Result failure = Result.failure("File Update", Maps.map("query", updateQuery.toJson(), "errors", Arrays.asList(e.getMessage())));
                LOGGER.error("File update failed with error", e);
                LOGGER.debug("File update failed with result: {}", failure.toJson());
                return failure;
            }
        };
    }

    @Override
    public Callable<Result> count(ListQuery listQuery) {
        return null;
    }

    @Override
    public Callable<Result> delete(DeleteQuery deleteQuery) {
        return null;
    }
}
