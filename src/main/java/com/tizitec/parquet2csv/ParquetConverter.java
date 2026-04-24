package com.tizitec.parquet2csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Core conversion logic. Reads any Parquet file using the low-level
 * {@link ParquetFileReader} API with a {@link GroupRecordConverter} —
 * fully generic and schema-driven. Hadoop classes are on the classpath
 * (parquet-hadoop imports them from its API) but no Hadoop runtime,
 * configuration, or filesystem is involved.
 *
 * <p>Type handling strategy:
 * <ul>
 *   <li>Primitive types (int32, int64, float, double, boolean, binary,
 *       fixed_len_byte_array, int96) → typed string via {@code group.getXxx()}
 *       or {@code group.getValueToString()}</li>
 *   <li>Complex types → compact JSON, with Parquet's
 *       {@code LIST}/{@code MAP} schema wrappers unwrapped:
 *       structs become JSON objects ({@code {"street":"Main","city":"NYC"}}),
 *       lists become JSON arrays ({@code ["red","blue"]}),
 *       maps become JSON objects keyed by the Parquet key
 *       ({@code {"env":"prod"}}). Nested complex types recurse. Serialization
 *       is skipped entirely when {@link ConversionConfig#skipComplex()} is {@code true}.</li>
 *   <li>Null / missing fields → configurable null placeholder at top level;
 *       {@code null} inside JSON structures</li>
 * </ul>
 *
 * <p>Logical types (DATE, TIME, TIMESTAMP, DECIMAL, UUID) are <b>not</b>
 * decoded into human-readable form — they come through as their underlying
 * physical type (e.g. TIMESTAMP_MILLIS → raw epoch long, DATE → days-since-epoch int).
 *
 * <p>Uses {@link LocalInputFile} for all file I/O — pure Java NIO,
 * no {@code HADOOP_HOME}, no {@code winutils.exe} required on any platform.
 */
public class ParquetConverter {

    private static final Logger log = LoggerFactory.getLogger(ParquetConverter.class);

    private static final ObjectMapper JSON = new ObjectMapper();

    private final ConversionConfig config;

    public ParquetConverter(ConversionConfig config) {
        this.config = config;
    }

    /**
     * Converts a single Parquet file to a CSV file in the configured output directory.
     *
     * @param parquetFile path to the source .parquet file
     * @return result object carrying stats (row count, columns, duration, output path)
     * @throws IOException if reading or writing fails
     */
    public ConversionResult convert(Path parquetFile) throws IOException {
        Instant start   = Instant.now();
        Path    csvFile = resolveCsvOutputPath(parquetFile);
        String  name    = parquetFile.getFileName().toString();

        // ── Pre-flight ────────────────────────────────────────────────────────
        if (Files.exists(csvFile) && !config.overwrite()) {
            throw new FileAlreadyExistsException(
                    csvFile.toString(), null,
                    "CSV already exists. Use --overwrite to replace it."
            );
        }

        long inputBytes = Files.size(parquetFile);
        log.info("┌─ Starting  : {}", name);
        log.info("│  Input     : {}", parquetFile.toAbsolutePath());
        log.info("│  Output    : {}", csvFile.toAbsolutePath());
        log.info("│  File size : {}", formatBytes(inputBytes));

        // ── Open via pure-Java LocalInputFile (no Hadoop filesystem) ──────────
        try (ParquetFileReader fileReader = ParquetFileReader.open(new LocalInputFile(parquetFile))) {

            ParquetMetadata metadata   = fileReader.getFooter();
            MessageType     schema     = metadata.getFileMetaData().getSchema();
            List<String>    headers    = extractHeaders(schema);
            int             cols       = headers.size();
            long            totalRows  = fileReader.getRecordCount();

            if (totalRows == 0) {
                log.warn("│  ⚠ File is empty — writing header-only CSV.");
                writeHeaderOnlyCsv(csvFile, headers);
                Duration elapsed = Duration.between(start, Instant.now());
                log.info("└─ Done ({}) — 0 rows.", ProgressLogger.formatDuration(elapsed));
                return new ConversionResult(csvFile, 0, cols, elapsed);
            }

            log.info("│  Columns   : {} → {}", cols, headers);
            log.info("│  Rows      : {}", ProgressLogger.formatCount(totalRows));
            log.info("│  Progress  : every {} rows", ProgressLogger.formatCount(config.progressInterval()));

            CSVFormat      csvFormat = buildCsvFormat(headers);
            ProgressLogger progress  = new ProgressLogger(name, config.progressInterval());

            // schema-derived, safe to reuse across every row group
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

            // ── Iterate row groups → pages → records ──────────────────────────
            try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8);
                 CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {

                PageReadStore pages;

                while ((pages = fileReader.readNextRowGroup()) != null) {
                    long rowsInGroup = pages.getRowCount();
                    RecordReader<Group> recordReader = columnIO.getRecordReader(
                            pages, new GroupRecordConverter(schema)
                    );

                    for (long i = 0; i < rowsInGroup; i++) {
                        Group record = recordReader.read();
                        printer.printRecord(toStringValues(record, schema));
                        progress.tick();
                    }
                }
            }

            // ── Summary ───────────────────────────────────────────────────────
            progress.done();

            Duration elapsed     = Duration.between(start, Instant.now());
            long     outputBytes = Files.size(csvFile);

            log.info("│  CSV size  : {}", formatBytes(outputBytes));
            log.info("└─ Completed : {} in {}", name, ProgressLogger.formatDuration(elapsed));

            return new ConversionResult(csvFile, progress.rowCount(), cols, elapsed);
        }
    }

    // ─── Result record ────────────────────────────────────────────────────────

    /**
     * Carries per-file conversion stats back to the command layer.
     */
    public record ConversionResult(
            Path     outputPath,
            long     rowCount,
            int      columnCount,
            Duration elapsed
    ) {}

    // ─── Schema extraction ────────────────────────────────────────────────────

    /**
     * Extracts the ordered list of column names from the Parquet schema.
     * Complex fields are included unless {@link ConversionConfig#skipComplex()} is set.
     *
     * @param schema the Parquet message schema read from the file footer
     * @return ordered list of column names for the CSV header
     */
    private List<String> extractHeaders(MessageType schema) {
        return schema.getFields().stream()
                .filter(f -> !shouldSkip(f))
                .map(Type::getName)
                .toList();
    }

    /**
     * Returns true if the field should be excluded because it is a complex/group type
     * and {@link ConversionConfig#skipComplex()} is enabled.
     *
     * @param field the Parquet schema field to check
     * @return true if the field should be omitted from output
     */
    private boolean shouldSkip(Type field) {
        return config.skipComplex() && !field.isPrimitive();
    }

    // ─── Record serialization ─────────────────────────────────────────────────

    /**
     * Converts a Parquet {@link Group} (one row) into an ordered list of String values,
     * one per included column, in schema field order.
     *
     * @param group  the row record read from the Parquet file
     * @param schema the message schema defining field order and types
     * @return ordered list of serialized field values
     */
    private List<String> toStringValues(Group group, MessageType schema) {
        List<String> values = new ArrayList<>(schema.getFieldCount());

        int fieldIdx = 0;
        for (Type field : schema.getFields()) {
            if (shouldSkip(field)) { fieldIdx++; continue; }

            // Check if the field has any value in this row (handles nulls/optional fields)
            if (group.getFieldRepetitionCount(fieldIdx) == 0) {
                values.add(config.nullValue());
            } else {
                values.add(serializeField(group, field, fieldIdx));
            }
            fieldIdx++;
        }
        return values;
    }

    /**
     * Serializes a single field value from a top-level {@link Group} row.
     *
     * <p>Primitive types are read with the appropriate typed accessor.
     * Complex (group) types are converted to compact JSON via
     * {@link #groupToJson(Group, GroupType)}.
     *
     * @param group    the row containing the field
     * @param field    the schema definition of the field
     * @param fieldIdx the integer index of the field within the schema
     * @return a non-null string representation
     */
    private String serializeField(Group group, Type field, int fieldIdx) {
        if (field.isPrimitive()) {
            return primitiveToString(group, field, fieldIdx, 0);
        }
        JsonNode node = groupToJson(group.getGroup(fieldIdx, 0), field.asGroupType());
        try {
            return JSON.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            // Only reachable via a misconfigured ObjectMapper — we build the tree
            // ourselves from trusted nodes, so this should not happen in practice.
            throw new IllegalStateException(
                    "JSON serialization failed for field '" + field.getName() + "'", e);
        }
    }

    /**
     * Reads a primitive Parquet field as a plain string (no JSON quoting).
     * Used for top-level CSV cells and for Parquet MAP keys.
     *
     * @param group    the row (or nested group) containing the field
     * @param field    the schema field (must be primitive)
     * @param fieldIdx the integer index of the field within its containing group
     * @param repIdx   the repetition index within the field
     * @return string representation of the primitive value
     */
    private String primitiveToString(Group group, Type field, int fieldIdx, int repIdx) {
        PrimitiveTypeName typeName = field.asPrimitiveType().getPrimitiveTypeName();
        return switch (typeName) {
            case INT32   -> String.valueOf(group.getInteger(fieldIdx, repIdx));
            case INT64   -> String.valueOf(group.getLong(fieldIdx, repIdx));
            case FLOAT   -> String.valueOf(group.getFloat(fieldIdx, repIdx));
            case DOUBLE  -> String.valueOf(group.getDouble(fieldIdx, repIdx));
            case BOOLEAN -> String.valueOf(group.getBoolean(fieldIdx, repIdx));
            // BINARY covers UTF8 strings, ENUMs, DECIMAL-as-bytes, JSON, BSON
            // FIXED_LEN_BYTE_ARRAY covers fixed decimals, UUIDs
            // INT96 is a legacy 12-byte timestamp (Spark/Hive era)
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    group.getValueToString(fieldIdx, repIdx);
        };
    }

    // ─── JSON serialization of complex Parquet types ──────────────────────────

    /**
     * Converts a Parquet {@link Group} to a {@link JsonNode}, unwrapping
     * the {@code LIST} and {@code MAP} logical-type schema wrappers so the
     * JSON reflects the user-intended structure, not Parquet's storage shape.
     *
     * @param group the group instance to convert
     * @param type  the schema type (must be a group type)
     * @return a JsonNode — ArrayNode for LIST, ObjectNode for MAP and struct
     */
    private JsonNode groupToJson(Group group, GroupType type) {
        LogicalTypeAnnotation anno = type.getLogicalTypeAnnotation();
        if (anno instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
            return listToJsonArray(group, type);
        }
        if (anno instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
            return mapToJsonObject(group, type);
        }
        return structToJsonObject(group, type);
    }

    /**
     * Unwraps a Parquet LIST group into a JSON array. Handles both the
     * modern 3-level layout ({@code list.element}) and the legacy 2-level
     * layout (repeated primitive directly).
     */
    private JsonNode listToJsonArray(Group group, GroupType listType) {
        ArrayNode arr = JSON.createArrayNode();
        Type repeatedField = listType.getType(0);
        int  repeatedIdx   = 0;
        int  reps          = group.getFieldRepetitionCount(repeatedIdx);

        if (repeatedField.isPrimitive()) {
            // Legacy 2-level LIST: repeated field IS the element
            for (int i = 0; i < reps; i++) {
                arr.add(primitiveToJsonNode(group, repeatedField, repeatedIdx, i));
            }
            return arr;
        }

        // Modern 3-level LIST: repeated group wraps a single "element" child
        GroupType wrapperType = repeatedField.asGroupType();
        Type      elementType = wrapperType.getType(0);
        for (int i = 0; i < reps; i++) {
            Group wrapper = group.getGroup(repeatedIdx, i);
            if (wrapper.getFieldRepetitionCount(0) == 0) {
                arr.add(NullNode.getInstance());
            } else if (elementType.isPrimitive()) {
                arr.add(primitiveToJsonNode(wrapper, elementType, 0, 0));
            } else {
                arr.add(groupToJson(wrapper.getGroup(0, 0), elementType.asGroupType()));
            }
        }
        return arr;
    }

    /**
     * Unwraps a Parquet MAP group into a JSON object. The key is stringified
     * (JSON requires string keys); the value recurses.
     */
    private JsonNode mapToJsonObject(Group group, GroupType mapType) {
        ObjectNode obj = JSON.createObjectNode();
        int kvIdx = 0;  // the single repeated "key_value" group
        int reps  = group.getFieldRepetitionCount(kvIdx);

        for (int i = 0; i < reps; i++) {
            Group     kv         = group.getGroup(kvIdx, i);
            GroupType kvType     = kv.getType();
            Type      keyField   = kvType.getType(0);
            Type      valueField = kvType.getType(1);

            String key = keyField.isPrimitive()
                    ? primitiveToString(kv, keyField, 0, 0)
                    : JSON.createObjectNode().toString();  // non-string keys: stable placeholder

            JsonNode valueNode;
            if (kv.getFieldRepetitionCount(1) == 0) {
                valueNode = NullNode.getInstance();
            } else if (valueField.isPrimitive()) {
                valueNode = primitiveToJsonNode(kv, valueField, 1, 0);
            } else {
                valueNode = groupToJson(kv.getGroup(1, 0), valueField.asGroupType());
            }
            obj.set(key, valueNode);
        }
        return obj;
    }

    /**
     * Converts a plain struct group (no LIST/MAP annotation) to a JSON object
     * with the Parquet field names as keys.
     */
    private JsonNode structToJsonObject(Group group, GroupType type) {
        ObjectNode obj = JSON.createObjectNode();
        int fieldIdx = 0;
        for (Type field : type.getFields()) {
            String fieldName = field.getName();
            if (group.getFieldRepetitionCount(fieldIdx) == 0) {
                obj.set(fieldName, NullNode.getInstance());
            } else if (field.isPrimitive()) {
                obj.set(fieldName, primitiveToJsonNode(group, field, fieldIdx, 0));
            } else {
                obj.set(fieldName, groupToJson(group.getGroup(fieldIdx, 0), field.asGroupType()));
            }
            fieldIdx++;
        }
        return obj;
    }

    /**
     * Reads a primitive Parquet value as a typed JSON node — numbers stay
     * numeric, booleans stay boolean, everything else becomes a string.
     */
    private JsonNode primitiveToJsonNode(Group group, Type field, int fieldIdx, int repIdx) {
        PrimitiveTypeName typeName = field.asPrimitiveType().getPrimitiveTypeName();
        return switch (typeName) {
            case INT32   -> IntNode.valueOf(group.getInteger(fieldIdx, repIdx));
            case INT64   -> LongNode.valueOf(group.getLong(fieldIdx, repIdx));
            case FLOAT   -> FloatNode.valueOf(group.getFloat(fieldIdx, repIdx));
            case DOUBLE  -> DoubleNode.valueOf(group.getDouble(fieldIdx, repIdx));
            case BOOLEAN -> BooleanNode.valueOf(group.getBoolean(fieldIdx, repIdx));
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    TextNode.valueOf(group.getValueToString(fieldIdx, repIdx));
        };
    }

    // ─── CSV helpers ──────────────────────────────────────────────────────────

    /**
     * Builds the Apache Commons CSV format driven by {@link ConversionConfig}.
     *
     * @param headers ordered column names for the CSV header row
     * @return configured {@code CSVFormat} instance
     */
    private CSVFormat buildCsvFormat(List<String> headers) {
        return CSVFormat.DEFAULT.builder()
                .setHeader(headers.toArray(String[]::new))
                .setDelimiter(config.delimiter())
                .setRecordSeparator(System.lineSeparator())
                .setNullString(config.nullValue())
                .build();
    }

    /**
     * Writes a header-only CSV file for an empty Parquet input.
     *
     * @param csvFile the target CSV path
     * @param headers the column names to write as the header row
     * @throws IOException if the file cannot be written
     */
    private void writeHeaderOnlyCsv(Path csvFile, List<String> headers) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8);
             CSVPrinter printer = new CSVPrinter(writer, buildCsvFormat(headers))) {
            // header is written automatically by CSVFormat; no rows to add
        }
    }

    // ─── Path helpers ─────────────────────────────────────────────────────────

    /**
     * Derives the output CSV path: same base name as the input, {@code .parquet} → {@code .csv},
     * placed in the configured output directory.
     *
     * @param parquetFile the source Parquet file
     * @return the target CSV path (not yet created)
     */
    private Path resolveCsvOutputPath(Path parquetFile) {
        String originalName = parquetFile.getFileName().toString();
        String csvName      = originalName.replaceAll("(?i)\\.parquet$", ".csv");
        return config.outputDir().resolve(csvName);
    }

    // ─── Formatting helpers ───────────────────────────────────────────────────

    /**
     * Formats a byte count as a human-readable string with an appropriate unit suffix.
     *
     * @param bytes the byte count; negative values return {@code "unknown"}
     * @return a formatted string such as {@code "142.3 MB"}
     */
    static String formatBytes(long bytes) {
        if (bytes < 0)             return "unknown";
        if (bytes < 1_024)         return bytes + " B";
        if (bytes < 1_048_576)     return String.format("%.1f KB", bytes / 1_024.0);
        if (bytes < 1_073_741_824) return String.format("%.1f MB", bytes / 1_048_576.0);
        return                            String.format("%.2f GB", bytes / 1_073_741_824.0);
    }
}
