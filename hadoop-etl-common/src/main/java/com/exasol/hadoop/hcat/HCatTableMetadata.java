package com.exasol.hadoop.hcat;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * All table metadata obtained from HCatalog table which are relevant
 * for the Hadoop ETL UDFs.
 */
public class HCatTableMetadata {

    // E.g. "hdfs://vm031.cos.dev.exasol.com:8020/user/hive/warehouse/albums_rc_multi_part"
    private String hdfsLocation = "";
    private List<HCatTableColumn> columns = new ArrayList<>();
    private List<HCatTableColumn> partitionColumns = new ArrayList<>();
    private String tableType = ""; // E.g. "MANAGED_TABLE" or "EXTERNAL_TABLE"
    // E.g. "org.apache.hadoop.hive.ql.io.RCFileInputFormat"
    private String inputFormatClass = "";
    // E.g. "org.apache.hadoop.hive.ql.io.RCFileOutputFormat"
    private String outputFormatClass = "";
    // E.g. "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
    private String serDeClass = "";
    private List<HCatSerDeParameter> serDeParameters = new ArrayList<>();

    public HCatTableMetadata(
            String hdfsLocation,
            List<HCatTableColumn> columns,
            List<HCatTableColumn> partitionColumns,
            String tableType,
            String inputFormatClass,
            String outputFormatClass,
            String serDeClass,
            List<HCatSerDeParameter> serDeParameters) {
        this.hdfsLocation = hdfsLocation;
        this.columns = columns;
        this.partitionColumns = partitionColumns;
        this.tableType = tableType;
        this.inputFormatClass = inputFormatClass;
        this.outputFormatClass = outputFormatClass;
        this.serDeClass = serDeClass;
        this.serDeParameters = serDeParameters;
    }

    @Override
    public String toString() {
        String sb =
            "HCatTableInfo: "
            + "\nhdfsLocation: "
            + hdfsLocation
            + "\ncolumns: "
            + columns
            + "\npartitionColumns: "
            + partitionColumns
            + "\ntableType: "
            + tableType
            + "\ninputFormatClass: "
            + inputFormatClass
            + "\noutputFormatClass: "
            + outputFormatClass
            + "\nserDeClass: "
            + serDeClass
            + "\nserDeParameters: "
            + serDeParameters;
        return sb;
    }

    public String getHdfsLocation() {
        return hdfsLocation;
    }

    /**
     * Returns a hdfs address.
     *
     * @return E.g. "hdfs://vm031.cos.dev.exasol.com:8020"
     */
    public String getHdfsAddress() {
        return extractAddressFromLocation(hdfsLocation);
    }

    public String getHdfsTableRootPath() {
        return extractPathFromLocation(hdfsLocation);
    }

    /**
     * Obtains address from a location.
     *
     * @param location E.g.
     *          "hdfs://vm031.cos.dev.exasol.com:8020/user/hive/warehouse/albums_rc_multi_part"
     * @return E.g. "hdfs://vm031.cos.dev.exasol.com:8020"
     */
    private static String extractAddressFromLocation(String location) {
        Pattern pattern = Pattern.compile("^.*://.*?/");
        Matcher matcher = pattern.matcher(location);
        if (!matcher.find()) {
            throw new RuntimeException(
                    "Could not extract hdfs server and port from this location: " + location);
        }
        String hdfsAddress = matcher.group();
        hdfsAddress = hdfsAddress.substring(0, hdfsAddress.length() - 1);
        return hdfsAddress;
    }

    /**
     * Obtains path from a location.
     *
     * @param location E.g.
     *     "hdfs://vm031.cos.dev.exasol.com:8020/user/hive/warehouse/albums_rc_multi_part"
     * @return E.g. "/user/hive/warehouse/albums_rc_multi_part"
     */
    private static String extractPathFromLocation(String location) {
        Pattern pattern = Pattern.compile("^.*://.*?/");
        Matcher matcher = pattern.matcher(location);
        // Remove the leading server address, to get something like
        // "/user/hive/warehouse/albums_rc_multi_part"
        return matcher.replaceAll("/");
    }

    public List<HCatTableColumn> getPartitionColumns() {
        return partitionColumns;
    }

    public List<HCatTableColumn> getColumns() {
        return columns;
    }

    public String getTableType() {
        return tableType;
    }

    public String getInputFormatClass() {
        return inputFormatClass;
    }

    public String getOutputFormatClass() {
        return outputFormatClass;
    }

    public String getSerDeClass() {
        return serDeClass;
    }

    public List<HCatSerDeParameter> getSerDeParameters() {
        return serDeParameters;
    }
}
