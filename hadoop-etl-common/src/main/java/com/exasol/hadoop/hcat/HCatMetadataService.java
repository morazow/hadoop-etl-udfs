package com.exasol.hadoop.hcat;

import java.security.PrivilegedExceptionAction;

import com.exasol.hadoop.WebHdfsAndHCatService;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;

import org.apache.hadoop.security.UserGroupInformation;

public class HCatMetadataService {

    /**
     * Retrieve the table metadata required for the Hadoop ETL UDFs from
     * HCatalog or webHCatalog.
     */
    public static HCatTableMetadata getMetadataForTable(
            final String dbName,
            final String tableName,
            final String hcatAddress,
            final String hcatUserOrServicePrincipal,
            final boolean useKerberos,
            final KerberosCredentials kerberosCredentials) throws Exception {

        UserGroupInformation ugi =
            useKerberos
            ? KerberosHadoopUtils.getKerberosUgi(kerberosCredentials)
            : UserGroupInformation.createRemoteUser(hcatUserOrServicePrincipal);
        HCatTableMetadata tableMeta = ugi.doAs(
                new PrivilegedExceptionAction<HCatTableMetadata>() {
                    public HCatTableMetadata run() throws Exception {
                        HCatTableMetadata tableMeta;
                        if (hcatAddress.toLowerCase().startsWith("thrift://")) {
                            // Get table metadata via faster native Hive Metastore API
                            tableMeta =
                                HiveMetastoreService.getTableMetadata(
                                        hcatAddress,
                                        dbName, tableName, useKerberos,
                                        hcatUserOrServicePrincipal);
                        } else {
                            // Get table metadata from webHCat
                            String responseJson =
                                WebHdfsAndHCatService.getExtendedTableInfo(
                                        hcatAddress,
                                        dbName,
                                        tableName,
                                        useKerberos
                                        ? kerberosCredentials.getPrincipal()
                                        : hcatUserOrServicePrincipal);
                            tableMeta = WebHCatJsonParser.parse(responseJson);
                        }
                        return tableMeta;
                    }
                });
        return tableMeta;
    }

    /**
     * Create a table partition, if it does not exist.
     *
     * <p>Returns if a partition was created.
     */
    public static boolean createTablePartitionIfNotExists(
            final String dbName,
            final String tableName,
            final String partitionName,
            final String hcatAddress,
            final String hcatUserOrServicePrincipal,
            final boolean useKerberos,
            final KerberosCredentials kerberosCredentials) throws Exception {

        UserGroupInformation ugi =
            useKerberos
            ? KerberosHadoopUtils.getKerberosUgi(kerberosCredentials)
            : UserGroupInformation.createRemoteUser(hcatUserOrServicePrincipal);
        boolean partitionCreated = ugi.doAs(
                new PrivilegedExceptionAction<Boolean>() {
                    public Boolean run() throws Exception {
                        return HiveMetastoreService.createPartitionIfNotExists(
                                hcatAddress,
                                useKerberos,
                                hcatUserOrServicePrincipal,
                                dbName,
                                tableName,
                                partitionName);
                    }
                });
        return partitionCreated;
    }

}
