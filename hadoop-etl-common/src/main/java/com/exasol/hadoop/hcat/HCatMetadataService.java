package com.exasol.hadoop.hcat;

import com.exasol.hadoop.WebHdfsAndHCatService;
import com.exasol.hadoop.hive.HiveMetastoreService;
import com.exasol.hadoop.kerberos.KerberosCredentials;
import com.exasol.hadoop.kerberos.KerberosHadoopUtils;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.UserGroupInformation;

public class HCatMetadataService {

  /** Retrieve the table metadata required for the Hadoop ETL UDFs from HCatalog or webHCatalog */
  public static HCatTableMetadata getMetadataForTable(
      final String dbName,
      final String tableName,
      final String hCatAddress,
      final String hCatUserOrServicePrincipal,
      final boolean useKerberos,
      final KerberosCredentials kerberosCredentials)
      throws Exception {

    UserGroupInformation ugi =
        useKerberos
            ? KerberosHadoopUtils.getKerberosUGI(kerberosCredentials)
            : UserGroupInformation.createRemoteUser(hCatUserOrServicePrincipal);
    HCatTableMetadata tableMeta =
        ugi.doAs(
            new PrivilegedExceptionAction<HCatTableMetadata>() {
              public HCatTableMetadata run() throws Exception {
                HCatTableMetadata tableMeta;
                if (hCatAddress.toLowerCase().startsWith("thrift://")) {
                  // Get table metadata via faster native Hive Metastore API
                  tableMeta =
                      HiveMetastoreService.getTableMetadata(
                          hCatAddress, dbName, tableName, useKerberos, hCatUserOrServicePrincipal);
                } else {
                  // Get table metadata from webHCat
                  String responseJson =
                      WebHdfsAndHCatService.getExtendedTableInfo(
                          hCatAddress,
                          dbName,
                          tableName,
                          useKerberos
                              ? kerberosCredentials.getPrincipal()
                              : hCatUserOrServicePrincipal);
                  tableMeta = WebHCatJsonParser.parse(responseJson);
                }
                return tableMeta;
              }
            });
    return tableMeta;
  }

  /** Create a table partition, if it does not exist. Returns if a partition was created. */
  public static boolean createTablePartitionIfNotExists(
      final String dbName,
      final String tableName,
      final String partitionName,
      final String hCatAddress,
      final String hCatUserOrServicePrincipal,
      final boolean useKerberos,
      final KerberosCredentials kerberosCredentials)
      throws Exception {

    UserGroupInformation ugi =
        useKerberos
            ? KerberosHadoopUtils.getKerberosUGI(kerberosCredentials)
            : UserGroupInformation.createRemoteUser(hCatUserOrServicePrincipal);
    boolean partitionCreated =
        ugi.doAs(
            new PrivilegedExceptionAction<Boolean>() {
              public Boolean run() throws Exception {
                return HiveMetastoreService.createPartitionIfNotExists(
                    hCatAddress,
                    useKerberos,
                    hCatUserOrServicePrincipal,
                    dbName,
                    tableName,
                    partitionName);
              }
            });
    return partitionCreated;
  }
}
