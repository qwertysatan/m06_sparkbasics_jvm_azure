package utils

import org.apache.spark.sql.SparkSession
import utils.ConfigUtils.conf

object SparkUtils {

  def initSpark(): SparkSession = SparkSession.builder()
    .appName("hotels")
    .master(conf.getString("homework.spark.master"))
    .config("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
    .config("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    .config("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", conf.getString("homework.azure.client.id"))
    .config("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", conf.getString("homework.azure.client.secret"))
    .config("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", conf.getString("homework.azure.client.endpoint"))
    .getOrCreate()

}
