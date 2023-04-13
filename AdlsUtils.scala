package com.carmignac.icare.elysee.utils

import com.azure.identity.AzureCliCredentialBuilder
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.carmignac.icare.elysee.data.RawFileValidator
import com.carmignac.icare.elysee.tools.conf.tables.FileDescriptors.FileValidator
import com.carmignac.icare.elysee.tools.conf.tables.SchemaFactory
import com.carmignac.icare.elysee.tools.conf.tables.TablesDescriptors.TableDescriptor
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDateTime

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.util.Try
import scala.util.control.NonFatal


object AdlsUtils extends KeyVaultUtils with SparkObject {

  private val saEdm01Name: String = getSecret("EDM-SA-STORAGE-ACCOUNT-NAME")
  private val saEdm01Token: String = getSecret("EDM-SA-EDM01-STORAGE-KEY")
  private val principalClientId: String = getSecret("EDM-DATABRICKS-SP-CLIENT-ID")
  private val principalSecret: String = getSecret("EDM-DATABRICKS-SP-CLIENT-SECRET")
  private val tenantId: String = getSecret("EDM-APIM-AAD-ID")

  /**
   *
   * @param filePath e.g: /raw/datasource/dataset/filename
   * @return
   */
  def buildPath(filePath: String): String =
    if(sys.env("ENV_TYPE") == "local") {
      val filePathSplitted: Array[String] = filePath.split("/").filter(_.nonEmpty)
      val containerName: String = filePathSplitted.head
      val tailFilePath: String = filePathSplitted.drop(1).mkString("/")
      "abfss://" + s"$containerName@$saEdm01Name.dfs.core.windows.net/$tailFilePath".replaceAll("//", "/")
    } else {
      s"/mnt/$filePath".replaceAll("//", "/")
    }

  def mountContainer(containerName: String): Unit =
    if(sys.env("ENV_TYPE") == "local") {
      spark.conf.set(s"fs.azure.account.key.$saEdm01Name.dfs.core.windows.net", saEdm01Token)
    } else {
      val containerMount = "/mnt/" + containerName //mount point to create
      val containerSource = s"""abfss://$containerName@$saEdm01Name.dfs.core.windows.net/""" //url to access to the container
      val config = Map(
        "fs.azure.account.auth.type" -> "OAuth",
        "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id" -> principalClientId,
        "fs.azure.account.oauth2.client.secret" -> principalSecret,
        "fs.azure.account.oauth2.client.endpoint" -> s"https://login.microsoftonline.com/$tenantId/oauth2/token"
      ) //setup config to access to the container

      /** if the mountpoint doesn't exist create it. then return the mount point *** */
      if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(containerMount)) {
        dbutils.fs.mount(source = containerSource, mountPoint = containerMount, extraConfigs = config)
      }
      /** try {
        dbutils.fs.ls(containerMount)
      }
      catch {
        case NonFatal(_) =>
        try {
          dbutils.fs.unmount(containerMount)
          dbutils.fs.mount(
            source = containerSource,
            mountPoint = containerMount,
            extraConfigs = config
          )
          dbutils.fs.refreshMounts()
        }catch{
          case NonFatal(e) =>
          println(s"could not unmount/mount $containerName with exception $e")

        }
      }
      différent method ::*/
      Try(Try(dbutils.fs.ls(containerMount)).getOrElse({
        dbutils.fs.unmount(containerMount)
        dbutils.fs.mount(
          source = containerSource,
          mountPoint = containerMount,
          extraConfigs = config)
        dbutils.fs.refreshMounts()
      })).getOrElse(println(s"could not unmount/mount $containerName"))


    }

  def unmountContainer(containerName: String): String = {
    val containerMount = "/mnt/" + containerName //mount point created

    /** if the mount point exists. unmount it *** */

    if (dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(containerMount)) {
      dbutils.fs.unmount(containerMount)
      s"Datalake unmounted on $containerMount"
    } else {
      s"Datalake mounted on $containerMount does not exists"
    }
  }

  def extractInfoFromAbfssPath(abfssPath: String): Array[String] = {
    val pattern = """^abfss:\/\/(\w+)@(\w+)\.dfs\.core\.windows\.net(\/.*)""".r
    val file: Array[String] = abfssPath.split("/").last.split('.')

    val (fileName: String, extension: Option[String]) =
      if(file.length > 1) (file.dropRight(1).mkString("."), file.lastOption)
      else (file.head, None)

    abfssPath match {
      // g1 = containerName, g2 = adlsName, g3 = filePath
      case pattern(g1, g2, g3) => Array(g1, g2, g3, fileName, extension.orNull)
      case _ => Array[String]()
    }
  }

  def getBlobServiceClient(endpoint: String): BlobServiceClient = new BlobServiceClientBuilder()
    .endpoint(endpoint)
    .credential(new AzureCliCredentialBuilder().build())
    .buildClient()

  def getBlobNameList(blobContainerClient: BlobContainerClient, filePath: String): List[String] = blobContainerClient
    .listBlobsByHierarchy(filePath + "/") // if filePath doesn't have a "/" at the end, it cannot be considered as a folder
    .map(_.getName)
    .toList
    .filter(_ != filePath.drop(1)) // when filePath is a file, listBlobsByHierarchy return only the filePath as list without "/" at the beginning

  def isFileExists(path: String): Boolean = {
    if(sys.env("ENV_TYPE") == "local") {
      val Array(containerName: String, adlsName: String, filePath: String, _: String, _: String) = extractInfoFromAbfssPath(path)

      val endpoint: String = s"https://$adlsName.blob.core.windows.net/"
      val blobServiceClient: BlobServiceClient = getBlobServiceClient(endpoint)
      blobServiceClient.getBlobContainerClient(containerName).getBlobClient(filePath).exists()
    } else {
      Try(dbutils.fs.ls(path)).isSuccess
    }
  }

  def nbElementsInDirectory(folderPath: String): Int = {
    try {
      if(sys.env("ENV_TYPE") == "local") {
        val Array(containerName: String, adlsName: String, filePath: String, _*) = extractInfoFromAbfssPath(folderPath)
        val endpoint: String = s"https://$adlsName.blob.core.windows.net/"
        val blobServiceClient: BlobServiceClient = getBlobServiceClient(endpoint)
        getBlobNameList(blobServiceClient.getBlobContainerClient(containerName), filePath).size
      } else {
        dbutils.fs.ls(folderPath).size
      }
    } catch {
      case e: Exception => println(s"Folder doesn't exist. : $folderPath")
        0
    }
  }

  def isEmptyFolder(path: String): Boolean = nbElementsInDirectory(path) == 0

  @tailrec
  private def removeBlobRecursively(blobContainerClient: BlobContainerClient, blobByHierarchyList: List[String]): Boolean = {
    if(blobByHierarchyList.isEmpty) true
    else {
      blobContainerClient.getBlobClient(blobByHierarchyList.head).delete()
      removeBlobRecursively(blobContainerClient, blobByHierarchyList.tail)
    }
  }

  def removeFile(path: String, recurse: Boolean=false): Boolean = {
    if(sys.env("ENV_TYPE") == "local") {
      val Array(containerName: String, adlsName: String, filePath: String, _*) = extractInfoFromAbfssPath(path)

      val endpoint: String = s"https://$adlsName.blob.core.windows.net/"
      val blobContainerClient: BlobContainerClient = getBlobServiceClient(endpoint).getBlobContainerClient(containerName)
      val blobByHierarchyList: List[String] = getBlobNameList(blobContainerClient, filePath)
      val blobClient: BlobClient = blobContainerClient.getBlobClient(filePath)

      if(recurse && blobByHierarchyList.nonEmpty) {
        removeBlobRecursively(blobContainerClient, blobByHierarchyList)
        blobClient.delete()
        true
      } else if(!recurse && blobByHierarchyList.nonEmpty) {
        println("ERROR! The specified path is a folder.")
        false
      } else if(blobClient.exists()) {
        blobClient.delete()
        true
      } else if(recurse && blobByHierarchyList.isEmpty) {
        true
      } else {
        println("ERROR! Path doesn't exist.")
        false
      }
    } else {
      Try(dbutils.fs.rm(path, recurse = recurse)).isSuccess
    }
  }

  private def copyBlob(blobContainerClient: BlobContainerClient, inFilePath: String, outFilePath: String): Boolean = {
    val sourceBlobClient: BlobClient = blobContainerClient.getBlobClient(inFilePath)
    val destBlobClient: BlobClient = blobContainerClient.getBlobClient(outFilePath)
    Try(destBlobClient.beginCopy(sourceBlobClient.getBlobUrl, null)).isSuccess
  }

  @tailrec
  private def copyBlobs(blobContainerClient: BlobContainerClient, blobNameList: List[String], outFilePath: String): Boolean = {
    if(blobNameList.isEmpty) true
    else {
      val filePath: String = blobNameList.head
      copyBlob(blobContainerClient, filePath, s"$outFilePath/${filePath.split("/").last}")
      copyBlobs(blobContainerClient, blobNameList.drop(1), outFilePath)
    }
  }

  /**
   *
   * @param inPath abfss path - e.g: AdlsUtils.buildPath(...)
   * @param outPath abfss path - e.g: AdlsUtils.buildPath(...)
   */
  def copyBlob(inPath: String, outPath: String, recurse: Boolean=false): Boolean = {
    if (sys.env("ENV_TYPE") == "local") {
      val Array(inContainerName: String, inAdlsName: String, inFilePath: String, _*) = extractInfoFromAbfssPath(inPath)
      val Array(_: String, _: String, outFilePath: String, _*) = extractInfoFromAbfssPath(outPath)

      val endpoint: String = s"https://$inAdlsName.blob.core.windows.net/"
      val blobContainerClient: BlobContainerClient = getBlobServiceClient(endpoint).getBlobContainerClient(inContainerName)
      val blobNameList: List[String] = getBlobNameList(blobContainerClient, inFilePath)

      if (blobNameList.nonEmpty && recurse) copyBlobs(blobContainerClient, blobNameList, outFilePath)
      else if (blobNameList.nonEmpty && !recurse) {
        println("ERROR! The specified path is a folder.")
        false
      }
      else {
        Try(copyBlob(blobContainerClient, inFilePath, outFilePath)).getOrElse {
          println("ERROR! Path doesn't exist.")
          false
        }
      }
    } else {
      dbutils.fs.cp(inPath, outPath)
    }
  }

  /**
   *
   * @param inPath abfss path - e.g: AdlsUtils.buildPath(...)
   * @param outPath abfss path - e.g: AdlsUtils.buildPath(...)
   */
  def moveBlob(inPath: String, outPath: String, recurse: Boolean=false): Boolean = {

    if (sys.env("ENV_TYPE") == "local") {
      copyBlob(inPath, outPath, recurse)
      removeFile(inPath, recurse)
    } else {
      dbutils.fs.mv(inPath, outPath, recurse)
    }
  }

  def deepLs(path: String): Seq[String] = {
    dbutils.fs.ls(path).flatMap {
      fileOrFolder =>
        fileOrFolder match {
          case file if !file.path.endsWith("/") => Seq(file.path)
          case folder => deepLs(folder.path)
        }
    }
  }

  def fetchFileValidatorConf(datasource: String, dataset: String): FileValidator = {
    val confPath: String =
      if(sys.env("ENV_TYPE") == "local") getClass.getResource(s"/conf/dataset/$datasource/$dataset.json").getPath
      else s"/dbfs/mnt/configuration/dataset/$datasource/$dataset.json"

    SchemaFactory.getFileContentValidationConf(confPath)
  }

  def fetchStagingConf(datasource: String, dataset: String): TableDescriptor = {
    val confPath: String =
      if(sys.env("ENV_TYPE") == "local") getClass.getResource(s"/conf/dataset/$datasource/$dataset.json").getPath
      else s"/dbfs/mnt/configuration/dataset/$datasource/$dataset.json"

    SchemaFactory.getTableContentConf(confPath)
  }

  def isRawFileValidated(datasetPath: String, fileContentConf: FileValidator, guid: String): Boolean = {
    println("getRawFileValidatedAsDf")
    val rawValidator: RawFileValidator = new RawFileValidator(datasetPath, fileContentConf, guid, None)

    rawValidator.separatorCheck && rawValidator.schemaCheck && rawValidator.filenameCheck
  }

  def removeEmptyFolder(folderPath: String): Unit =
    if(isEmptyFolder(folderPath))
      removeFile(folderPath, true)
    else
      println(s"WARNING! Folder isn't empty or doesn't exist : $folderPath")

  def moveFileFromInToBadFolder(filePath: String): Unit = {
    println(LocalDateTime.now() + " MOVE_FILE_FROM_IN_TO_BAD_FOLDER")
    moveBlob(filePath, filePath.replace("/in/", "/bad/"), recurse = true)
  }

  def moveFileFromInToValidFolder(filePath: String): Unit = {
    println(LocalDateTime.now() + " MOVE_FILE_FROM_IN_TO_VALID_FOLDER")
    moveBlob(filePath, filePath.replace("/in/", "/valid/"), recurse = true)
    AdlsUtils.removeEmptyFolder(filePath.split("/").dropRight(1).mkString("/"))
  }

  def moveFileFromValidToArchiveFolder(filePath: String): Unit = {
    println(LocalDateTime.now() + " MOVE_FILE_FROM_VALID_TO_ARCHIVE_FOLDER")
    moveBlob(filePath, filePath.replace("/valid/", "/archive/"), recurse = true)
    AdlsUtils.removeEmptyFolder(filePath.split("/").dropRight(1).mkString("/"))
  }

  def writeXMLFile (filePath:String, dataframeToWrite:DataFrame,rootTag:String,rowTag:String) :Unit ={

    dataframeToWrite
      .repartition(1)
      .write.format("com.databricks.spark.xml")
      .option("rootTag", rootTag)
      .option("rowTag", rowTag)
      .option("declaration","version='1.0' encoding='utf-8'")
      .mode("overwrite")
      .save(filePath)

    val filepathxml=filePath+".xml"
    moveBlob(filePath+"/part-00000",filepathxml)
    removeFile(filePath+"/_SUCCESS")
    removeEmptyFolder(filePath)
  }
}
