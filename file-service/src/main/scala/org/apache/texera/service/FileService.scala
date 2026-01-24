package org.apache.texera.service

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.AuthDynamicFeature
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import org.apache.texera.amber.config.StorageConfig
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.{JwtAuthFilter, SessionUser}
import org.apache.texera.dao.SqlServer
import org.apache.texera.service.`type`.DatasetFileNode
import org.apache.texera.service.`type`.serde.DatasetFileNodeSerializer
import org.apache.texera.service.resource.{DatasetAccessResource, DatasetResource, DatasetUploadWebsocketResource, HealthCheckResource}
import org.apache.texera.service.util.S3StorageClient
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.websocket.jakarta.server.config.JakartaWebSocketServletContainerInitializer

import jakarta.websocket.server.ServerContainer
import java.nio.file.Path
import java.time.Duration

class FileService extends Application[FileServiceConfiguration] with LazyLogging {

  override def initialize(bootstrap: Bootstrap[FileServiceConfiguration]): Unit = {
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    val customSerializerModule = new SimpleModule("CustomSerializers")
    customSerializerModule.addSerializer(classOf[DatasetFileNode], new DatasetFileNodeSerializer())
    bootstrap.getObjectMapper.registerModule(customSerializerModule)
  }

  override def run(configuration: FileServiceConfiguration, environment: Environment): Unit = {
    environment.jersey.setUrlPattern("/api/*")

    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

    S3StorageClient.createBucketIfNotExist(StorageConfig.lakefsBucketName)
    LakeFSStorageClient.healthCheck()

    // Sessions (Servlet-side, not Jersey resources)
    environment.servlets.setSessionHandler(new SessionHandler)

    // WebSockets (Jakarta)
    JakartaWebSocketServletContainerInitializer.configure(
      environment.getApplicationContext,
      (_: jakarta.servlet.ServletContext, container: ServerContainer) => {
        container.setDefaultMaxSessionIdleTimeout(Duration.ofHours(1).toMillis)
        container.addEndpoint(classOf[DatasetUploadWebsocketResource])
      }
    )

    environment.jersey.register(classOf[HealthCheckResource])

    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))
    environment.jersey.register(new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser]))

    environment.jersey.register(classOf[DatasetResource])
    environment.jersey.register(classOf[DatasetAccessResource])
  }
}

object FileService {
  def main(args: Array[String]): Unit = {
    val configFilePath = Path
      .of(sys.env.getOrElse("TEXERA_HOME", "."))
      .resolve("file-service")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("file-service-web-config.yaml")
      .toAbsolutePath
      .toString

    new FileService().run("server", configFilePath)
  }
}
