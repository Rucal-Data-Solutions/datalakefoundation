package datalake.core

// import org.slf4j.{LoggerFactory, Logger}
// import org.slf4j.event.Level
import org.apache.log4j.{LogManager, Logger, Level}


class DatalakeException(msg: String, lvl: Level) extends Exception(msg){
    @transient lazy val logger = LogManager.getLogger(this.getClass())
    logger.error(msg, this)
}