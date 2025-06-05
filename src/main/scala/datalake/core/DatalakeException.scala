package datalake.core

import org.apache.logging.log4j.{LogManager, Logger, Level}

class DatalakeException(msg: String, lvl: Level) extends Exception(msg) {
    @transient private lazy val logger = LogManager.getLogger(this.getClass)
    logger.log(lvl, msg, this)
}