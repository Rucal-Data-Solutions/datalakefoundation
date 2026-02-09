package datalake.core

import org.apache.logging.log4j.{LogManager, Logger, Level}
import datalake.log.DatalakeLogManager

class DatalakeException(msg: String, lvl: Level) extends Exception(msg) {
    @transient private lazy val logger = LogManager.getLogger(this.getClass)
    DatalakeLogManager.logException(logger, lvl, msg, this)
}