package datalake.core

import org.apache.logging.log4j.{LogManager, Logger, Level}


class DatalakeException(msg: String, lvl: Level, logger: Logger) extends Throwable(msg){
    logger.log(lvl, msg, this)
}