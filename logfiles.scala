package com.carmignac.icare.elysee

import java.sql.Timestamp
import com.carmignac.icare.elysee.log.LoggerUtils.{getServerTime, getServicePrincipal, listToJson, writeToLA}
import org.joda.time.DateTime

import java.lang

package object log {

  //case class TypeValue(`type`: String, value: String) // should we return a tuple rather then object creation?? I agree lol

  case class Message(mDetails: List[(String, String)] = Nil, mStep: String = "", mMessage: String = "", mStackTrace: String = "", mMessageCategory: String = "")

  case class Status(mFunctionalScope: String = "", mStep: String = "", mFunctionalExecutable: String = "")

  case class Summary(mResume: String = "", mOutcome: String = "")

  case class Init(mResume: String = "")

  case class DetailedLog(mStackTrace: String = "") //extends Message

  sealed abstract class Log() {

    var mGuid: String
    var mCategory: String = "Functional" // voir vincent
    var mLogTime: DateTime = getServerTime
    var mAccount: String = getServicePrincipal
    var mScope: String // functional technical etc
    var mSeverity: String = "" // LogLevel
    var mCallingMethod: String = ""
    var mMessage: Message = Message()
    var mExecutable: String = "" // pipeline name for example performance allocation // so the questionn is to look at where the pipeline name exists ??
    var mProgram: String = "Databricks" //job name maybe?
    var mStatus: Status = Status()
    var mSummary: Summary = Summary() // functional resumé at end of processing
    var mInit: Init = Init() // when you start any process
    var mTimeGenerated: DateTime = getServerTime // le moment la log a été inséré sur LA

    override def toString: String =
      s"""
         |{"Guid":"${mGuid}","Category":"${mCategory}","LogTime":"${mLogTime}","Account":"${mAccount}","Scope":"${mScope}","Severity":"${mSeverity}","CallingMethod":"${mCallingMethod}",
         |"Message_message":"${mMessage.mMessage}","Message_details":[${listToJson(mMessage.mDetails)}],"Message_messageCategory":"${mMessage.mMessageCategory}","Message_stackTrace":"${mMessage.mStackTrace}",
         |"Executable":"${mExecutable}","Program":"${mProgram}",
         |"Status_step":"${mStatus.mStep}","Status_functionalExecutable":"${mStatus.mFunctionalExecutable}","Status_functionalScope":"${mStatus.mFunctionalScope}","Step":"${mStatus.mStep}",
         |"Summary_outcome":"${mSummary.mOutcome}","Summary_resume":"${mSummary.mResume}",
         |"Init_resume":"${mInit.mResume}",
         |"TimeGenerated":"${mTimeGenerated}"
         |}
         |""".stripMargin.filter(_ >= ' ')

    def info()


  }


  case class TechnicalLog() extends Log {

    override var mGuid: String = this.mGuid
    mCategory = "Technical"
    override var mScope: String = "Technical"
    def trace(iCallingMethod: String, iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String, iMessageStackTrace: String,
              iFunctionalScope: String, iResume: String, iOutcome: String): Unit = {

      mSeverity = "Trace"
      mMessage = Message(mDetails = iMessageDetails, mMessage = iMessage, mMessageCategory = iMessageCategory, mStackTrace = iMessageStackTrace)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = iOutcome)
      mCallingMethod = iCallingMethod
      mInit = Init(iResume)
      mLogTime = getServerTime

      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }

    }

    def debug(iCallingMethod: String, iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String, iMessageStackTrace: String,
              iFunctionalScope: String, iResume: String, iOutcome: String): Unit = {

      mSeverity = "Debug"
      mMessage = Message(mDetails = iMessageDetails, mMessage = iMessage, mMessageCategory = iMessageCategory, mStackTrace = iMessageStackTrace)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = iOutcome)
      mCallingMethod = iCallingMethod
      mInit = Init(iResume)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    def info(iCallingMethod: String, iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String,
             iFunctionalScope: String, iResume: String, iOutcome: String): Unit = {


      mSeverity = "Information"
      mMessage = Message(mDetails = iMessageDetails, mMessage = iMessage, mMessageCategory = iMessageCategory)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = iOutcome)
      mCallingMethod = iCallingMethod
      mInit = Init(iResume)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    @throws[UnsupportedOperationException]
    override def info(): Unit = {
      throw new UnsupportedOperationException(""" method "info" with no parameters is not supported in a Technical log, please use info instead""")
    }

    def warn(iCallingMethod: String, iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String, iMessageStackTrace: String,
             iFunctionalScope: String, iResume: String, iOutcome: String): Unit = {


      mSeverity = "Warning"
      mMessage = Message(mDetails = iMessageDetails, mMessage = iMessage, mMessageCategory = iMessageCategory, mStackTrace = iMessageStackTrace)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = iOutcome)
      mCallingMethod = iCallingMethod
      mInit = Init(iResume)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    def error(iCallingMethod: String, iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String, iMessageStackTrace: String,
              iFunctionalScope: String, iResume: String, iOutcome: String): Unit = {


      mSeverity = "Critical"
      mMessage = Message(mDetails = iMessageDetails, mMessage = iMessage, mMessageCategory = iMessageCategory, mStackTrace = iMessageStackTrace)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = iOutcome)
      mCallingMethod = iCallingMethod
      mInit = Init(iResume)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    def critical(iCallingMethod: String, iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String, iMessageStackTrace: String,
                 iFunctionalScope: String, iResume: String, iOutcome: String): Unit = {


      mSeverity = "Critical"
      mMessage = Message(mDetails = iMessageDetails, mMessage = iMessage, mMessageCategory = iMessageCategory, mStackTrace = iMessageStackTrace)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = iOutcome)
      mCallingMethod = iCallingMethod
      mInit = Init(iResume)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }
  }

  object TechnicalLogger {

    var lTechnicalLog: TechnicalLog = null

    def getInstance(iGuid: String): TechnicalLog = {

      if (lTechnicalLog == null) {
        val lNewLog = TechnicalLog()
        lNewLog.mGuid = iGuid
        lNewLog

      } else {
        lTechnicalLog
      }
    }

    def getInstance(iGuid: String, iExecutable: String): TechnicalLog = {
      if (lTechnicalLog == null) {
        val lNewLog = TechnicalLog()
        lNewLog.mGuid = iGuid
        lNewLog.mExecutable = iExecutable
        lNewLog

      } else {
        lTechnicalLog
      }
    }
  }

  case class InitLog() extends Log {

    override var mGuid: String = this.mGuid
    override var mScope: String = "Init"
    def info(iStep: String, iMessage: String, iMessageDetails: List[(String, String)],
             iFunctionalScope: String, iResume: String): Unit = {


      mSeverity = "Information"
      mMessage = Message(mDetails = iMessageDetails, mMessage = iMessage)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mInit = Init(iResume)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    @throws[UnsupportedOperationException]
    override def info(): Unit = {
      throw new UnsupportedOperationException(""" method "info" with no parameters is not supported in an InitLog, please use info instead""")
    }

  }

  object InitLogger {

    var lInitLog: InitLog = null

    def getInstance(iGuid: String): InitLog = {

      if (lInitLog == null) {
        val lNewLog = InitLog()
        lNewLog.mGuid = iGuid
        lNewLog

      } else {
        lInitLog
      }
    }

    def getInstance(iGuid: String, iExecutable: String): InitLog = {
      if (lInitLog == null) {
        val lNewLog = InitLog()
        lNewLog.mGuid = iGuid
        lNewLog.mExecutable = iExecutable
        lNewLog

      } else {
        lInitLog
      }
    }
  }

  case class SummaryLog() extends Log {

    override var mGuid: String = this.mGuid
    override var mScope: String = "Summary"
    @throws[UnsupportedOperationException]
    override def info(): Unit = {
      throw new UnsupportedOperationException(""" method "info" with no parameters is not supported in an InitLog, please use info instead""")
    }

    def info(iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String,
             iFunctionalScope: String, iResume: String): Unit = {


      mSeverity = "Information"
      mMessage = Message(mDetails = iMessageDetails, mStep = iStep, mMessage = iMessage, mMessageCategory = iMessageCategory)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = "Success")
      mLogTime = getServerTime

      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    def warn(iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String,
             iFunctionalScope: String, iResume: String): Unit = {


      mSeverity = "Warning"
      mMessage = Message(mDetails = iMessageDetails, mStep = iStep, mMessage = iMessage, mMessageCategory = iMessageCategory)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = "Warning")
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    def error(iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String, iMessageStackTrace: String,
              iFunctionalScope: String, iResume: String): Unit = {


      mSeverity = "Error"
      mMessage = Message(mDetails = iMessageDetails, mStep = iStep, mMessage = iMessage, mMessageCategory = iMessageCategory, mStackTrace = iMessageStackTrace)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mSummary = Summary(mResume = iResume, mOutcome = "Failure")
      mLogTime = getServerTime

      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

  }

  object SummaryLogger {

    var lSummaryLog: SummaryLog = null

    def getInstance(iGuid: String): SummaryLog = {

      if (lSummaryLog == null) {
        val lNewLog = SummaryLog()
        lNewLog.mGuid = iGuid
        lNewLog

      } else {
        lSummaryLog
      }
    }

    def getInstance(iGuid: String, iExecutable: String): SummaryLog = {
      if (lSummaryLog == null) {
        val lNewLog = SummaryLog()
        lNewLog.mGuid = iGuid
        lNewLog.mExecutable = iExecutable
        lNewLog

      } else {
        lSummaryLog
      }
    }
  }

  case class FunctionalLog() extends Log {

    override var mGuid: String = this.mGuid
    override var mScope: String = "Functional"
    @throws[UnsupportedOperationException]
    override def info(): Unit = {
      throw new UnsupportedOperationException(""" method "info" with no parameters is not supported in an InitLog, please use info instead""")
    }

    def info(iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String,
             iFunctionalScope: String): Unit = {

      mSeverity = "Information"
      mMessage = Message(mDetails = iMessageDetails, mStep = iStep, mMessage = iMessage, mMessageCategory = iMessageCategory)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mLogTime = getServerTime

      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }


    def warn(iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String,
             iFunctionalScope: String): Unit = {


      mSeverity = "Warning"
      mMessage = Message(mDetails = iMessageDetails, mStep = iStep, mMessage = iMessage, mMessageCategory = iMessageCategory)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mLogTime = getServerTime

      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }


    def error(iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String,
              iFunctionalScope: String): Unit = {


      mSeverity = "Error"
      mMessage = Message(mDetails = iMessageDetails, mStep = iStep, mMessage = iMessage, mMessageCategory = iMessageCategory)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

    def critical(iStep: String, iMessage: String, iMessageDetails: List[(String, String)], iMessageCategory: String,
                 iFunctionalScope: String): Unit = {

      mSeverity = "Critical"
      mMessage = Message(mDetails = iMessageDetails, mStep = iStep, mMessage = iMessage, mMessageCategory = iMessageCategory)
      mStatus = Status(mFunctionalScope = iFunctionalScope, mStep = iStep)
      mLogTime = getServerTime


      try {
        writeToLA(this.toString)
      } catch {
        case e: Exception => println("Cannot write log to Log analystics due to:" +
          s"${e.printStackTrace()}")
      }
    }

  }

  object FunctionalLogger {

    var lFunctionalLog: FunctionalLog = null

    def getInstance(iGuid: String): FunctionalLog = {

      if (lFunctionalLog == null) {
        val lNewLog = FunctionalLog()
        lNewLog.mGuid = iGuid
        lNewLog

      } else {
        lFunctionalLog
      }
    }

    def getInstance(iGuid: String, iExecutable: String): FunctionalLog = {
      if (lFunctionalLog == null) {
        val lNewLog = FunctionalLog()
        lNewLog.mGuid = iGuid
        lNewLog.mExecutable = iExecutable
        lNewLog

      } else {
        lFunctionalLog
      }
    }
  }


}


