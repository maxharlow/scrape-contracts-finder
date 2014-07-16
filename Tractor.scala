import java.io.File
import scala.xml.XML
import scala.collection.immutable.ListMap
import dispatch._
import dispatch.Defaults._
import com.github.tototoshi.csv.CSVWriter

object Tractor extends App {

  println("""
    ,---,
    |___|    .        ___ __   _   _  ___  _  __
    /``\ |---^-,_      |  |_) /_\ / `  |  / \ |_)
   ( () )  ( o )||     |  | \ | | \_,  |  \_/ | \
    \__/````\_/  \
  """)

  val http = Http.configure(_ setFollowRedirects true)

  val files = for {
    year <- 2011 to 2014
    month <- 1 to 12
  }
  yield http {
    val monthFormatted = "%02d".format(month)
    url(s"http://www.contractsfinder.businesslink.gov.uk/public_files/Notices/Monthly/notices_${year}_${monthFormatted}.xml") OK as.String
  }

  val bodies = for {
    file <- files
    data = file.apply()
    xml <- XML.loadString(data.splitAt(data.indexOf("<?xml"))._2)
    body <- xml \ "NOTICES" \ "_"
  }
  yield body

  val notices = bodies.flatten

  val awards = notices filter { notice =>
    notice.label == "CONTRACT_AWARD"
  }

  val awardsData = awards map { award =>
    ListMap(
      "noticeId" -> (award \ "SYSTEM" \ "NOTICE_ID").text,
      "referenceNumber" -> (award \ "FD_CONTRACT_AWARD" \ "PROCEDURE_DEFINITION_CONTRACT_AWARD_NOTICE" \ "ADMINISTRATIVE_INFORMATION_CONTRACT_AWARD" \ "FILE_REFERENCE_NUMBER").text,
      "publishedDate" -> (award \ "SYSTEM" \ "SYSTEM_PUBLISHED_DATE").text,
      "valueCurrency" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "TOTAL_FINAL_VALUE" \ "COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE" \ "@CURRENCY").text,
      "value" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "TOTAL_FINAL_VALUE" \ "COSTS_RANGE_AND_CURRENCY_WITH_VAT_RATE" \ "VALUE_COST").text,
      "status" -> (award \ "SYSTEM" \ "SYSTEM_NOTICE_STATE").text,
      "orgName" -> (award \ "FD_CONTRACT_AWARD" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT_AWARD" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "ORGANISATION").text,
      "orgContact" -> (award \ "FD_CONTRACT_AWARD" \ "CONTRACTING_AUTHORITY_INFORMATION" \ "NAME_ADDRESSES_CONTACT_CONTRACT_AWARD" \ "CA_CE_CONCESSIONAIRE_PROFILE" \ "E_MAIL").text,
      "title" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "TITLE_CONTRACT").text,
      "description" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "SHORT_CONTRACT_DESCRIPTION").text,
      "noticeType" -> (award \ "SYSTEM" \ "NOTICE_TYPE_FRIENDLY_NAME").text,
      "region" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "LOCATION_NUTS" \ "SITE_OR_LOCATION" \ "LABEL" \ "p").head.text,
      "noticeState" -> (award \ "SYSTEM" \ "NOTICE_STATE").text,
      "noticeStateChangeDate" -> (award \ "SYSTEM" \ "SYSTEM_NOTICE_STATE_CHANGE_DATE").text,
      "classification" -> (award \ "FD_CONTRACT_AWARD" \ "OBJECT_CONTRACT_INFORMATION_CONTRACT_AWARD_NOTICE" \ "DESCRIPTION_AWARD_NOTICE_INFORMATION" \ "CPV" \\ "CPV_CODE").map(_ \ "@CODE" text).mkString("|"),
      "numDocs" -> (award \ "FD_CONTRACT_AWARD" \ "DOCUMENTS" \ "_").length.toString
    )
  }

  val awardsCsv = CSVWriter.open(new File("awards.csv"))
  awardsCsv.writeRow(awardsData.head.keySet.toSeq)
  awardsData foreach { award =>
    awardsCsv.writeRow(award.values.toSeq)
  }
  awardsCsv.close()

}
