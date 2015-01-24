package som.fun.crawl

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.net.URL
import java.net.HttpURLConnection
import scala.collection.JavaConversions._
import java.io.ByteArrayOutputStream
import java.util.concurrent.CountDownLatch
import java.util.HashSet
import java.io.PrintStream
import java.io.FileOutputStream
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer

object CrawlerTest extends App {

//  new Crawler("http://www.qq.com", filter = (url: String) => url.contains(".qq.com")).crawl
  val seq = Seq("a","b")
  println(seq.mkString(","))
   println(Seq.empty[String].mkString(",").length())
}

/**
 * @param startPage crawler would crawl from that page
 * @param filter crawler just crawl those url which match the filter
 * @param onComplete handler for download complete
 */
class Crawler(startPage: String,
  filter: (String => Boolean) = (url: String) => true,
  onDataLoaded: (String, Int, Array[Byte], Map[String, String]) => Any = (url: String, status: Int, data: Array[Byte], headers: Map[String, String]) => { println(s"download $url done") }) {
  private val latch = new CountDownLatch(1)
  private val linkRegex = """ (src|href)="([^"]+)"|(src|href)='([^']+)' """.trim.r
  private val htmlTypeRegex = "\btext/html\b"
  private val crawledPool = new HashSet[String]

  def crawl {
    crawlPageLinks(startPage, new String(get(startPage)._2))
    latch.await()
  }

  private def crawlPageLinks(pageUrl: String, pageContent: String) {
    val links = parseCrawlLinks(pageUrl, pageContent)
    links.map {
      link =>
        val future = Future(get(link))
        future.onSuccess {
          case data if isTextPage(data._3) =>
            crawlPageLinks(link, new String(data._2))
        }
        future.onFailure {
          case e: Exception =>
            println(s"visit $link error!")
            e.printStackTrace
        }
    }
  }

  private def getFullUrl(parentUrl: String, link: String) = {
    val baseHost = getHostBase(parentUrl)
    link match {
      case link if link.startsWith("/") => baseHost + link
      case link if link.startsWith("http:") || link.startsWith("https:") => link
      case _ =>
        val index = parentUrl.lastIndexOf("/")
        parentUrl.substring(0, index) + "/" + link
    }
  }

  private def parseCrawlLinks(parentUrl: String, html: String) = {
    val baseHost = getHostBase(parentUrl)
    val links = fetchLinks(html).map {
      link =>
        link match {
          case link if link.startsWith("/") => baseHost + link
          case link if link.startsWith("http:") || link.startsWith("https:") => link
          case _ =>
            val index = parentUrl.lastIndexOf("/")
            parentUrl.substring(0, index) + "/" + link
        }
    }.filter {
      link => !crawledPool.contains(link) && this.filter(link)
    }
    println("find " + links.size + " links at page " + parentUrl)
    links
  }
  def get(url: String) = {
    val uri = new URL(url);
    val conn = uri.openConnection().asInstanceOf[HttpURLConnection];
    conn.setConnectTimeout(100000)
    conn.setReadTimeout(1000000)
    val stream = conn.getInputStream()
    val buf = Array.fill[Byte](1024)(0)
    var len = stream.read(buf)
    val out = new ByteArrayOutputStream
    while (len > -1) {
      out.write(buf, 0, len)
      len = stream.read(buf)
    }

    val data = out.toByteArray()
    val status = conn.getResponseCode()

    val headers = conn.getHeaderFields().toMap.map {
      head => (head._1, head._2.mkString(","))
    }
    conn.disconnect
    crawledPool.add(url)
    this.onDataLoaded(url, status, data, headers)
    (conn.getResponseCode(), data, headers)

  }
  private def fetchLinks(html: String) = {
    val list = for (m <- linkRegex.findAllIn(html).matchData if (m.group(1) != null || m.group(3) != null)) yield {
      if (m.group(1) != null) m.group(2) else m.group(4)
    }
    list.filter {
      link => !link.startsWith("#") && !link.startsWith("javascript:") && link != "" && !link.startsWith("mailto:")
    }.toSet

  }

  private def getHostBase(url: String) = {
    val uri = new URL(url)
    val portPart = if (uri.getPort() == -1 || uri.getPort() == 80) "" else ":" + uri.getPort()
    uri.getProtocol() + "://" + uri.getHost() + portPart
  }

  private def isTextPage(headers: Map[String, String]) = {
    val contentType = if (headers contains "Content-Type") headers("Content-Type") else null
    contentType match {
      case null => false
      case contentType if contentType isEmpty => false
      case contentType if Pattern.compile(htmlTypeRegex).matcher(contentType).find => true
      case _ => false
    }

  }

} 

