import scala.collection.immutable.ListBuffer;
import scala.collection.immutable.Set;
import scala.util.Random;
import scala.util.Time;
import scala.math.ordering.comparatorToOrdering
import scala.io.Source

//crating scala object to declare variables
class CSVReader ()
{
    class Trade (var id : Int, var name : String, var timestamp : String, var otype : String, var quantity : Int, var rate : Int)
    {
        def getRate() : Int=
        {
            return rate
        }
        def getId() : Int=
        {
            return id
        }
        def this(tradeDetails : Array[String])
        {
            this(Integer.parseInt(tradeDetails(0)), tradeDetails(1), tradeDetails(2), tradeDetails(3), Integer.parseInt(tradeDetails(4)), Integer.parseInt(tradeDetails(5)))
        }
    }
}



//main method to read the files and declaring orderbook and orderclosed fields

  def main(args: Array[String]): Unit = {
    var line: String = ""
    val splitBy: String = ","
    val orderBook: List[CSVReader.Trade] = new ArrayList[CSVReader.Trade]
    val ordersClosed: List[String] = new ArrayList[String]
    try {
      val br: BufferedReader = new BufferedReader(new FileReader("./EXAMPLE_ORDERS.CSV"))
      while ( {
        (line = br.readLine) != null
      }) {
        val tradeDetails: Array[String] = line.split(splitBy)
        val trade: CSVReader.Trade = new CSVReader.Trade(tradeDetails)
        matchingEngine(orderBook, trade, ordersClosed)
      }
      val csvOutputFile: File = new File("./OUTPUT.csv")
      try {
        val pw: PrintWriter = new PrintWriter(csvOutputFile)
        try ordersClosed.stream.forEach(pw.println)
        finally {
          if (pw != null) pw.close()
        }
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

//matching engine code 

  def matchingEngine(orderBook: List[CSVReader.Trade], trade: CSVReader.Trade, ordersClosed: List[String]): Unit = {
    val tradeStream: Map[Integer, List[CSVReader.Trade]] = orderBook.stream.filter((bookTrade: CSVReader.Trade) => !(bookTrade.otype == trade.otype) && bookTrade.quantity == trade.quantity).collect(Collectors.groupingBy(Trade.getRate))
    var matchedTrade: CSVReader.Trade = null
    if (!(tradeStream.keySet.isEmpty)) {
      if (trade.otype eq "BUY") {
        matchedTrade = tradeStream.get(tradeStream.keySet.stream.min(Integer.compareTo).get).stream.min(Comparator.comparing(Trade.getId)).orElse(null)
      }
      else {
        matchedTrade = tradeStream.get(tradeStream.keySet.stream.max(Integer.compareTo).get).stream.min(Comparator.comparing(Trade.getId)).orElse(null)
      }
    }
    if (matchedTrade != null) {
      orderBook.remove(matchedTrade)
      ordersClosed.add(trade.id + "," + matchedTrade.id + "," + trade.timestamp + "," + trade.quantity + "," + matchedTrade.rate)
    }
    else {
      orderBook.add(trade)
    }
  }
}