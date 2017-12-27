import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType,ArrayType}
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer

  def getTemperatures(temp:String):Array[String]={
    if(!temp.equals("9999") && !temp.equals("\\N")) {
      val celTemp = temp.toDouble / 10
      import java.text.DecimalFormat
      val dec = new DecimalFormat("#0.00")
      val fhTemp = dec.format(celTemp * 9 / 5 + 32)
      return Array(String.valueOf(celTemp).concat("c"),(String.valueOf(fhTemp)).concat("f"))
    }else{
      return Array("\\N","\\N")
    }
  }
     val file = sc.textFile("/user/poojagoyanka101/data/ghcnd-stations.txt")
    val file2 = sc.textFile("/user/poojagoyanka101/data/USC00011084.dly.txt")
    val elementValues = Seq("TOBS","PRCP","SNOW","SNWD","ACMC","ACMH","ACSC","ACSH","AWDR","AWND","DAEV","DAPR","DATN","DATX","DAWM","DWPR","EVAP","FMTM","FRGB","FRGT","FRTH","GAHT","MDEV","MDPR","MDSF","MDTN","MDTX","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN04","SN05","SN06","SN07","SN11","SN12","SN13","SN14","SN15","SN16","SN17","SN21","SN22","SN23","SN24","SN25","SN26","SN27","SN31","SN32","SN33","SN34","SN35","SN36","SN37","SN41","SN42","SN43","SN44","SN45","SN46","SN47","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN62","SN63","SN64","SN65","SN66","SN67","SN71","SN72","SN73","SN74","SN75","SN76","SN77","SN81","SN82","SN83","SN84","SN85","SN86","SN87","SX01","SX02","SX03","SX04","SX05","SX06","SX07","SX11","SX12","SX13","SX14","SX15","SX16","SX17","SX21","SX22","SX23","SX24","SX25","SX26","SX27","SX31","SX32","SX33","SX34","SX35","SX36","SX37","SX41","SX42","SX43","SX44","SX45","SX46","SX47","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX62","SX63","SX64","SX65","SX66","SX67","SX71","SX72","SX73","SX74","SX75","SX76","SX77","SX81","SX82","SX83","SX84","SX85","SX86","SX87","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT00","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT20","WT21","WT22","WV01","WV03","WV07","WV18","WV20")
    val blankSeq = 0 to 30 map(x => "\\N")
    var idIndex = 0
  val schema = new StructType().add(StructField("id", StringType)).add(StructField("Date", StringType)).add(StructField("Latitude", StringType)).add(StructField("Longitute", StringType)).add(StructField("Tmax_c", StringType)).add(StructField("Tmax_f", StringType)).add(StructField("Tmin_c", StringType)).add(StructField("Tmin_f", StringType)).add(StructField("TOBS", StringType)).add(StructField("PRCP", StringType)).add(StructField("SNOW", StringType)).add(StructField("SNWD", StringType)).add(StructField("ACMC", StringType)).add(StructField("ACMH", StringType)).add(StructField("ACSC", StringType)).add(StructField("ACSH", StringType)).add(StructField("AWDR", StringType)).add(StructField("AWND", StringType)).add(StructField("DAEV", StringType)).add(StructField("DAPR", StringType)).add(StructField("DATN", StringType)).add(StructField("DATX", StringType)).add(StructField("DAWM", StringType)).add(StructField("DWPR", StringType)).add(StructField("EVAP", StringType)).add(StructField("FMTM", StringType)).add(StructField("FRGB", StringType)).add(StructField("FRGT", StringType)).add(StructField("FRTH", StringType)).add(StructField("GAHT", StringType)).add(StructField("MDEV", StringType)).add(StructField("MDPR", StringType)).add(StructField("MDSF", StringType)).add(StructField("MDTN", StringType)).add(StructField("MDTX", StringType)).add(StructField("MDWM", StringType)).add(StructField("MNPN", StringType)).add(StructField("MXPN", StringType)).add(StructField("PGTM", StringType)).add(StructField("PSUN", StringType)).add(StructField("SN01", StringType)).add(StructField("SN02", StringType)).add(StructField("SN03", StringType)).add(StructField("SN04", StringType)).add(StructField("SN05", StringType)).add(StructField("SN06", StringType)).add(StructField("SN07", StringType)).add(StructField("SN11", StringType)).add(StructField("SN12", StringType)).add(StructField("SN13", StringType)).add(StructField("SN14", StringType)).add(StructField("SN15", StringType)).add(StructField("SN16", StringType)).add(StructField("SN17", StringType)).add(StructField("SN21", StringType)).add(StructField("SN22", StringType)).add(StructField("SN23", StringType)).add(StructField("SN24", StringType)).add(StructField("SN25", StringType)).add(StructField("SN26", StringType)).add(StructField("SN27", StringType)).add(StructField("SN31", StringType)).add(StructField("SN32", StringType)).add(StructField("SN33", StringType)).add(StructField("SN34", StringType)).add(StructField("SN35", StringType)).add(StructField("SN36", StringType)).add(StructField("SN37", StringType)).add(StructField("SN41", StringType)).add(StructField("SN42", StringType)).add(StructField("SN43", StringType)).add(StructField("SN44", StringType)).add(StructField("SN45", StringType)).add(StructField("SN46", StringType)).add(StructField("SN47", StringType)).add(StructField("SN51", StringType)).add(StructField("SN52", StringType)).add(StructField("SN53", StringType)).add(StructField("SN54", StringType)).add(StructField("SN55", StringType)).add(StructField("SN56", StringType)).add(StructField("SN57", StringType)).add(StructField("SN61", StringType)).add(StructField("SN62", StringType)).add(StructField("SN63", StringType)).add(StructField("SN64", StringType)).add(StructField("SN65", StringType)).add(StructField("SN66", StringType)).add(StructField("SN67", StringType)).add(StructField("SN71", StringType)).add(StructField("SN72", StringType)).add(StructField("SN73", StringType)).add(StructField("SN74", StringType)).add(StructField("SN75", StringType)).add(StructField("SN76", StringType)).add(StructField("SN77", StringType)).add(StructField("SN81", StringType)).add(StructField("SN82", StringType)).add(StructField("SN83", StringType)).add(StructField("SN84", StringType)).add(StructField("SN85", StringType)).add(StructField("SN86", StringType)).add(StructField("SN87", StringType)).add(StructField("SX01", StringType)).add(StructField("SX02", StringType)).add(StructField("SX03", StringType)).add(StructField("SX04", StringType)).add(StructField("SX05", StringType)).add(StructField("SX06", StringType)).add(StructField("SX07", StringType)).add(StructField("SX11", StringType)).add(StructField("SX12", StringType)).add(StructField("SX13", StringType)).add(StructField("SX14", StringType)).add(StructField("SX15", StringType)).add(StructField("SX16", StringType)).add(StructField("SX17", StringType)).add(StructField("SX21", StringType)).add(StructField("SX22", StringType)).add(StructField("SX23", StringType)).add(StructField("SX24", StringType)).add(StructField("SX25", StringType)).add(StructField("SX26", StringType)).add(StructField("SX27", StringType)).add(StructField("SX31", StringType)).add(StructField("SX32", StringType)).add(StructField("SX33", StringType)).add(StructField("SX34", StringType)).add(StructField("SX35", StringType)).add(StructField("SX36", StringType)).add(StructField("SX37", StringType)).add(StructField("SX41", StringType)).add(StructField("SX42", StringType)).add(StructField("SX43", StringType)).add(StructField("SX44", StringType)).add(StructField("SX45", StringType)).add(StructField("SX46", StringType)).add(StructField("SX47", StringType)).add(StructField("SX51", StringType)).add(StructField("SX52", StringType)).add(StructField("SX53", StringType)).add(StructField("SX54", StringType)).add(StructField("SX55", StringType)).add(StructField("SX56", StringType)).add(StructField("SX57", StringType)).add(StructField("SX61", StringType)).add(StructField("SX62", StringType)).add(StructField("SX63", StringType)).add(StructField("SX64", StringType)).add(StructField("SX65", StringType)).add(StructField("SX66", StringType)).add(StructField("SX67", StringType)).add(StructField("SX71", StringType)).add(StructField("SX72", StringType)).add(StructField("SX73", StringType)).add(StructField("SX74", StringType)).add(StructField("SX75", StringType)).add(StructField("SX76", StringType)).add(StructField("SX77", StringType)).add(StructField("SX81", StringType)).add(StructField("SX82", StringType)).add(StructField("SX83", StringType)).add(StructField("SX84", StringType)).add(StructField("SX85", StringType)).add(StructField("SX86", StringType)).add(StructField("SX87", StringType)).add(StructField("TSUN", StringType)).add(StructField("WDF1", StringType)).add(StructField("WDF2", StringType)).add(StructField("WDF5", StringType)).add(StructField("WDFG", StringType)).add(StructField("WDFI", StringType)).add(StructField("WDFM", StringType)).add(StructField("WDMV", StringType)).add(StructField("WESD", StringType)).add(StructField("WESF", StringType)).add(StructField("WSF1", StringType)).add(StructField("WSF2", StringType)).add(StructField("WSF5", StringType)).add(StructField("WSFG", StringType)).add(StructField("WSFI", StringType)).add(StructField("WSFM", StringType)).add(StructField("WT00", StringType)).add(StructField("WT01", StringType)).add(StructField("WT02", StringType)).add(StructField("WT03", StringType)).add(StructField("WT04", StringType)).add(StructField("WT05", StringType)).add(StructField("WT06", StringType)).add(StructField("WT07", StringType)).add(StructField("WT08", StringType)).add(StructField("WT09", StringType)).add(StructField("WT10", StringType)).add(StructField("WT11", StringType)).add(StructField("WT12", StringType)).add(StructField("WT13", StringType)).add(StructField("WT14", StringType)).add(StructField("WT15", StringType)).add(StructField("WT16", StringType)).add(StructField("WT17", StringType)).add(StructField("WT18", StringType)).add(StructField("WT19", StringType)).add(StructField("WT20", StringType)).add(StructField("WT21", StringType)).add(StructField("WT22", StringType)).add(StructField("WV01", StringType)).add(StructField("WV03", StringType)).add(StructField("WV07", StringType)).add(StructField("WV18", StringType)).add(StructField("WV20", StringType))
    val rdd1 = file.map(line1 => line1.split(" ")).map{array =>{

      while(array(idIndex).equals("")){
        idIndex = idIndex + 1

      }
      var latitudeIndex = idIndex + 1
      while(array(latitudeIndex).equals("")){
        latitudeIndex = latitudeIndex + 1
      }
      var longitutudeIndex = latitudeIndex + 1

      while(array(longitutudeIndex).equals("")){
        longitutudeIndex = longitutudeIndex + 1
      }
      (array(idIndex),(array(latitudeIndex),array(longitutudeIndex)))
    }}



    val rdd2 = file2.map{x => {
      val id = x.substring(0,11)
      val year =x.substring(11,15)
      val month =x.substring(15,17)
      val element = x.substring(17,21)
    val values = 0 to 30 map { i => x.substring(8 * i + 22, 8 * i + 26) }
      (id,(year,month,element,values))
    }

    }

   val joined = rdd1.join(rdd2)
    val pairrdd =joined.map(x => ((x._1,x._2._2._1,x._2._2._2,x._2._1._1,x._2._1._2),(x._2._2._3,x._2._2._4)))
    val grouped = pairrdd.groupByKey()

    val flatted_value = grouped.flatMap{
      x =>{
        val valuesMap = scala.collection.mutable.Map[String,IndexedSeq[String]]()
        x._2.foreach(y => valuesMap += (y._1 -> y._2))

        1 to 31 map {y => Row((Array(x._1._1,x._1._2.concat("-").concat(x._1._3).concat("-")
          .concat(String.valueOf(y)),
          x._1._4,
          x._1._5) ++
          getTemperatures(valuesMap.getOrElse("TMAX",blankSeq)(y-1)) ++
          getTemperatures(valuesMap.getOrElse("TMIN",blankSeq)(y-1)) ++
          elementValues.map(z => valuesMap.getOrElse(z,blankSeq)(y-1))):_*)
          }
      }
    }
	import sqlContext.implicits._
    sqlContext.createDataFrame(flatted_value,schema)

