import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable.ArrayBuffer


     val file = sc.textFile("C:\\Users\\user\\Pictures\\panel\\ghcnd-stations.txt")
    val file2 = sc.textFile("C:\\Users\\user\\Pictures\\panel\\USC00011084.dly.txt")
    val elementValues = Seq("TMAX","TMIN","TOBS","PRCP","SNOW","SNWD","ACMC","ACMH","ACSC","ACSH","AWDR","AWND","DAEV","DAPR","","DATN","DATX","DAWM","DWPR","EVAP","FMTM","FRGB","FRGT","FRTH","GAHT","MDEV","MDPR","MDSF","MDTN","MDTX","MDWM","MNPN","MXPN","PGTM","PSUN","SN01","SN02","SN03","SN04","SN05","SN06","SN07","SN11","SN12","SN13","SN14","SN15","SN16","SN17","SN21","SN22","SN23","SN24","SN25","SN26","SN27","SN31","SN32","SN33","SN34","SN35","SN36","SN37","SN41","SN42","SN43","SN44","SN45","SN46","SN47","SN51","SN52","SN53","SN54","SN55","SN56","SN57","SN61","SN62","SN63","SN64","SN65","SN66","SN67","SN71","SN72","SN73","SN74","SN75","SN76","SN77","SN81","SN82","SN83","SN84","SN85","SN86","SN87","SX01","SX02","SX03","SX04","SX05","SX06","SX07","SX11","SX12","SX13","SX14","SX15","SX16","SX17","SX21","SX22","SX23","SX24","SX25","SX26","SX27","SX31","SX32","SX33","SX34","SX35","SX36","SX37","SX41","SX42","SX43","SX44","SX45","SX46","SX47","SX51","SX52","SX53","SX54","SX55","SX56","SX57","SX61","SX62","SX63","SX64","SX65","SX66","SX67","SX71","SX72","SX73","SX74","SX75","SX76","SX77","SX81","SX82","SX83","SX84","SX85","SX86","SX87","TSUN","WDF1","WDF2","WDF5","WDFG","WDFI","WDFM","WDMV","WESD","WESF","WSF1","WSF2","WSF5","WSFG","WSFI","WSFM","WT00","WT01","WT02","WT03","WT04","WT05","WT06","WT07","WT08","WT09","WT10","WT11","WT12","WT13","WT14","WT15","WT16","WT17","WT18","WT19","WT20","WT21","WT22","WV01","WV03","WV07","WV18","WV20")
    val blankSeq = 0 to 30 map(x => "\\N")
    var idIndex = 0
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
//    joined.foreach(println)

    val pairrdd =joined.map(x => ((x._1,x._2._2._1,x._2._2._2),(x._2._1._1,x._2._1._2,x._2._2._3,x._2._2._4)))
    val grouped = pairrdd.groupByKey()

    val flatted_value = grouped.flatMap{
      x =>{
        val valuesMap = scala.collection.mutable.Map[String,IndexedSeq[String]]()
        x._2.foreach(y => valuesMap += (y._3 -> y._4))
        1 to 31 map {y => (x._1._1.concat(",").concat(x._1._2.concat("-").concat(x._1._3).concat("-").concat(String.valueOf(y))).concat(",").concat(
          elementValues.map(z => valuesMap.getOrElse(z,blankSeq)(y-1)).mkString(",")))
          }
      }
    }
    //flatted_value.foreach(println)
    flatted_value.saveAsTextFile("output")