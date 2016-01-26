import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 15-10-12.
 */
object Kmeans {
  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("mllib-geo").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //装载数据
    val data = sc.textFile("/root/kmeans1.csv")
    val parsedData = data.map(x => Vectors.dense(x.split(' ').map(_.toDouble)))
    //将数据集聚类，初定K值为2，经过30次迭代形成数据模型
    val KClusters = 2
    val numIterations = 30
    // 计算数据中心点
    val model = KMeans.train(parsedData, KClusters, numIterations)
    //数据模型的中心点
    println("Cluster centers: ")

    for (c <- model.clusterCenters){
      println("clusterCenters: " + c.toString)
    }
    //用误差平方之和评估数据模型
    val cost = model.computeCost(parsedData)
    println("Within Set Sum of Squared Erroes = " + cost)
    //用模型测试单点数据
    println("Vectors 0.2 0.2 0.2 is belongs to clusters: " + model.predict(Vectors.dense("0.2 0.2".split(' ').map(_.toDouble))))
    println("Vectors 0.3 0.3 0.3 is belongs to clusters: " + model.predict(Vectors.dense("0.3 0.3".split(' ').map(_.toDouble))))
    println("Vectors 2 2 2 is belongs to clusters: " + model.predict(Vectors.dense("2 2".split(' ').map(_.toDouble))))
    println("Vectors 3 3 3 is belongs to clusters: " + model.predict(Vectors.dense("3 3".split(' ').map(_.toDouble))))
    //交叉评估a，只返回结果
    val testdata = data.map(x => Vectors.dense(x.split(' ').map(_.toDouble)))
    val resulta = model.predict(testdata)
    resulta.saveAsTextFile("hdfs://10.4.1.4:9000/out/testkmeansRa")
    //交叉评估b，返回数据集和结果
    val resultb = data.map{ x =>
      val linevectore = Vectors.dense(x.split(' ').map(_.toDouble))
      val prediction = model.predict(linevectore)
      x + " " + prediction
    }.saveAsTextFile("hdfs://10.4.1.4:9000/out/testkmeansRb")
    sc.stop()
  }
}
