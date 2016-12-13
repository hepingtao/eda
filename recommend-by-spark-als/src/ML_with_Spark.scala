#4.2 提取有效特征
val rawData= sc.textFile("file:///file/iTouchTV/MovieLens/ml-1m/ratings.dat")
rawData.first()
val rawRatings =rawData.map(_.split("::").take(3))

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

val ratings = rawRatings.map{case Array(user,movie,rating) =>Rating(user.toInt, movie.toInt, rating.toDouble)}

#4.3 训练推荐模型
val model=ALS.train(ratings,50,10,0.01)
model.userFeatures
model.userFeatures.count

# 4.4 使用推荐模型

# 4.4.1 用户推荐
# 1.生成推荐
val predictedRating = model.predict(789,123)

val userId=789
val K = 10
val topKRecs = model.recommendProducts(userId,K)
println(topKRecs.mkString("\n"))

#2.检验推荐内容
val movies = sc.textFile("file:///file/iTouchTV/MovieLens/ml-1m/movies.dat")
val titles = movies.map(line => line.split("::").take(2)).map(array => (array(0).toInt, array(1).toString)).collectAsMap()
titles(123)

val moviesForUser = ratings.keyBy(_.user).lookup(789)

println(moviesForUser.size)

moviesForUser.sortBy(-_.rating).take(10).map(rating=>(titles(rating.product),rating.rating)).foreach(println)

topKRecs.map(rating=> (titles(rating.product),rating.rating)).foreach(println)

##4.4.2 物品推荐

import org.jblas.DoubleMatrix
val aMatrix = new DoubleMatrix(Array(1.0,2.0,3.0))
def cosineSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix):Double={vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())}

val itemId =567
val itemFactor = model.productFeatures.lookup(itemId).head
val itemVector = new DoubleMatrix(itemFactor)
cosineSimilarity(itemVector,itemVector)

val sims = model.productFeatures.map{ case(id ,factor)=>
  val factorVector = new DoubleMatrix (factor)
  val sim = cosineSimilarity(factorVector, itemVector)
  (id,sim)
}

val sortedSims = sims.top(K)(Ordering.by[(Int,Double),Double]{case(id,similarity)=> similarity})

println(sortedSims.take(10).mkString("\n"))

# 2. 检查推荐的相似物品

println(titles(itemId))

val sortedSims2 = sims.top(K+1)(Ordering.by[(Int,Double),Double]{case (id,similarity) => similarity})

sortedSim2.slice(1,11).map{case (id,sim)=>(titles(id),sim)}.mkString("\n")

#4.5 推荐模型效果评估
#4.5.1 均方差
val actualRating = moviesForUser.take(1)(0)

val predictedRating = model.predict(789,actualRating.product)

val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)

val usersProducts = ratings.map { case Rating(user, product, rating) =>
  (user, product)
}
val predictions =
  model.predict(usersProducts).map { case Rating(user, product, rating) =>
    ((user, product), rating)
  }
val ratingsAndPredictions = ratings.map { case Rating(user, product, rating) =>
  ((user, product), rating)
}.join(predictions)

val MSE = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) =>math.pow((actual-predicted),2)}.reduce(_ + _) / ratingsAndPredictions.count
println("Mean Squared Error = " + MSE)

val RMSE = math.sqrt(MSE)
println("Root Mean Squared Error = " + RMSE)

# 4.5.2 K值平均准确率














