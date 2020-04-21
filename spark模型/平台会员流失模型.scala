//////平台会员流失预警随机森林RF//////
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.{IndexToString,VectorIndexer,VectorAssembler,StringIndexer,OneHotEncoder}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier} 
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator 
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._


object CustChurnYigou{
	def main(args:Array[String]){
		val spark=SparkSession.builder()
					.appName("CustChurnYigou")
					.enableHiveSupport()
					.getOrCreate()
		import spark.implicits._
		//val dataDF1=spark.read.option("header","true").csv("E:/Users/wangxl/spark/CustChurn/yigouData.csv")
		val dataDF1=spark.sql("select * from bimining.member_churn_yigou_train_w where gender is not null")
		val dataDF2=dataDF1.drop("child_age")
		val dataDF3=dataDF2.drop("reliability_score_age")
		val dataDF4=dataDF3.drop("reliability_grade_age")
		val dataDF5=dataDF4.drop("accur_grade_age")
		val dataDF=dataDF5
		
		//转换数据类型
		val df=dataDF.select(
		dataDF("member_id").cast("String"),
		dataDF("whether_churn").cast("Double"),
		dataDF("visit_amnt_1y").cast("Double"),
		dataDF("visit_days_1y").cast("Double"),
		dataDF("avg_vist_1y").cast("Double"),
		dataDF("avg_drtn_1y").cast("Double"),
		dataDF("visit_grps_1y").cast("Double"),
		dataDF("visit_amnt_6m").cast("Double"),
		dataDF("visit_days_6m").cast("Double"),
		dataDF("avg_vist_6m").cast("Double"),
		dataDF("avg_drtn_6m").cast("Double"),
		dataDF("visit_grps_6m").cast("Double"),
		dataDF("visit_amnt_3m").cast("Double"),
		dataDF("visit_days_3m").cast("Double"),
		dataDF("avg_vist_3m").cast("Double"),
		dataDF("avg_drtn_3m").cast("Double"),
		dataDF("visit_grps_3m").cast("Double"),
		dataDF("visit_amnt_1m").cast("Double"),
		dataDF("visit_days_1m").cast("Double"),
		dataDF("avg_vist_1m").cast("Double"),
		dataDF("avg_drtn_1m").cast("Double"),
		dataDF("visit_grps_1m").cast("Double"),
		dataDF("visit_amnt_3w").cast("Double"),
		dataDF("visit_days_3w").cast("Double"),
		dataDF("avg_vist_3w").cast("Double"),
		dataDF("avg_drtn_3w").cast("Double"),
		dataDF("visit_grps_3w").cast("Double"),
		dataDF("visit_amnt_2w").cast("Double"),
		dataDF("visit_days_2w").cast("Double"),
		dataDF("avg_vist_2w").cast("Double"),
		dataDF("avg_drtn_2w").cast("Double"),
		dataDF("visit_grps_2w").cast("Double"),
		dataDF("visit_amnt_1w").cast("Double"),
		dataDF("visit_days_1w").cast("Double"),
		dataDF("avg_vist_1w").cast("Double"),
		dataDF("avg_drtn_1w").cast("Double"),
		dataDF("visit_grps_1w").cast("Double"),
		dataDF("recent_clct").cast("Double"),
		dataDF("tot_clct_num_1y").cast("Double"),
		dataDF("tot_clct_num_1w").cast("Double"),
		dataDF("tot_clct_num_2w").cast("Double"),
		dataDF("tot_clct_num_3w").cast("Double"),
		dataDF("tot_clct_num_1m").cast("Double"),
		dataDF("tot_clct_num_2m").cast("Double"),
		dataDF("tot_clct_num_3m").cast("Double"),
		dataDF("tot_clct_num_6m").cast("Double"),
		dataDF("recent_cart").cast("Double"),
		dataDF("tot_cart_num_1y").cast("Double"),
		dataDF("tot_cart_num_1w").cast("Double"),
		dataDF("tot_cart_num_2w").cast("Double"),
		dataDF("tot_cart_num_3w").cast("Double"),
		dataDF("tot_cart_num_1m").cast("Double"),
		dataDF("tot_cart_num_2m").cast("Double"),
		dataDF("tot_cart_num_3m").cast("Double"),
		dataDF("tot_cart_num_6m").cast("Double"),
		dataDF("recent_buy").cast("Double"),
		dataDF("cost_amnt_1y").cast("Double"),
		dataDF("order_amnt_1y").cast("Double"),
		dataDF("buy_grps_1y").cast("Double"),
		dataDF("cost_amnt_6m").cast("Double"),
		dataDF("order_amnt_6m").cast("Double"),
		dataDF("buy_grps_6m").cast("Double"),
		dataDF("cost_amnt_3m").cast("Double"),
		dataDF("order_amnt_3m").cast("Double"),
		dataDF("buy_grps_3m").cast("Double"),
		dataDF("cost_amnt_1m").cast("Double"),
		dataDF("order_amnt_1m").cast("Double"),
		dataDF("buy_grps_1m").cast("Double"),
		dataDF("cost_amnt_3w").cast("Double"),
		dataDF("order_amnt_3w").cast("Double"),
		dataDF("buy_grps_3w").cast("Double"),
		dataDF("cost_amnt_2w").cast("Double"),
		dataDF("order_amnt_2w").cast("Double"),
		dataDF("buy_grps_2w").cast("Double"),
		dataDF("cost_amnt_1w").cast("Double"),
		dataDF("order_amnt_1w").cast("Double"),
		dataDF("buy_grps_1w").cast("Double"),
		dataDF("return_amnt_1y").cast("Double"),
		dataDF("return_amnt_6m").cast("Double"),
		dataDF("return_amnt_3m").cast("Double"),
		dataDF("return_amnt_1m").cast("Double"),
		dataDF("return_amnt_3w").cast("Double"),
		dataDF("return_amnt_2w").cast("Double"),
		dataDF("return_amnt_1w").cast("Double"),
		dataDF("gender").cast("String"),
		dataDF("age_value").cast("Double"),
		dataDF("cust_level_num").cast("String"),
		dataDF("purchase_power").cast("String"),
		dataDF("loyalty_level").cast("String"),
		dataDF("scor_label").cast("String"),
		dataDF("complaints_1y").cast("Double"),
		dataDF("complaints_6m").cast("Double"),
		dataDF("complaints_3m").cast("Double"),
		dataDF("complaints_1m").cast("Double"),
		dataDF("complaints_3w").cast("Double"),
		dataDF("complaints_2w").cast("Double"),
		dataDF("complaints_1w").cast("Double"),
		dataDF("att_amnt_1y").cast("Double"),
		dataDF("att_amnt_6m").cast("Double"),
		dataDF("att_amnt_3m").cast("Double"),
		dataDF("att_amnt_1m").cast("Double"),
		dataDF("att_amnt_3w").cast("Double"),
		dataDF("att_amnt_2w").cast("Double"),
		dataDF("att_amnt_1w").cast("Double"),
		dataDF("lgs_amnt_1y").cast("Double"),
		dataDF("lgs_amnt_6m").cast("Double"),
		dataDF("lgs_amnt_3m").cast("Double"),
		dataDF("lgs_amnt_1m").cast("Double"),
		dataDF("lgs_amnt_3w").cast("Double"),
		dataDF("lgs_amnt_2w").cast("Double"),
		dataDF("lgs_amnt_1w").cast("Double"),
		dataDF("gds_cmnt_1y").cast("Double"),
		dataDF("gds_cmnt_6m").cast("Double"),
		dataDF("gds_cmnt_3m").cast("Double"),
		dataDF("gds_cmnt_1m").cast("Double"),
		dataDF("gds_cmnt_3w").cast("Double"),
		dataDF("gds_cmnt_2w").cast("Double")
		)

		//gender one-hot编码
		val indexer=new StringIndexer().setInputCol("gender").setOutputCol("genderIndex").fit(df)
		val indexed=indexer.transform(df)
		val encoder=new OneHotEncoder().setInputCol("genderIndex").setOutputCol("genderVec").setDropLast(false)
		val encoded=encoder.transform(indexed)
		//cust_level_num one-hot编码
		val indexer1=new StringIndexer().setInputCol("cust_level_num").setOutputCol("custLevelIndex").fit(encoded)
		val indexed1=indexer1.transform(encoded)
		val encoder1=new OneHotEncoder().setInputCol("custLevelIndex").setOutputCol("custLevelVec").setDropLast(false)
		val encoded1=encoder1.transform(indexed1)
		//purchase_power one-hot编码
		val indexer2=new StringIndexer().setInputCol("purchase_power").setOutputCol("purchaseIndex").fit(encoded1)
		val indexed2=indexer2.transform(encoded1)
		val encoder2=new OneHotEncoder().setInputCol("purchaseIndex").setOutputCol("purchaseVec").setDropLast(false)
		val encoded2=encoder2.transform(indexed2)
		//loyalty_level one-hot编码
		val indexer3=new StringIndexer().setInputCol("loyalty_level").setOutputCol("loyaltyIndex").fit(encoded2)
		val indexed3=indexer3.transform(encoded2)
		val encoder3=new OneHotEncoder().setInputCol("loyaltyIndex").setOutputCol("loyaltyVec").setDropLast(false)
		val encoded3=encoder3.transform(indexed3)
		//scor_label one-hot编码
		val indexer4=new StringIndexer().setInputCol("scor_label").setOutputCol("scorLabelIndex").fit(encoded3)
		val indexed4=indexer4.transform(encoded3)
		val encoder4=new OneHotEncoder().setInputCol("scorLabelIndex").setOutputCol("scorLabelVec").setDropLast(false)
		val encoded4=encoder4.transform(indexed4)
		

	
		val colArray=Array("visit_amnt_1y","visit_days_1y","avg_vist_1y","avg_drtn_1y","visit_grps_1y",
		"visit_amnt_6m","visit_days_6m","avg_vist_6m","avg_drtn_6m","visit_grps_6m",
		"visit_amnt_3m","visit_days_3m","avg_vist_3m","avg_drtn_3m","visit_grps_3m",
		"visit_amnt_1m","visit_days_1m","avg_vist_1m","avg_drtn_1m","visit_grps_1m",
		"visit_amnt_3w","visit_days_3w","avg_vist_3w","avg_drtn_3w","visit_grps_3w",
		"visit_amnt_2w","visit_days_2w","avg_vist_2w","avg_drtn_2w","visit_grps_2w",
		"visit_amnt_1w","visit_days_1w","avg_vist_1w","avg_drtn_1w","visit_grps_1w",
		"recent_clct","tot_clct_num_1y","tot_clct_num_1w","tot_clct_num_2w",
		"tot_clct_num_3w","tot_clct_num_1m","tot_clct_num_2m","tot_clct_num_3m",
		"tot_clct_num_6m","recent_cart","tot_cart_num_1y","tot_cart_num_1w","tot_cart_num_2w",
		"tot_cart_num_3w","tot_cart_num_1m","tot_cart_num_2m","tot_cart_num_3m",
		"tot_cart_num_6m","recent_buy","cost_amnt_1y","order_amnt_1y","buy_grps_1y",
		"cost_amnt_6m","order_amnt_6m","buy_grps_6m","cost_amnt_3m","order_amnt_3m",
		"buy_grps_3m","cost_amnt_1m","order_amnt_1m","buy_grps_1m","cost_amnt_3w",
		"order_amnt_3w","buy_grps_3w","cost_amnt_2w","order_amnt_2w","buy_grps_2w",
		"cost_amnt_1w","order_amnt_1w","buy_grps_1w","return_amnt_1y","return_amnt_6m",
		"return_amnt_3m","return_amnt_1m","return_amnt_3w","return_amnt_2w",
		"return_amnt_1w","genderVec","age_value","custLevelVec","purchaseVec",
		"loyaltyVec","scorLabelVec","complaints_1y","complaints_6m","complaints_3m",
		"complaints_1m","complaints_3w","complaints_2w","complaints_1w","att_amnt_1y",
		"att_amnt_6m","att_amnt_3m","att_amnt_1m","att_amnt_3w","att_amnt_2w","att_amnt_1w",
		"lgs_amnt_1y","lgs_amnt_6m","lgs_amnt_3m","lgs_amnt_1m","lgs_amnt_3w","lgs_amnt_2w",
		"lgs_amnt_1w","gds_cmnt_1y","gds_cmnt_6m","gds_cmnt_3m","gds_cmnt_1m","gds_cmnt_3w",
		"gds_cmnt_2w") //"child_age","reliability_score_age",
		val vecDF=new VectorAssembler().setInputCols(colArray).setOutputCol("features").transform(encoded4)
		val data=vecDF

		//将whether_churn设置为标签
		val labelIndex=new StringIndexer().setInputCol("whether_churn").setOutputCol("label").fit(data)
  
		//将数据三七分为测试和训练数据
		val Array(trainingDF,testDF)=data.randomSplit(Array(0.7,0.3),seed=12345)
		
		
		////////训练模型///////
		val classifier=new RandomForestClassifier().setImpurity("gini").setMaxDepth(10).setNumTrees(40).
						setFeatureSubsetStrategy("auto").setSeed(5043)

		//将indexed的标签转换为原始标签
		val labelConverter=new IndexToString()
		.setInputCol("prediction")
		.setOutputCol("predictedLabel")
		.setLabels(labelIndex.labels)
		
		//pipeline工作流
		val pipeline=new Pipeline().setStages(Array(labelIndex,classifier,labelConverter))
		val model=pipeline.fit(trainingDF)
		
		//读入预测数据
		val preDF1=spark.sql("select * from bimining.member_churn_yigou_pre_w where gender is not null")
		val preDF2=preDF1.drop("child_age")
		val preDF21=preDF2.drop("reliability_score_age")
		val preDF22=preDF21.drop("reliability_grade_age")
		val preDF3=preDF22.drop("accur_grade_age")

		
		//转换数据类型
		val preDF4=preDF3.select(
		preDF3("member_id").cast("String"),
		preDF3("visit_amnt_1y").cast("Double"),
		preDF3("visit_days_1y").cast("Double"),
		preDF3("avg_vist_1y").cast("Double"),
		preDF3("avg_drtn_1y").cast("Double"),
		preDF3("visit_grps_1y").cast("Double"),
		preDF3("visit_amnt_6m").cast("Double"),
		preDF3("visit_days_6m").cast("Double"),
		preDF3("avg_vist_6m").cast("Double"),
		preDF3("avg_drtn_6m").cast("Double"),
		preDF3("visit_grps_6m").cast("Double"),
		preDF3("visit_amnt_3m").cast("Double"),
		preDF3("visit_days_3m").cast("Double"),
		preDF3("avg_vist_3m").cast("Double"),
		preDF3("avg_drtn_3m").cast("Double"),
		preDF3("visit_grps_3m").cast("Double"),
		preDF3("visit_amnt_1m").cast("Double"),
		preDF3("visit_days_1m").cast("Double"),
		preDF3("avg_vist_1m").cast("Double"),
		preDF3("avg_drtn_1m").cast("Double"),
		preDF3("visit_grps_1m").cast("Double"),
		preDF3("visit_amnt_3w").cast("Double"),
		preDF3("visit_days_3w").cast("Double"),
		preDF3("avg_vist_3w").cast("Double"),
		preDF3("avg_drtn_3w").cast("Double"),
		preDF3("visit_grps_3w").cast("Double"),
		preDF3("visit_amnt_2w").cast("Double"),
		preDF3("visit_days_2w").cast("Double"),
		preDF3("avg_vist_2w").cast("Double"),
		preDF3("avg_drtn_2w").cast("Double"),
		preDF3("visit_grps_2w").cast("Double"),
		preDF3("visit_amnt_1w").cast("Double"),
		preDF3("visit_days_1w").cast("Double"),
		preDF3("avg_vist_1w").cast("Double"),
		preDF3("avg_drtn_1w").cast("Double"),
		preDF3("visit_grps_1w").cast("Double"),
		preDF3("recent_clct").cast("Double"),
		preDF3("tot_clct_num_1y").cast("Double"),
		preDF3("tot_clct_num_1w").cast("Double"),
		preDF3("tot_clct_num_2w").cast("Double"),
		preDF3("tot_clct_num_3w").cast("Double"),
		preDF3("tot_clct_num_1m").cast("Double"),
		preDF3("tot_clct_num_2m").cast("Double"),
		preDF3("tot_clct_num_3m").cast("Double"),
		preDF3("tot_clct_num_6m").cast("Double"),
		preDF3("recent_cart").cast("Double"),
		preDF3("tot_cart_num_1y").cast("Double"),
		preDF3("tot_cart_num_1w").cast("Double"),
		preDF3("tot_cart_num_2w").cast("Double"),
		preDF3("tot_cart_num_3w").cast("Double"),
		preDF3("tot_cart_num_1m").cast("Double"),
		preDF3("tot_cart_num_2m").cast("Double"),
		preDF3("tot_cart_num_3m").cast("Double"),
		preDF3("tot_cart_num_6m").cast("Double"),
		preDF3("recent_buy").cast("Double"),
		preDF3("cost_amnt_1y").cast("Double"),
		preDF3("order_amnt_1y").cast("Double"),
		preDF3("buy_grps_1y").cast("Double"),
		preDF3("cost_amnt_6m").cast("Double"),
		preDF3("order_amnt_6m").cast("Double"),
		preDF3("buy_grps_6m").cast("Double"),
		preDF3("cost_amnt_3m").cast("Double"),
		preDF3("order_amnt_3m").cast("Double"),
		preDF3("buy_grps_3m").cast("Double"),
		preDF3("cost_amnt_1m").cast("Double"),
		preDF3("order_amnt_1m").cast("Double"),
		preDF3("buy_grps_1m").cast("Double"),
		preDF3("cost_amnt_3w").cast("Double"),
		preDF3("order_amnt_3w").cast("Double"),
		preDF3("buy_grps_3w").cast("Double"),
		preDF3("cost_amnt_2w").cast("Double"),
		preDF3("order_amnt_2w").cast("Double"),
		preDF3("buy_grps_2w").cast("Double"),
		preDF3("cost_amnt_1w").cast("Double"),
		preDF3("order_amnt_1w").cast("Double"),
		preDF3("buy_grps_1w").cast("Double"),
		preDF3("return_amnt_1y").cast("Double"),
		preDF3("return_amnt_6m").cast("Double"),
		preDF3("return_amnt_3m").cast("Double"),
		preDF3("return_amnt_1m").cast("Double"),
		preDF3("return_amnt_3w").cast("Double"),
		preDF3("return_amnt_2w").cast("Double"),
		preDF3("return_amnt_1w").cast("Double"),
		preDF3("gender").cast("String"),
		preDF3("age_value").cast("Double"),
		preDF3("cust_level_num").cast("String"),
		preDF3("purchase_power").cast("String"),
		preDF3("loyalty_level").cast("String"),
		preDF3("scor_label").cast("String"),
		preDF3("complaints_1y").cast("Double"),
		preDF3("complaints_6m").cast("Double"),
		preDF3("complaints_3m").cast("Double"),
		preDF3("complaints_1m").cast("Double"),
		preDF3("complaints_3w").cast("Double"),
		preDF3("complaints_2w").cast("Double"),
		preDF3("complaints_1w").cast("Double"),
		preDF3("att_amnt_1y").cast("Double"),
		preDF3("att_amnt_6m").cast("Double"),
		preDF3("att_amnt_3m").cast("Double"),
		preDF3("att_amnt_1m").cast("Double"),
		preDF3("att_amnt_3w").cast("Double"),
		preDF3("att_amnt_2w").cast("Double"),
		preDF3("att_amnt_1w").cast("Double"),
		preDF3("lgs_amnt_1y").cast("Double"),
		preDF3("lgs_amnt_6m").cast("Double"),
		preDF3("lgs_amnt_3m").cast("Double"),
		preDF3("lgs_amnt_1m").cast("Double"),
		preDF3("lgs_amnt_3w").cast("Double"),
		preDF3("lgs_amnt_2w").cast("Double"),
		preDF3("lgs_amnt_1w").cast("Double"),
		preDF3("gds_cmnt_1y").cast("Double"),
		preDF3("gds_cmnt_6m").cast("Double"),
		preDF3("gds_cmnt_3m").cast("Double"),
		preDF3("gds_cmnt_1m").cast("Double"),
		preDF3("gds_cmnt_3w").cast("Double"),
		preDF3("gds_cmnt_2w").cast("Double")
		)

		//gender one-hot编码
		val indexerGender=new StringIndexer().setInputCol("gender").setOutputCol("genderIndex").fit(preDF4)
		val indexedGender=indexerGender.transform(preDF4)
		val encoderGender=new OneHotEncoder().setInputCol("genderIndex").setOutputCol("genderVec").setDropLast(false)
		val encodedGender=encoderGender.transform(indexedGender)
		//cust_level_num one-hot编码
		val indexerCustLevelNum=new StringIndexer().setInputCol("cust_level_num").setOutputCol("custLevelIndex").fit(encodedGender)
		val indexedCustLevelNum=indexerCustLevelNum.transform(encodedGender)
		val encoderCustLevelNum=new OneHotEncoder().setInputCol("custLevelIndex").setOutputCol("custLevelVec").setDropLast(false)
		val encodedCustLevelNum=encoderCustLevelNum.transform(indexedCustLevelNum)
		//purchase_power one-hot编码
		val indexerPurchasePower=new StringIndexer().setInputCol("purchase_power").setOutputCol("purchaseIndex").fit(encodedCustLevelNum)
		val indexedPurchasePower=indexerPurchasePower.transform(encodedCustLevelNum)
		val encoderPurchasePower=new OneHotEncoder().setInputCol("purchaseIndex").setOutputCol("purchaseVec").setDropLast(false)
		val encodedPurchasePower=encoderPurchasePower.transform(indexedPurchasePower)
		//loyalty_level one-hot编码
		val indexerLoyaltyLevel=new StringIndexer().setInputCol("loyalty_level").setOutputCol("loyaltyIndex").fit(encodedPurchasePower)
		val indexedLoyaltyLevel=indexerLoyaltyLevel.transform(encodedPurchasePower)
		val encoderLoyaltyLevel=new OneHotEncoder().setInputCol("loyaltyIndex").setOutputCol("loyaltyVec").setDropLast(false)
		val encodedLoyaltyLevel=encoderLoyaltyLevel.transform(indexedLoyaltyLevel)
		//scor_label one-hot编码
		val indexerScorLabel=new StringIndexer().setInputCol("scor_label").setOutputCol("scorLabelIndex").fit(encodedLoyaltyLevel)
		val indexedScorLabel=indexerScorLabel.transform(encodedLoyaltyLevel)
		val encoderScorLabel=new OneHotEncoder().setInputCol("scorLabelIndex").setOutputCol("scorLabelVec").setDropLast(false)
		val encodedScorLabel=encoderScorLabel.transform(indexedScorLabel)
		

	
		val colArrayPre=Array("visit_amnt_1y","visit_days_1y","avg_vist_1y","avg_drtn_1y","visit_grps_1y",
		"visit_amnt_6m","visit_days_6m","avg_vist_6m","avg_drtn_6m","visit_grps_6m",
		"visit_amnt_3m","visit_days_3m","avg_vist_3m","avg_drtn_3m","visit_grps_3m",
		"visit_amnt_1m","visit_days_1m","avg_vist_1m","avg_drtn_1m","visit_grps_1m",
		"visit_amnt_3w","visit_days_3w","avg_vist_3w","avg_drtn_3w","visit_grps_3w",
		"visit_amnt_2w","visit_days_2w","avg_vist_2w","avg_drtn_2w","visit_grps_2w",
		"visit_amnt_1w","visit_days_1w","avg_vist_1w","avg_drtn_1w","visit_grps_1w",
		"recent_clct","tot_clct_num_1y","tot_clct_num_1w","tot_clct_num_2w",
		"tot_clct_num_3w","tot_clct_num_1m","tot_clct_num_2m","tot_clct_num_3m",
		"tot_clct_num_6m","recent_cart","tot_cart_num_1y","tot_cart_num_1w","tot_cart_num_2w",
		"tot_cart_num_3w","tot_cart_num_1m","tot_cart_num_2m","tot_cart_num_3m",
		"tot_cart_num_6m","recent_buy","cost_amnt_1y","order_amnt_1y","buy_grps_1y",
		"cost_amnt_6m","order_amnt_6m","buy_grps_6m","cost_amnt_3m","order_amnt_3m",
		"buy_grps_3m","cost_amnt_1m","order_amnt_1m","buy_grps_1m","cost_amnt_3w",
		"order_amnt_3w","buy_grps_3w","cost_amnt_2w","order_amnt_2w","buy_grps_2w",
		"cost_amnt_1w","order_amnt_1w","buy_grps_1w","return_amnt_1y","return_amnt_6m",
		"return_amnt_3m","return_amnt_1m","return_amnt_3w","return_amnt_2w",
		"return_amnt_1w","genderVec","age_value","custLevelVec","purchaseVec",
		"loyaltyVec","scorLabelVec","complaints_1y","complaints_6m","complaints_3m",
		"complaints_1m","complaints_3w","complaints_2w","complaints_1w","att_amnt_1y",
		"att_amnt_6m","att_amnt_3m","att_amnt_1m","att_amnt_3w","att_amnt_2w","att_amnt_1w",
		"lgs_amnt_1y","lgs_amnt_6m","lgs_amnt_3m","lgs_amnt_1m","lgs_amnt_3w","lgs_amnt_2w",
		"lgs_amnt_1w","gds_cmnt_1y","gds_cmnt_6m","gds_cmnt_3m","gds_cmnt_1m","gds_cmnt_3w",
		"gds_cmnt_2w") //"child_age","reliability_score_age",
		val vecDFPre=new VectorAssembler().setInputCols(colArrayPre).setOutputCol("features").transform(encodedScorLabel)
		//输出结果
		val predictions2 = model.transform(vecDFPre)
		
		//获取概率值
		val fun:((org.apache.spark.ml.linalg.DenseVector,Double)=>Double)=(v:org.apache.spark.ml.linalg.DenseVector,i:Double)=>{v(i.toInt).toDouble}
		val sqlfunc=udf(fun)
		val PreResult=predictions2.withColumn("prob",sqlfunc(predictions2("probability"),predictions2("prediction")))
		PreResult.select("member_id","predictedLabel","prob").write.insertInto("bimining.member_churn_predict_yigou")
		
		
		
		
		//////////////////////////////////////////////模型准确度评估//////////////////////////
		
		//输出结果
		val predictions1 = model.transform(testDF)
		
		//获取概率值
		val result=predictions1.withColumn("prob",sqlfunc(predictions1("probability"),predictions1("prediction")))
		result.select("whether_churn","label","prediction","probability","prob").show(100,false)

		//模型准确度评估
		val evaluator1 = new BinaryClassificationEvaluator().setLabelCol("label")
		val accuracy1=evaluator1.evaluate(result)
		println("模型accuracy1 是 :"+accuracy1)
		result.groupBy("label","prediction").count().show()
		println("标签为1的样本数为："+data.where("whether_churn=1.0").count())
		println("标签为0的样本数为："+data.where("whether_churn=0").count())
		val TP:Double=result.where("label=0.0 and prediction=0.0").count()
		val FP:Double=result.where("label=1.0 and prediction=0.0").count()
		val FN:Double=result.where("label=0.0 and prediction=1.0").count()
		println("TP、FP、FN为："+TP+","+FP+","+FN)
		val P:Double=TP/(TP+FP)
		val R:Double=TP/(TP+FN)
		val F1:Double=2*P*R/(P+R)
		println("准确率、召回率、F1为："+P+","+R+","+F1)

	}
}




