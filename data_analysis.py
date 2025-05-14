from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, count as spark_count
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pandas as pd
from pyecharts.charts import Bar, Line, Pie, Funnel
from pyecharts import options as opts
import pymysql

# 初始化 Spark
spark = (
    SparkSession.builder.appName("UserBehaviorAnalysis")
    .config(
        "spark.jars",
        "/home/lynn/bigdata/spark-3.2.0/jars/mysql-connector-java-8.0.31.jar",
    )
    .getOrCreate()
)

# 读取数据
df = spark.read.csv(
    "file:///home/lynn/demo/raw_user.csv", header=True, inferSchema=True
)

# 删除 user_geohash 列
df_cleaned = df.drop("user_geohash")

# 清洗无效行
df_filtered = df_cleaned.filter(
    (col("behavior_type").between(1, 4)) & col("item_category").isNotNull()
)

# 时间字段提取
df_parsed = (
    df_filtered.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd HH"))
    .withColumn("year", date_format(col("time"), "yyyy").cast("integer"))
    .withColumn("month", date_format(col("time"), "MM").cast("integer"))
    .withColumn("day", date_format(col("time"), "dd").cast("integer"))
    .withColumn("hour", date_format(col("time"), "HH").cast("integer"))
)

# 只保留购买行为（behavior_type == 4）
purchase_df = df_parsed.filter(col("behavior_type") == 4)

# 1. 商品购买 Top 10
top_items = (
    purchase_df.groupBy("item_id")
    .agg(spark_count("*").alias("count"))
    .orderBy("count", ascending=False)
    .limit(10)
)
top_items_pd = top_items.toPandas()

# 2. 月度购买趋势
monthly_purchase = (
    purchase_df.groupBy("month").agg(spark_count("*").alias("count")).orderBy("month")
)
monthly_purchase_pd = monthly_purchase.toPandas()

# 3. 商品分类购买分布
category_purchase = (
    purchase_df.groupBy("item_category")
    .agg(spark_count("*").alias("count"))
    .orderBy("count", ascending=False)
)
category_purchase_pd = category_purchase.toPandas()

# 4. 用户行为转化漏斗分析（浏览 → 收藏 → 加购 → 购买）
behavior_stats = (
    df_parsed.groupBy("behavior_type").agg(spark_count("*").alias("count")).toPandas()
)
behavior_dict = behavior_stats.set_index("behavior_type")["count"].to_dict()

total_views = behavior_dict.get(1, 0)
total_purchases = behavior_dict.get(4, 0)
conversion_rate = (total_purchases / total_views * 100) if total_views > 0 else 0

stages = [
    ("浏览", total_views),
    ("收藏", behavior_dict.get(2, 0)),
    ("加购物车", behavior_dict.get(3, 0)),
    ("购买", total_purchases),
]

# 5. 小时级活跃度分析
hourly_trend = (
    purchase_df.groupBy("hour").agg(spark_count("*").alias("count")).orderBy("hour")
)
hourly_trend_pd = hourly_trend.toPandas()

# 6. 用户分群分析（KMeans）
user_features = df_parsed.groupBy("user_id").agg(
    F.count("item_id").alias("total_actions"),
    F.sum(F.when(col("behavior_type") == 4, 1).otherwise(0)).alias("purchase_count"),
)

assembler = VectorAssembler(
    inputCols=["total_actions", "purchase_count"], outputCol="features"
)
feature_df = assembler.transform(user_features)

kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(feature_df)
predictions = model.transform(feature_df).select("user_id", "prediction").toPandas()

# 7. 商品销量预测模型（线性回归）
item_behavior = (
    df_parsed.filter(col("behavior_type").isin([1, 4]))
    .groupBy("item_id")
    .agg(
        spark_count(F.when(col("behavior_type") == 1, 1)).alias("views"),
        spark_count(F.when(col("behavior_type") == 4, 1)).alias("purchases"),
    )
    .filter(col("purchases") > 0)
)

# 特征转换
assembler = VectorAssembler(inputCols=["views"], outputCol="features")
regression_data = assembler.transform(item_behavior)

# 线性回归训练
lr = LinearRegression(featuresCol="features", labelCol="purchases")
lr_model = lr.fit(regression_data)

# 预测与图表化
lr_predictions = lr_model.transform(regression_data)
pred_pd = lr_predictions.select("item_id", "purchases", "prediction").toPandas()

# ====================== 生成 HTML 图表 ======================
# 1. Top10 商品柱状图
bar = (
    Bar()
    .add_xaxis(top_items_pd["item_id"].astype(str).tolist())
    .add_yaxis("购买量", top_items_pd["count"].tolist())
    .set_global_opts(title_opts=opts.TitleOpts(title="Top 10 商品"))
)
bar.render("top_items.html")

# 2. 月度趋势折线图
line_month = (
    Line()
    .add_xaxis(monthly_purchase_pd["month"].astype(int).astype(str).tolist())
    .add_yaxis("购买量", monthly_purchase_pd["count"].tolist(), is_smooth=True)
    .set_global_opts(title_opts=opts.TitleOpts(title="月度购买趋势"))
)
line_month.render("monthly_trend.html")

# 3. 商品分类饼图
pie_category = (
    Pie()
    .add(
        "",
        [
            list(z)
            for z in zip(
                category_purchase_pd["item_category"].astype(str).tolist(),
                category_purchase_pd["count"].tolist(),
            )
        ],
    )
    .set_global_opts(title_opts=opts.TitleOpts(title="商品分类分布"))
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))
)
pie_category.render("category_distribution.html")

# 4. 行为转化漏斗图
funnel = (
    Funnel()
    .add("用户行为", stages)
    .set_global_opts(title_opts=opts.TitleOpts(title="用户行为转化漏斗"))
)
funnel.render("conversion_funcel.html")

# 5. 每小时活跃度图
line_hour = (
    Line()
    .add_xaxis(hourly_trend_pd["hour"].astype(int).astype(str).tolist())
    .add_yaxis("购买量", hourly_trend_pd["count"].tolist())
    .set_global_opts(title_opts=opts.TitleOpts(title="小时级活跃度"))
)
line_hour.render("hourly_trend.html")

# 6. 用户分群饼图
cluster_counts = predictions.groupby("prediction").size().reset_index(name="count")
pie_cluster = (
    Pie()
    .add(
        "",
        [
            list(z)
            for z in zip(
                cluster_counts.index.astype(str).tolist(),
                cluster_counts["count"].tolist(),
            )
        ],
    )
    .set_global_opts(title_opts=opts.TitleOpts(title="用户分群分布"))
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))
)
pie_cluster.render("user_clusters.html")

# 7. 商品销量预测图
line_pred = (
    Line()
    .add_xaxis(pred_pd["item_id"].astype(str).tolist())
    .add_yaxis("实际购买量", pred_pd["purchases"].tolist())
    .add_yaxis(
        "预测购买量",
        pred_pd["prediction"].round().astype(int).tolist(),
        is_smooth=True,
        linestyle_opts=opts.LineStyleOpts(opacity=0.5),
    )
    .set_global_opts(title_opts=opts.TitleOpts(title="商品销量预测 vs 实际"))
)
line_pred.render("item_sales_prediction.html")

print("✅ 所有分析完成，HTML 图表已生成！")
