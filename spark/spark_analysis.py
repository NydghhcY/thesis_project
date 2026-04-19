# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# 【核心补丁】：强制修复 Pandas 2.0+ 兼容性
if not hasattr(pd.DataFrame, 'iteritems'):
    pd.DataFrame.iteritems = pd.DataFrame.items
if not hasattr(pd.Series, 'iteritems'):
    pd.Series.iteritems = pd.Series.items

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.ml.fpm import FPGrowth
from statsmodels.tsa.holtwinters import SimpleExpSmoothing

# ================= 1. 初始化 Spark Session =================
spark = SparkSession.builder \
    .appName("Thesis_Hotspot_Analysis_Final_Stable") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "500") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

mysql_url = "jdbc:mysql://192.168.88.130:3306/thesis_db?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai"
db_properties = {"user": "root", "password": "123456", "driver": "com.mysql.cj.jdbc.Driver"}

print("✅ [Step 1] Spark 引擎启动...")

try:
    # ================= 2. 数据读取与预处理 =================
    raw_df = spark.read.jdbc(url=mysql_url, table="raw_paper_data", properties=db_properties)

    # 【优化】：增强年份提取能力。如果年份是 "2024-01-01" 这种格式，substring(1,4) 能确保拿到 2024
    processed_df = raw_df.dropDuplicates(['title']) \
        .filter("title IS NOT NULL AND title != ''") \
        .withColumn("publish_year_str", F.col("publish_year").cast("string")) \
        .withColumn("year_int", F.substring(F.col("publish_year_str"), 1, 4).cast("int")) \
        .filter("year_int >= 2012") \
        .select("id", "title", "discipline", F.col("year_int").alias("publish_year"), "authors", "source")

    # 【重要】：在写入前先统计一下条数！
    cleaned_count = processed_df.count()
    print(f"📊 数据清洗自检：从原始库提取出 {cleaned_count} 条有效文献（2012年至今）")

    if cleaned_count > 0:
        processed_df.write.jdbc(url=mysql_url, table="cleaned_paper_data", mode="overwrite", properties=db_properties)
        print("✅ [Step 2] 高质量数据底座已成功存入 cleaned_paper_data。")
    else:
        print("⚠️ [Warning] 清洗后数据量为 0！请检查原始表 raw_paper_data 是否有数据，或年份字段格式是否正确。跳过写入防止清空表。")

    # 增强型停用词库
    custom_stop_words = StopWordsRemover.loadDefaultStopWords("english") + \
        ["based", "using", "method", "approach", "study", "analysis", "new", "proposed", 
         "model", "paper", "data", "system", "research", "results", "effective", "potential"]
    
    clean_title_df = processed_df.withColumn("clean_title", F.lower(F.regexp_replace(F.col("title"), "[^a-zA-Z\\s]", "")))
    tokenizer = Tokenizer(inputCol="clean_title", outputCol="words")
    words_df = tokenizer.transform(clean_title_df)
    
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=custom_stop_words)
    filtered_df = remover.transform(words_df)

    # ================= 3. 时序演进分析 =================
    print("⏳ [Step 3] 计算历史演进中...")
    exploded_df = filtered_df.withColumn("word", F.explode("filtered_words")) \
        .filter(F.length(F.col("word")) > 3)
        
    word_freq_df = exploded_df.groupBy("discipline", "publish_year", "word").count()

    window_spec = Window.partitionBy("discipline", "publish_year").orderBy(F.desc("count"))
    history_trend_df = word_freq_df.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") <= 8) \
        .select("discipline", "publish_year", "word", "count")

    history_trend_df.write.jdbc(url=mysql_url, table="hotspot_trend_data", mode="overwrite", properties=db_properties)

    # 转换 Pandas 进行 EMA 预测
    pdf = history_trend_df.toPandas()
    if not pdf.empty:
        max_year_in_data = int(pdf['publish_year'].max())
        steps_to_forecast = 2028 - max_year_in_data
        
        print(f"📊 历史终点: {max_year_in_data} 年，外推 {steps_to_forecast} 个周期...")

        forecast_records = []
        grouped = pdf.groupby(['discipline', 'word'])

        for (discipline, word), group_data in grouped:
            group_data = group_data.sort_values('publish_year')
            full_years = pd.DataFrame({'publish_year': range(2012, max_year_in_data + 1)})
            merged = pd.merge(full_years, group_data, on='publish_year', how='left').fillna(0)
            history_counts = merged['count'].values.astype(float)
            
            if len(history_counts) >= 5 and steps_to_forecast > 0:
                try:
                    model = SimpleExpSmoothing(history_counts, initialization_method="heuristic").fit(smoothing_level=0.5)
                    forecast = model.forecast(steps_to_forecast)
                    for i, val in enumerate(forecast):
                        forecast_records.append({
                            'discipline': discipline, 
                            'publish_year': max_year_in_data + 1 + i, 
                            'word': word, 
                            'count': max(1, int(val))
                        })
                except: pass

        if forecast_records:
            forecast_sdf = spark.createDataFrame(pd.DataFrame(forecast_records))
            forecast_sdf.write.jdbc(url=mysql_url, table="hotspot_trend_data", mode="append", properties=db_properties)
            print("✅ 2026-2028 预测轨道已追加！")

    # ================= 4. FP-Growth 星系网络 =================
    print("⏳ [Step 4] 执行星系网络挖掘...")
    basket_df = filtered_df.select(
        F.array_union(F.array(F.col("discipline")), F.expr("filter(filtered_words, x -> length(x) > 4)")).alias("items")
    )

    fp_growth = FPGrowth(itemsCol="items", minSupport=0.001, minConfidence=0.15) 
    fp_model = fp_growth.fit(basket_df)
    
    rules = fp_model.associationRules
    flat_rules = rules.filter(F.size(F.col("antecedent")) == 1) \
        .filter(F.size(F.col("consequent")) == 1) \
        .select(
            F.col("antecedent")[0].alias("source"),
            F.col("consequent")[0].alias("target"),
            F.col("confidence")
        )

    window_spec_fp = Window.partitionBy("source").orderBy(F.desc("confidence"))
    balanced_rules = flat_rules.withColumn("rank", F.row_number().over(window_spec_fp)) \
        .filter(F.col("rank") <= 8).drop("rank")

    balanced_rules.write.jdbc(url=mysql_url, table="fp_growth_rules", mode="overwrite", properties=db_properties)
    print("🔗 [Finish] 全流程执行完毕！")

except Exception as e:
    print(f"❌ 运行报错: {e}")
finally:
    spark.stop()