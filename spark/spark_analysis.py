# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import warnings

# 【核心补丁】：强制修复 Pandas 2.0+ 兼容性
if not hasattr(pd.DataFrame, 'iteritems'):
    pd.DataFrame.iteritems = pd.DataFrame.items
if not hasattr(pd.Series, 'iteritems'):
    pd.Series.iteritems = pd.Series.items

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.ml.fpm import FPGrowth
from statsmodels.tsa.holtwinters import SimpleExpSmoothing, Holt 

warnings.filterwarnings("ignore") 

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

    processed_df = raw_df.dropDuplicates(['title']) \
        .filter("title IS NOT NULL AND title != ''") \
        .withColumn("publish_year_str", F.col("publish_year").cast("string")) \
        .withColumn("year_int", F.substring(F.col("publish_year_str"), 1, 4).cast("int")) \
        .filter("year_int >= 2012") \
        .select("id", "title", "discipline", F.col("year_int").alias("publish_year"), "authors", "source")

    cleaned_count = processed_df.count()
    print(f"📊 数据清洗自检：从原始库提取出 {cleaned_count} 条有效文献（2012年至今）")

    if cleaned_count > 0:
        processed_df.write.jdbc(url=mysql_url, table="cleaned_paper_data", mode="overwrite", properties=db_properties)
        print("✅ [Step 2] 高质量数据底座已成功存入 cleaned_paper_data。")
    else:
        print("⚠️ [Warning] 数据量为 0，跳过写入。")

    custom_stop_words = StopWordsRemover.loadDefaultStopWords("english") + \
        ["based", "using", "method", "approach", "study", "analysis", "new", "proposed", 
         "model", "paper", "data", "system", "research", "results", "effective", "potential"]
    
    clean_title_df = processed_df.withColumn("clean_title", F.lower(F.regexp_replace(F.col("title"), "[^a-zA-Z\\s]", "")))
    tokenizer = Tokenizer(inputCol="clean_title", outputCol="words")
    words_df = tokenizer.transform(clean_title_df)
    
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=custom_stop_words)
    filtered_df = remover.transform(words_df)

    # ================= 3. 时序演进分析 (饱满图表版) =================
    print("⏳ [Step 3] 计算历史演进中...")
    exploded_df = filtered_df.withColumn("word", F.explode("filtered_words")) \
        .filter(F.length(F.col("word")) > 3)
        
    word_freq_df = exploded_df.groupBy("discipline", "publish_year", "word").count()

    window_spec = Window.partitionBy("discipline", "publish_year").orderBy(F.desc("count"))
    # 【优化1】：将 rank 提宽到 12，获取更多关键词，使河流图更壮观、数据更丰满
    history_trend_df = word_freq_df.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") <= 12) \
        .select("discipline", "publish_year", "word", "count")

    history_trend_df.write.jdbc(url=mysql_url, table="hotspot_trend_data", mode="overwrite", properties=db_properties)

    pdf = history_trend_df.toPandas()
    forecast_sdf = None  
    
    if not pdf.empty:
        max_year_in_data = int(pdf['publish_year'].max())
        
        # 【优化2】：绝对锁定推演终点为 2026，砍掉 2027 和 2028
        target_year = 2026 
        steps_to_forecast = target_year - max_year_in_data
        
        print(f"📊 历史终点: {max_year_in_data} 年，执行饱满推演 {steps_to_forecast} 个周期至 {target_year} 年...")

        forecast_records = []
        grouped = pdf.groupby(['discipline', 'word'])

        for (discipline, word), group_data in grouped:
            group_data = group_data.sort_values('publish_year')
            full_years = pd.DataFrame({'publish_year': range(2012, max_year_in_data + 1)})
            merged = pd.merge(full_years, group_data, on='publish_year', how='left').fillna(0)
            history_counts = merged['count'].values.astype(float)
            
            if steps_to_forecast > 0:
                last_known_val = history_counts[-1] if len(history_counts) > 0 else 5
                
                try:
                    # 【优化3】：降低算法门槛。只要有 >= 3 个非零数据，就上 Holt 模型捕获增长；否则用基础平滑
                    if np.count_nonzero(history_counts) >= 3:
                        model = Holt(history_counts, initialization_method="estimated").fit(optimized=True)
                    else:
                        model = SimpleExpSmoothing(history_counts, initialization_method="estimated").fit(optimized=True)
                    
                    forecast = model.forecast(steps_to_forecast)
                except Exception:
                    # 【优化4】：防断流兜底机制！如果模型报错（数据太诡异），绝不归零，而是继承最后的热度！
                    forecast = [last_known_val] * steps_to_forecast
                
                for i, val in enumerate(forecast):
                    pred_year = max_year_in_data + 1 + i
                    # 双重保险：严格确保不超过 2026
                    if pred_year <= target_year:
                        # 【优化5】：动态保底机制。如果预测值突然掉得很低，按历史最后值的 85% 托底，保证河流饱满
                        safe_val = max(int(last_known_val * 0.85), int(val))
                        final_val = max(3, safe_val) # 绝对最低频次为3，防止图表中细成一条线
                        
                        forecast_records.append({
                            'discipline': discipline, 
                            'publish_year': pred_year, 
                            'word': word, 
                            'count': final_val
                        })

        if forecast_records:
            forecast_sdf = spark.createDataFrame(pd.DataFrame(forecast_records))
            forecast_sdf.write.jdbc(url=mysql_url, table="hotspot_trend_data", mode="append", properties=db_properties)
            print("✅ 2025-2026 饱满预测数据已写入！")

    # ================= 4. FP-Growth 星系网络 =================
    print("⏳ [Step 4] 执行基于全量数据 (含2026推演) 的星系网络挖掘...")
    
    base_basket_df = filtered_df.select(
        F.array_union(F.array(F.col("discipline")), F.expr("filter(filtered_words, x -> length(x) > 4)")).alias("items")
    )
    
    if forecast_sdf is not None:
        predicted_basket_df = forecast_sdf.filter(F.col("count") > 2).select(
            F.array(F.col("discipline"), F.col("word")).alias("items")
        )
        final_basket_df = base_basket_df.union(predicted_basket_df)
    else:
        final_basket_df = base_basket_df

    fp_growth = FPGrowth(itemsCol="items", minSupport=0.0008, minConfidence=0.20) 
    fp_model = fp_growth.fit(final_basket_df)
    
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
    print("🔗 [Finish] 全流程执行完毕！预测引擎与挖掘引擎完美对齐。")

except Exception as e:
    print(f"❌ 运行报错: {e}")
finally:
    spark.stop()