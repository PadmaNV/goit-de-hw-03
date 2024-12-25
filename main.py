from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, isnan, avg, sum, format_number

# Створюємо сесію Spark
spark = (SparkSession.builder.appName("HW2").getOrCreate())

#1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.
#2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.

# Завантажуємо датасет
products_df = (spark.read
               .csv('/mnt/d/00_GoIT/00_Data_Engineering/HW/goit-de-hw-03/data/products.csv', header=True)
               .dropna())
purchases_df = (spark.read
                .csv('/mnt/d/00_GoIT/00_Data_Engineering/HW/goit-de-hw-03/data/purchases.csv', header=True)
                .dropna())
users_df = (spark.read
            .csv('/mnt/d/00_GoIT/00_Data_Engineering/HW/goit-de-hw-03/data/users.csv', header=True)
            .dropna())

products_df.show(5)
purchases_df.show(5)
users_df.show(5)


# Перейменовуємо колонки, щоб уникнути конфлікту
users_df = users_df.withColumnRenamed("user_id", "user_id_users")
products_df = products_df.withColumnRenamed("product_id", "product_id_products")

# Виконуємо з'єднання
joined_df = (
    purchases_df.join(users_df, purchases_df.user_id == users_df.user_id_users, 'left')
    .join(products_df, purchases_df.product_id == products_df.product_id_products, 'left')
    .select(
        'purchase_id',
        'date',
        purchases_df.user_id.alias('user_id'),
        'name',
        'age',
        'email',
        purchases_df.product_id.alias('product_id'),
        'product_name',
        'category',
        'quantity',
        'price'
    )
    .withColumn(
        "age", col("age").cast("integer")
    ).withColumn(
        "quantity", col("quantity").cast("double")
    ).withColumn(
        "price", col("price").cast("double")
    )
    .withColumn(
        'Total_amount',
        round(col('quantity').cast('double') * col('price').cast('double'), 2)
    )
    .withColumn(
        "Total_amount", col("Total_amount").cast("double")
    )
    .dropna()
)
joined_df.show(7)
joined_df.printSchema()


#3. Визначте загальну суму покупок за кожною категорією продуктів.

print("Загальна сума по категоріям: ")
joined_df.groupBy("category") \
    .sum("Total_amount") \
    .withColumnRenamed("sum(Total_amount)", "Total_amount") \
    .withColumn("Total_amount", round(col("Total_amount"), 2)) \
    .show()

#4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.

# print("Сума по категоріям для вікової категорії від 18 до 25 включно:")
# joined_df.filter((col("age") >= 18) & (col("age") <= 25)) \
#     .groupBy("category") \
#     .sum("Total_amount") \
#     .withColumnRenamed("sum(Total_amount)", "Total_amount") \
#     .withColumn("Total_amount", round(col("Total_amount"), 2)) \
#     .show()

print("Сума по категоріям для вікової категорії від 18 до 25 включно:")

joined_df.filter((col("age") >= 18) & (col("age") <= 25)) \
    .groupBy("category") \
    .agg(
        round(sum(col("Total_amount")), 2).alias("Total_amount"),  # Сума покупок
        round(avg(col("age")), 0).alias("Average_age") # Середній вік
    ) \
    .show()

#5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.

print("Частка покупок за кожною категорією для вікової категорії від 18 до 25 включно:")

joined_df.filter((col("age") >= 18) & (col("age") <= 25)) \
    .groupBy("category") \
    .agg(
        round(sum(col("Total_amount")), 2).alias("Total_amount")
    ) \
    .withColumn(
        "Raw_Share",
        (col("Total_amount") /
         joined_df.filter((col("age") >= 18) & (col("age") <= 25))
         .groupBy()
         .sum("Total_amount")
         .collect()[0][0]) * 100
    ) \
    .withColumn(
        "Share", format_number(col("Raw_Share"), 2)  # Форматуємо для виводу
    ) \
    .orderBy(col("Raw_Share").desc()) \
    .drop("Raw_Share") \
    .show()




#6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.

print("ТОП 3 категорії витрат зі споживачами віком від 18 до 25 років:")

joined_df.filter((col("age") >= 18) & (col("age") <= 25)) \
    .groupBy("category") \
    .agg(
        round(sum(col("Total_amount")), 2).alias("Total_amount")
    ) \
    .withColumn(
        "Raw_Share",
        (col("Total_amount") /
         joined_df.filter((col("age") >= 18) & (col("age") <= 25))
         .groupBy()
         .sum("Total_amount")
         .collect()[0][0]) * 100
    ) \
    .withColumn(
        "Share", format_number(col("Raw_Share"), 2)  # Форматуємо для виводу
    ) \
    .orderBy(col("Raw_Share").desc()) \
    .drop("Raw_Share") \
    .limit(3) \
    .show()


spark.stop()