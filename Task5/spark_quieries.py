from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max, sum, count, unix_timestamp, first, upper
from pyspark.sql.functions import desc
from pyspark.sql.window import Window
from pyspark.sql.functions import when
from dotenv import load_dotenv
import os

spark = SparkSession.builder.appName("LoadDataFromPostgres").config("spark.jars", "C:\\Users\\User1\\drivers\\postgresql-42.7.4.jar").getOrCreate()

url = "jdbc:postgresql://localhost:5432/pagila" 


load_dotenv()

properties = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER"),
}
def query1():
    df_categories = spark.read.jdbc(url=url, table="category", properties=properties)
    df_film = spark.read.jdbc(url=url, table="film", properties=properties)
    df_film_categories = spark.read.jdbc(url=url, table="film_category", properties=properties)

    df_film_categories_alias = df_film_categories.alias("fc")
    df_film_alias = df_film.alias("f")
    df_categories_alias = df_categories.alias("c")

    joined_df = df_film_categories_alias \
        .join(df_film_alias, df_film_alias.film_id == df_film_categories_alias.film_id, how="inner") \
        .join(df_categories_alias, df_film_categories_alias.category_id == df_categories_alias.category_id, how="inner")

    category_count_df = joined_df.groupBy("c.name").count()

    sorted_category_count_df = category_count_df.orderBy(col("count").desc())

    sorted_category_count_df.show()

def query2():
    df_actor = spark.read.jdbc(url=url, table="actor", properties=properties)
    df_film = spark.read.jdbc(url=url, table="film", properties=properties)
    df_film_actor = spark.read.jdbc(url=url, table="film_actor", properties=properties)
    df_rental = spark.read.jdbc(url=url, table="rental", properties=properties)
    df_inventory = spark.read.jdbc(url=url, table="inventory", properties=properties)

    df_actor_alias = df_actor.alias("a")
    df_film_alias = df_film.alias("f")
    df_film_actor_alias = df_film_actor.alias("fa")
    df_rental_alias = df_rental.alias("r")
    df_inventory_alias = df_inventory.alias("i")

    joined_df = df_film_actor_alias \
        .join(df_actor_alias, df_film_actor_alias.actor_id == df_actor_alias.actor_id, how="inner") \
        .join(df_film_alias, df_film_actor_alias.film_id == df_film_alias.film_id, how="inner") \
        .join(df_inventory_alias, df_inventory_alias.film_id == df_film_alias.film_id, how="inner") \
        .join(df_rental_alias, df_inventory_alias.inventory_id == df_rental_alias.inventory_id, how="inner")

    actor_rental_count_df = joined_df.groupBy("a.actor_id", "a.first_name", "a.last_name").count()

    sorted_actor_rental_count_df = actor_rental_count_df.orderBy(col("count").desc())

    sorted_actor_rental_count_df.limit(10).show()

def query3():

    df_category = spark.read.jdbc(url=url, table="category", properties=properties)
    df_film_category = spark.read.jdbc(url=url, table="film_category", properties=properties)
    df_film = spark.read.jdbc(url=url, table="film", properties=properties)
    df_inventory = spark.read.jdbc(url=url, table="inventory", properties=properties)
    df_rental = spark.read.jdbc(url=url, table="rental", properties=properties)
    df_payment = spark.read.jdbc(url=url, table="payment", properties=properties)

    result = (
        df_category
        .join(df_film_category, df_category.category_id == df_film_category.category_id)
        .join(df_film, df_film_category.film_id == df_film.film_id)
        .join(df_inventory, df_film.film_id == df_inventory.film_id)
        .join(df_rental, df_inventory.inventory_id == df_rental.inventory_id)
        .join(df_payment, df_rental.rental_id == df_payment.rental_id)
        .groupBy(df_category.name)
        .agg(sum(df_payment.amount).alias("total_spent"))
        .orderBy(desc("total_spent"))
        .limit(3)
    )

    result.show()

def query4():
    df_film = spark.read.jdbc(url=url, table="film", properties=properties)
    df_inventory = spark.read.jdbc(url=url, table="inventory", properties=properties)

    result = (
        df_film
        .join(df_inventory, df_film.film_id == df_inventory.film_id, "left")
        .filter(df_inventory.film_id.isNull()).orderBy(df_film.title)
        .select(df_film.title)
    )

    result.show()

def query5():
    df_actor = spark.read.jdbc(url=url, table="actor", properties=properties)
    df_film_actor = spark.read.jdbc(url=url, table="film_actor", properties=properties)
    df_film = spark.read.jdbc(url=url, table="film", properties=properties)
    df_film_category = spark.read.jdbc(url=url, table="film_category", properties=properties)
    df_category = spark.read.jdbc(url=url, table="category", properties=properties)

    result = (
        df_actor
        .join(df_film_actor, df_actor.actor_id == df_film_actor.actor_id)
        .join(df_film, df_film_actor.film_id == df_film.film_id)
        .join(df_film_category, df_film.film_id == df_film_category.film_id)
        .join(df_category, df_film_category.category_id == df_category.category_id)
        .filter(df_category.name == "Children")
        .groupBy(df_actor.actor_id, df_actor.first_name, df_actor.last_name)
        .agg(count(df_film_actor.film_id).alias("film_count"))
        .orderBy(desc("film_count"))
        .limit(3)
    )

    result.show()

def query6():
    df_customer = spark.read.jdbc(url=url, table="customer", properties=properties)
    df_address = spark.read.jdbc(url=url, table="address", properties=properties)
    df_city = spark.read.jdbc(url=url, table="city", properties=properties)

    result = (
        df_customer
        .join(df_address, df_customer.address_id == df_address.address_id)
        .join(df_city, df_address.city_id == df_city.city_id)
        .groupBy(df_city.city)
        .agg(
            sum(when(df_customer.active == 1, 1).otherwise(0)).alias("active_customers"),
            sum(when(df_customer.active == 0, 1).otherwise(0)).alias("inactive_customers")
        )
        .orderBy(desc("inactive_customers"))
    )

    result.show()

def query7():
    df_rental = spark.read.jdbc(url=url, table="rental", properties=properties)
    df_customer = spark.read.jdbc(url=url, table="customer", properties=properties)
    df_address = spark.read.jdbc(url=url, table="address", properties=properties)
    df_city = spark.read.jdbc(url=url, table="city", properties=properties)
    df_inventory = spark.read.jdbc(url=url, table="inventory", properties=properties)
    df_film = spark.read.jdbc(url=url, table="film", properties=properties)
    df_film_category = spark.read.jdbc(url=url, table="film_category", properties=properties)
    df_category = spark.read.jdbc(url=url, table="category", properties=properties)

    rental_hours = (
        df_rental
        .join(df_customer, df_rental.customer_id == df_customer.customer_id)
        .join(df_address, df_customer.address_id == df_address.address_id)
        .join(df_city, df_address.city_id == df_city.city_id)
        .join(df_inventory, df_rental.inventory_id == df_inventory.inventory_id)
        .join(df_film, df_inventory.film_id == df_film.film_id)
        .join(df_film_category, df_film.film_id == df_film_category.film_id)
        .join(df_category, df_film_category.category_id == df_category.category_id)
        .filter((df_city.city.contains('-')) | (upper(df_city.city).startswith('A')))
        .withColumn(
            "total_hours",
            sum((unix_timestamp(df_rental.return_date) - unix_timestamp(df_rental.rental_date)) / 3600)
            .over(Window.partitionBy(df_city.city, df_category.name))
        )
        .groupBy(df_city.city, df_category.name)
        .agg(first("total_hours").alias("total_hours"))
    )

    window_spec = Window.partitionBy("city")
    result = (
        rental_hours
        .withColumn("max_hours", max("total_hours").over(window_spec))
        .filter(col("total_hours") == col("max_hours"))
        .orderBy("city", "name")
        .select("city", "name", "total_hours")
    )

    result.show()


query7()