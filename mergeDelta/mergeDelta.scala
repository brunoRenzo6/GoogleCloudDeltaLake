package example


import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object hello
{
    def main(args: Array[String])
    {
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
		
		import spark.implicits._
		import io.delta._
		import io.delta.tables._

		//MERGE DELTA TABLE


		import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType,DateType}
		import java.sql.Date

		val updateSeq = Seq(Row(3, "2020-11-03", "Duchas", 1),
		                    Row(6, "2020-04-26", "Tomadas e Interruptores", 2),
		                    Row(7, "2020-04-26", "Cabos e Fios Eletricos", 2),
		                    Row(8, "2020-11-03", "Tijolo", 2))

		val someSchema = List(
		  StructField("cod_categoria_produto", IntegerType, false),
		  StructField("data_inicio", StringType, false),
		  StructField("categoria_produto", StringType, false),
		  StructField("cod_grupo_produto", IntegerType, false)
		)

		val updateDF = spark.createDataFrame(
		  spark.sparkContext.parallelize(updateSeq),
		  StructType(someSchema)
		).withColumn("data_inicio", col("data_inicio").cast("date"))

		//display(updateDF)


		// ==========================================
		// Merge Scala API is available since DBR 6.0
		// ==========================================

		spark.sql("CREATE TABLE categoriaProduto USING DELTA LOCATION 'gs://bkt-scd-delta-1/categoriaProduto'")

		val categoriaProduto: DeltaTable =   // table with schema (customerId, address, current, effectiveDate, endDate)
		  DeltaTable.forPath("gs://bkt-scd-delta-1/categoriaProduto")

		val updatesDF = updateDF

		// Rows to INSERT new addresses of existing customers
		val newAddressesToInsert = updatesDF
		  .as("updates")
		  .join(categoriaProduto.toDF.as("categoriaProduto"), "cod_categoria_produto")
		  .where("categoriaProduto.data_end is null AND updates.cod_grupo_produto <> categoriaProduto.cod_grupo_produto")

		// Stage the update by unioning two sets of rows
		// 1. Rows that will be inserted in the `whenNotMatched` clause
		// 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
		val stagedUpdates = newAddressesToInsert
		  .selectExpr("NULL as mergeKey", "updates.*")   // Rows for 1.
		  .union(
		    updatesDF.selectExpr("cod_categoria_produto as mergeKey", "*")  // Rows for 2.
		  )

		// Apply SCD Type 2 operation using merge
		categoriaProduto
		  .as("categoriaProduto")
		  .merge(
		    stagedUpdates.as("staged_updates"),
		    "categoriaProduto.cod_categoria_produto = mergeKey")
		  .whenMatched("categoriaProduto.data_end is null AND categoriaProduto.cod_grupo_produto <> staged_updates.cod_grupo_produto")
		  .updateExpr(Map(                                      // Set current to false and endDate to source's effective date.
		    "data_end" -> "staged_updates.data_inicio"))
		  .whenNotMatched()
		  .insertExpr(Map(
		    "cod_categoria_produto" -> "staged_updates.cod_categoria_produto",
		    "data_inicio" -> "staged_updates.data_inicio",
		    "categoria_produto" -> "staged_updates.categoria_produto",
		    "cod_grupo_produto" -> "staged_updates.cod_grupo_produto",  // Set current to true along with the new address and its effective date.
		    "data_end" -> "null"))
		  .execute()



		spark.stop()
    }
}
