package example


import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.functions._



object hello
{
    def main(args: Array[String])
    {
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
		
		import spark.implicits._
		import io.delta._


		//READ CSV FILES AND SAVE AS DELTA


		//---------------------------------------
		//-----Balconista------------------------

		var mySchema = "cod_balconista int, data_inicio date, data_fim date, nome_balconista string, cod_loja int";

		var tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/balconista.csv");

		var tmp_df2 = tmp_df.drop("data_fim")

		var window = Window.partitionBy("cod_balconista").orderBy($"cod_balconista", $"data_inicio".desc)
		var tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/balconista")

		//---------------------------------------
		//-----GrupoProduto----------------------

		mySchema = "cod_grupo_produto int, data_inicio date, data_fim date, grupo_produto string";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/grupo_produto.csv");

		tmp_df2 = tmp_df.drop("data_fim")

		window = Window.partitionBy("cod_grupo_produto").orderBy($"cod_grupo_produto", $"data_inicio".desc)
		tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/grupoProduto")

		//---------------------------------------
		//-----CategoriaProduto------------------

		mySchema = "cod_categoria_produto int, data_inicio date, data_fim date, categoria_produto string, cod_grupo_produto int";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/categoria_produto.csv");

		tmp_df2 = tmp_df.drop("data_fim")

		window = Window.partitionBy("cod_categoria_produto").orderBy($"cod_categoria_produto", $"data_inicio".desc)
		tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/categoriaProduto")

		//---------------------------------------
		//-----Regional--------------------------
		mySchema = "cod_regional int, data_inicio date, data_fim date, regional string";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/regional.csv");

		tmp_df2 = tmp_df.drop("data_fim")

		window = Window.partitionBy("cod_regional").orderBy($"cod_regional", $"data_inicio".desc)
		tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/regional")
		//---------------------------------------
		//-----PontosAcumulados------------------
		mySchema = "cod_loja int, cod_balconista int, data_referencia date, pontos decimal(10,2)";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/pontos_acumulados.csv");

		tmp_df.write.format("delta").save("gs://bkt-scd-delta-1/pontosAcumulados")
		//---------------------------------------
		//-----Produto---------------------------
		mySchema = "cod_produto int, cod_categoria_produto int, produto string, data_inicio date, data_fim date";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/produto.csv");

		tmp_df2 = tmp_df.drop("data_fim")

		window = Window.partitionBy("cod_produto").orderBy($"cod_produto", $"data_inicio".desc)
		tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/produto")
		//---------------------------------------
		//-----Loja------------------------------
		mySchema = "cod_loja int, loja string, cod_regional int, data_inicio date, data_fim date";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/loja.csv");

		tmp_df2 = tmp_df.drop("data_fim")

		window = Window.partitionBy("cod_loja").orderBy($"cod_loja", $"data_inicio".desc)
		tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/loja")
		//---------------------------------------
		//-----Meta------------------------------
		mySchema = "cod_loja int, meta_mensal decimal(10,2), data_inicio date, data_fim date";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/meta.csv");

		tmp_df2 = tmp_df.drop("data_fim")

		window = Window.partitionBy("cod_loja").orderBy($"cod_loja", $"data_inicio".desc)
		tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/meta")
		//---------------------------------------
		//-----PontosProduto---------------------
		mySchema = "cod_produto int, pontos decimal(10,2), data_inicio date, data_fim date";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/pontos_produtos.csv");

		tmp_df2 = tmp_df.drop("data_fim")

		window = Window.partitionBy("cod_produto").orderBy($"cod_produto", $"data_inicio".desc)
		tmp_df3 = tmp_df2.withColumn("data_end", lag($"data_inicio", 1, null).over(window))

		tmp_df3.write.format("delta").save("gs://bkt-scd-delta-1/pontosProduto")
		//---------------------------------------
		//-----Vendas----------------------------
		mySchema = "cod_loja int, cod_produto int,  cod_balconista int, data_venda date, valor_venda decimal(10,2), qtd_venda int";

		tmp_df = spark
		  .read
		  .option("delimiter",";")
		  .option("dateFormat","yyyyMMdd")
		  .schema(mySchema)
		  .csv("gs://bkt-scd-raw-1/venda.csv");


		tmp_df.write.format("delta").save("gs://bkt-scd-delta-1/vendas")
		//---------------------------------------


















		spark.stop()
    }
}
