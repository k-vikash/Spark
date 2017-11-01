import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class GdpData {
	/**
	 * 
	 * @param args
	 */

	public static void main(String args[]) {

		SparkSession spark = SparkSession.builder().appName("org.spark.Gdp")
				.master("local[*]").enableHiveSupport().getOrCreate();
		final DataFrameReader dataFrameReader = spark.read();

		dataFrameReader.option("header", "true");

		Dataset<Row> df = dataFrameReader.csv("gdp.csv");

		df = df.withColumnRenamed("Country Name", "Country_Name");
		df = df.withColumnRenamed("Country Code", "Country_Code");

		df.createOrReplaceTempView("df_sql");

		Dataset<Row> vl = spark
				.sql("select sql2.Year, sql1.Country_Name, ((sql2.Value -sql1.Value)/sql1.Value) as percentage_increase from df_sql sql1, df_sql sql2 Where sql1.Country_Name= sql2.Country_Name AND ((sql2.Year-sql1.Year)==1)");
		// creating sql view
		vl.createOrReplaceTempView("sqlvi");

		vl = spark
				.sql("select Year, MAX(percentage_increase)as value from sqlvi GROUP BY Year");

		vl.createOrReplaceTempView("sqlvi2");
		vl = spark
				.sql("select sqlvi.Year, sqlvi.Country_Name from sqlvi, sqlvi2 where sqlvi.Year==sqlvi2.Year AND sqlvi.percentage_increase==sqlvi2.value ORDER BY sqlvi.YEAR ASC");
		vl.show(56);
	}

}
