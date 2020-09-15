from pyspark.sql import SparkSession


class hudi:
    def __init__(self):
        self.data_path = "C:\\Users\\Jihen.Bouguerra\\hadoop\\hudi-table"
        self.spark = SparkSession.builder.appName('Hudi_crud').getOrCreate()
        self.sc = self.spark.sparkContext
        table_name = "test"
        data = self.spark.range(0, 5)
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.operation': 'insert'
        }
        data.write.format("hudi"). \
        options(**hudi_options). \
        mode("overwrite").save(self.data_path)



if __name__ == '__main__':
    delta_lake = hudi()
