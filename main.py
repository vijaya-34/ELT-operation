from pyspark.sql import SparkSession

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .getOrCreate()
data_file = 'supermarket_sales - Sheet1.csv'

#EXTRACT
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
gender = sdfData.groupBy('Gender').count()
print(gender.show())

#LOAD
sdfData.registerTempTable('sales')
output = scSpark.sql('SELECT * from sales')
output.show()

output = scSpark.sql('SELECT * from sales WHERE `Unit Price` < 15 AND Quantity < 10')
output.show()

output = scSpark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
output.show()

#TRANSFORM
output.coalesce(1).write.format('json').save('filtered.json')