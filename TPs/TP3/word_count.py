import pyspark
try :
    sc.close()
except:
    None
sc = pyspark.SparkContext()

txt = sc.textFile('README.md')
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print(python_lines.count())
