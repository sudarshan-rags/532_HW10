from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrameWriter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd

conf = SparkConf().setAppName("HW 10")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


schema_student = StructType([
		StructField("ID", IntegerType(), True),
		StructField("NAME", StringType(), True)
		])

schema_attend = StructType([
		StructField("ID", IntegerType(), True),
		StructField("COURSE_ID", IntegerType(), True)
		])

schema_course = StructType([
		StructField("COURSE_ID", IntegerType(), True),
		StructField("COURSE_NAME", StringType(), True)
		])


df_student_values = {'ID' :  [1, 2, 3, 4, 5],
		     'NAME' : ['Paul', 'Ryan', 'Benedict', 'Watson', 'Holmes']
		     }
df_student = pd.DataFrame(df_student_values, columns = ['ID', 'NAME'])

df_attend_values = {'ID' : [1 , 2, 3, 4, 5],
		     'COURSE_ID' : [1, 2, 1, 2, 3]
		    }
df_attend = pd.DataFrame(df_attend_values, columns = ['ID', 'COURSE_ID'])

df_course_values = {'COURSE_ID' : [1, 2 ,3],
	 	    'COURSE_NAME' : ['Physics', 'Chemistry', 'Mathematics']
	 	    }
df_course = pd.DataFrame(df_course_values, columns = ['COURSE_ID', 'COURSE_NAME'])

df_student = sqlContext.createDataFrame(df_student, schema_student)
df_student.registerTempTable("Student")

df_attend = sqlContext.createDataFrame(df_attend, schema_attend)
df_attend.registerTempTable("Attend")

df_course = sqlContext.createDataFrame(df_course, schema_course)
df_course.registerTempTable("Course")

sqlContext.sql("select * from Student").show()
sqlContext.sql("select * from Attend").show()
sqlContext.sql("select * from Course").show()
sqlContext.sql("SELECT Student.Name \
		FROM ((Student \
		INNER JOIN Attend ON Student.ID = Attend.ID) \
		INNER JOIN Course ON Course.COURSE_ID = Attend.COURSE_ID) WHERE Course.COURSE_NAME = 'Physics';").explain(extended=True)



		

         
