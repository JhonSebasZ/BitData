# Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import datediff, year, col, to_date
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Indexar y codificar la variable "Genero"
gender_indexer = StringIndexer(inputCol="Genero", outputCol="GeneroIndex")
df = gender_indexer.fit(df).transform(df)

gender_encoder = OneHotEncoder(inputCols=["GeneroIndex"], outputCols=["GeneroVec"])
df = gender_encoder.fit(df).transform(df)

# Imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadisticas básicas
df.summary().show()

# Limpeza de datos
# Eliminar valores nulos
df = df.na.drop()

# Quitar valores duplicados
df = df.dropDuplicates()

# Transformar datos
# Convertir columna fecha a tipo fecha
df = df.withColumn("FechaInscripcionBeneficiario", to_date(col("FechaInscripcionBeneficiario"), 'yyyy-MM-dd'))
df = df.withColumn("FechaUltimoBeneficioAsignado", to_date(col("FechaUltimoBeneficioAsignado"), 'yyyy-MM-dd'))

# Análisis exploratorio de datos
# Distribución por etnia
print("Beneficiarios por etnia\n")
ethnicity_distribution = df.groupBy("Etnia").count().orderBy("count", ascending=False)
ethnicity_distribution.show()

# Distribución por estado
print("Beneficiarios por estado\n")
status_count = df.groupBy("EstadoBeneficiario").count()
status_count.show()

# Distribución por genero
print("Beneficiarios por genero\n")
gender_distribution = df.groupBy("Genero").count()
gender_distribution.show()

# Distribución por edades
print("Beneficiarios por edades\n")
age_distribution = df.groupBy("RangoEdad").count()
age_distribution.show()

# Contar beneficiarios por departamento
print("Beneficiarios por departamento\n")
department_distribution = df.groupBy("NombreDepartamentoAtencion").count().orderBy("count", ascending=False)
department_distribution.show()

print("Beneficiarios por departamento y tipo de beneficio\n")
benefits_by_department = df.groupBy("NombreDepartamentoAtencion", "TipoBeneficio").count().orderBy("count", ascending=False)
benefits_by_department.show()

# Añadir columna de año de inscripción
df_year = df.withColumn("AñoInscripcion", year(col("FechaInscripcionBeneficiario")))

# Contar inscripciones por año
print("Inscripciones por año\n")
yearly_inscriptions = df_year.groupBy("AñoInscripcion").count().orderBy("AñoInscripcion")
yearly_inscriptions.show()