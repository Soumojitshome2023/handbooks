# ⚡ PySpark Ultimate Command Handbook & Reference

A standalone, production-grade **PySpark Reference Handbook** engineered for rapid, daily lookup during data engineering, analytics, and ETL development. 

Every command is documented with **verified syntax**, **simple explanations**, **copyable code snippets**, and **exact DataFrame output tables** mapped to standardized sample datasets.

---

## 📖 Table of Contents

1. [Primary Reference Datasets](#-primary-reference-datasets)
2. [Module 1: Data Ingestion & Storage Utilities](#module-1-data-ingestion--storage-utilities)
3. [Module 2: Schema Definition & Management](#module-2-schema-definition--management)
4. [Module 3: Column Selection & Aliasing](#module-3-column-selection--aliasing)
5. [Module 4: Filtering & Row Selection](#module-4-filtering--row-selection)
6. [Module 5: Adding & Modifying Columns (withColumn)](#module-5-adding--modifying-columns-withcolumn)
7. [Module 6: Sorting & Ordering Data](#module-6-sorting--ordering-data)
8. [Module 7: Limiting, Dropping & Deduplication](#module-7-limiting-dropping--deduplication)
9. [Module 8: Set Operations (Union & Union By Name)](#module-8-set-operations-union--union-by-name)
10. [Module 9: String Manipulation Functions](#module-9-string-manipulation-functions)
11. [Module 10: Date & Timestamp Functions](#module-10-date--timestamp-functions)
12. [Module 11: Handling Missing & Null Data](#module-11-handling-missing--null-data)
13. [Module 12: Arrays, Splitting & Exploding](#module-12-arrays-splitting--exploding)
14. [Module 13: GroupBy & Aggregations](#module-13-groupby--aggregations)
15. [Module 14: Advanced Aggregations (Collect List & Pivot)](#module-14-advanced-aggregations-collect-list--pivot)
16. [Module 15: Conditional Logic (when - otherwise)](#module-15-conditional-logic-when---otherwise)
17. [Module 16: Joining DataFrames](#module-16-joining-dataframes)
18. [Module 17: Window Analytical Functions](#module-17-window-analytical-functions)
19. [Module 18: User Defined Functions (UDF)](#module-18-user-defined-functions-udf)
20. [Module 19: Data Writing & Persistence](#module-19-data-writing--persistence)
21. [Module 20: Spark SQL & Temporary Views](#module-20-spark-sql--temporary-views)

---

## 📊 Primary Reference Datasets

All transformations, filters, aggregations, and window functions throughout this handbook operate on the following standardized sample dataframes. Refer back to these tables to trace inputs and outputs.

### Primary Dataset: `df` (BigMart Sales Sample — 10 Rows × 12 Columns)
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Visibility | Item_Type             | Item_MRP | Outlet_Identifier | Outlet_Establishment_Year | Outlet_Size | Outlet_Location_Type | Outlet_Type       | Item_Outlet_Sales |
|-----------------|-------------|------------------|-----------------|-----------------------|----------|-------------------|---------------------------|-------------|----------------------|-------------------|-------------------|
| FDA15           | 9.30        | Low Fat          | 0.016           | Dairy                 | 249.8    | OUT049            | 1999                      | Medium      | Tier 1               | Supermarket Type1 | 3735.13           |
| DRC01           | 5.92        | Regular          | 0.0192          | Soft Drinks           | 48.26    | OUT018            | 2009                      | Medium      | Tier 3               | Supermarket Type2 | 443.42            |
| FDN15           | 17.50       | Low Fat          | 0.0167          | Meat                  | 141.61   | OUT049            | 1999                      | Medium      | Tier 1               | Supermarket Type1 | 2097.01           |
| FDX07           | 19.20       | Regular          | 0.0             | Fruits and Vegetables | 182.09   | OUT010            | 1998                      | NULL        | Tier 3               | Grocery Store     | 732.37            |
| NCD19           | 8.93        | Low Fat          | 0.0269          | Household             | 53.86    | OUT013            | 1987                      | High        | Tier 3               | Supermarket Type1 | 994.7             |
| FDP10           | 16.20       | Low Fat          | 0.0574          | Snack Foods           | 104.99   | OUT027            | 1985                      | Medium      | Tier 3               | Supermarket Type3 | 3182.84           |
| FDH17           | 16.20       | Regular          | 0.0166          | Meat                  | 96.44    | OUT017            | 2007                      | NULL        | Tier 2               | Supermarket Type2 | 1205.09           |
| FDU28           | 19.20       | Regular          | 0.0944          | Frozen Foods          | 190.05   | OUT045            | 2002                      | NULL        | Tier 2               | Supermarket Type1 | 1845.6            |
| DRJ24           | 2.73        | Low Fat          | 0.0142          | Soft Drinks           | 39.11    | OUT049            | 1999                      | Medium      | Tier 1               | Supermarket Type1 | 398.81            |
| FDS46           | 17.60       | Regular          | 0.0472          | Snack Foods           | 119.67   | OUT046            | 1997                      | Small       | Tier 1               | Supermarket Type1 | 2145.41           |

### JSON Drivers Dataset: `df_json` (Formula 1 Drivers)
| driverId | code | forename | surname    | nationality |
|----------|------|----------|------------|-------------|
| 1        | HAM  | Lewis    | Hamilton   | British     |
| 2        | BOT  | Valtteri | Bottas     | Finnish     |
| 3        | VER  | Max      | Verstappen | Dutch       |
| 4        | LEC  | Charles  | Leclerc    | Monegasque  |

### Join Reference Datasets: `df1` (Employees) & `df2` (Departments)

**`df1` (Employees):**
| emp_id | emp_name | dept_id |
|--------|----------|---------|
| 1      | gaur     | d01     |
| 2      | kit      | d02     |
| 3      | sam      | d03     |
| 4      | tim      | d03     |
| 5      | aman     | d05     |
| 6      | nad      | d06     |

**`df2` (Departments):**
| dept_id | department |
|---------|------------|
| d01     | HR         |
| d02     | Marketing  |
| d03     | Accounts   |
| d04     | IT         |
| d05     | Finance    |

### User Library Dataset: `df_book` (for `collect_list`)
| user  | book  |
|-------|-------|
| user1 | book1 |
| user1 | book2 |
| user2 | book2 |
| user2 | book4 |
| user3 | book1 |

---

## Module 1: Data Ingestion & Storage Utilities

### 1.1 Ingesting JSON Files
`spark.read.format('json').load(path)`

#### 📐 Syntax
```python
DataFrameReader.format("json") \
    .option("inferSchema", bool) \
    .option("header", bool) \
    .option("multiLine", bool) \
    .load(path)
```

#### 📖 Explanation
Reads semi-structured JSON records from distributed storage into a strongly-typed PySpark DataFrame.
- `inferSchema=True`: Automatically scans records to infer column types.
- `multiLine=False`: Parses standard line-delimited JSON Lines (one JSON object per line).
- `path`: Storage location URI (DBFS, S3, ADLS, or local path).

> 💡 **PRO TIP:** For massive JSON files in production pipelines, supply an explicit `StructType` schema instead of `inferSchema=True` to avoid double file scanning.

#### 💻 Code Example
```python
# Ingest JSON dataset with schema inference
df_json = spark.read.format('json') \
    .option('inferSchema', True) \
    .option('header', True) \
    .option('multiLine', False) \
    .load('/FileStore/tables/drivers.json')

df_json.display()
```

#### 📊 DataFrame Output
| driverId | code | forename | surname    | nationality |
|----------|------|----------|------------|-------------|
| 1        | HAM  | Lewis    | Hamilton   | British     |
| 2        | BOT  | Valtteri | Bottas     | Finnish     |
| 3        | VER  | Max      | Verstappen | Dutch       |
| 4        | LEC  | Charles  | Leclerc    | Monegasque  |

---

### 1.2 Databricks File System Utilities (dbutils.fs)
`dbutils.fs.ls(path) / cp / mv / rm / mkdirs`

#### 📐 Syntax
```python
dbutils.fs.ls(dir_path)
dbutils.fs.cp(from_path, to_path, recurse=False)
dbutils.fs.mv(from_path, to_path, recurse=False)
dbutils.fs.rm(path, recurse=False)
dbutils.fs.mkdirs(dir_path)
```

#### 📖 Explanation
Built-in file system utility inside Databricks notebooks to inspect, copy, move, and manage files on DBFS or cloud storage mounts.
- `dbutils.fs.ls(path)`: Lists directory contents, returning path, file name, size in bytes, and modification timestamp.

#### 💻 Code Example
```python
# List files inside Databricks FileStore tables directory
files_list = dbutils.fs.ls('/FileStore/tables/')
display(files_list)
```

#### 📊 DataFrame Output
| path                                     | name              | size   | modificationTime |
|------------------------------------------|-------------------|--------|------------------|
| dbfs:/FileStore/tables/BigMart_Sales.csv | BigMart_Sales.csv | 860341 | 1693401200000    |
| dbfs:/FileStore/tables/drivers.json      | drivers.json      | 143820 | 1693401250000    |
| dbfs:/FileStore/tables/CSV/              | CSV/              | 0      | 1693401300000    |

---

### 1.3 Ingesting CSV Files with Options
`spark.read.format('csv').option(...).load(path)`

#### 📐 Syntax
```python
spark.read.format("csv") \
    .option("header", bool) \
    .option("inferSchema", bool) \
    .option("delimiter", ",") \
    .load(path)
```

#### 📖 Explanation
Loads delimited text and CSV data into a PySpark DataFrame with granular configuration options.
- `header=True`: Treats the first line as column header names.
- `inferSchema=True`: Automatically detects column data types.
- `delimiter=','`: Custom character delimiter (e.g. `\t`, `|`, `;`).

> 💡 **PRO TIP:** Always define explicit schemas in production ETL jobs to ensure strict data validation and deterministic performance.

#### 💻 Code Example
```python
# Load CSV file with headers and schema inference
df = spark.read.format('csv') \
    .option('inferSchema', True) \
    .option('header', True) \
    .load('/FileStore/tables/BigMart_Sales.csv')

df.display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Visibility | Item_Type             | Item_MRP |
|-----------------|-------------|------------------|-----------------|-----------------------|----------|
| FDA15           | 9.30        | Low Fat          | 0.016           | Dairy                 | 249.8    |
| DRC01           | 5.92        | Regular          | 0.0192          | Soft Drinks           | 48.26    |
| FDN15           | 17.50       | Low Fat          | 0.0167          | Meat                  | 141.61   |
| FDX07           | 19.20       | Regular          | 0.0             | Fruits and Vegetables | 182.09   |
| NCD19           | 8.93        | Low Fat          | 0.0269          | Household             | 53.86    |

---

### 1.4 Displaying DataFrames (display vs show)
`df.display() / df.show(n=20, truncate=True)`

#### 📐 Syntax
```python
# Databricks Rich UI Viewer:
df.display()

# Standard PySpark ASCII Console Output:
df.show(n=20, truncate=True, vertical=False)
```

#### 📖 Explanation
Renders DataFrame content for inspection:
- `df.display()`: Interactive Databricks UI component offering built-in sorting, filtering, charting, and pagination.
- `df.show(n)`: Standard PySpark ASCII table printout to standard output.

#### 💻 Code Example
```python
# Rich interactive table viewer
df.display()

# Standard terminal console output
df.show(5, truncate=False)
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Visibility | Item_Type             |
|-----------------|-------------|------------------|-----------------|-----------------------|
| FDA15           | 9.30        | Low Fat          | 0.016           | Dairy                 |
| DRC01           | 5.92        | Regular          | 0.0192          | Soft Drinks           |
| FDN15           | 17.50       | Low Fat          | 0.0167          | Meat                  |
| FDX07           | 19.20       | Regular          | 0.0             | Fruits and Vegetables |

---

## Module 2: Schema Definition & Management

### 2.1 Inspecting Schema Structure
`df.printSchema() / df.dtypes / df.schema`

#### 📐 Syntax
```python
df.printSchema()
df.dtypes  # list of (column_name, data_type) tuples
df.schema  # StructType tree object
```

#### 📖 Explanation
Displays the DataFrame's hierarchical schema in a clean tree structure, detailing column names, data types, and nullability flags.

#### 💻 Code Example
```python
# Print schema tree structure to console
df.printSchema()
```

#### 📊 DataFrame Output
| Column Name               | Data Type               | Nullable | Description                      |
|---------------------------|-------------------------|----------|----------------------------------|
| Item_Identifier           | StringType              | True     | Unique Product Code (e.g. FDA15) |
| Item_Weight               | StringType / DoubleType | True     | Weight of the product in kg      |
| Item_Fat_Content          | StringType              | True     | Fat category (Low Fat, Regular)  |
| Item_Visibility           | DoubleType              | True     | Store display allocation ratio   |
| Item_Type                 | StringType              | True     | Product category name            |
| Item_MRP                  | DoubleType              | True     | Maximum Retail Price in INR      |
| Outlet_Identifier         | StringType              | True     | Unique Store ID (e.g. OUT049)    |
| Outlet_Establishment_Year | IntegerType             | True     | Store founding year              |
| Outlet_Size               | StringType              | True     | Store size (Small, Medium, High) |
| Outlet_Location_Type      | StringType              | True     | City classification tier         |
| Outlet_Type               | StringType              | True     | Store model format               |
| Item_Outlet_Sales         | DoubleType              | True     | Total outlet product revenue     |

---

### 2.2 DDL Schema String Definition
`spark.read.schema(ddl_schema_string)`

#### 📐 Syntax
```python
ddl_schema = "col1_name DATA_TYPE, col2_name DATA_TYPE, ..."
spark.read.schema(ddl_schema).load(path)
```

#### 📖 Explanation
Defines a strict schema using SQL Data Definition Language (DDL) string syntax. Highly human-readable and avoids schema inference overhead.

> 💡 **PRO TIP:** DDL schemas are much more compact than StructType syntax while supporting all SQL primitive types (STRING, INT, BIGINT, DOUBLE, TIMESTAMP, DATE, BOOLEAN).

#### 💻 Code Example
```python
# Define schema using SQL DDL string format
my_ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight STRING,
    Item_Fat_Content STRING, 
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size STRING,
    Outlet_Location_Type STRING, 
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
'''

# Load CSV enforcing the DDL schema
df = spark.read.format('csv') \
    .schema(my_ddl_schema) \
    .option('header', True) \
    .load('/FileStore/tables/BigMart_Sales.csv')

df.display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Visibility | Item_Type             | Item_MRP |
|-----------------|-------------|------------------|-----------------|-----------------------|----------|
| FDA15           | 9.30        | Low Fat          | 0.016           | Dairy                 | 249.8    |
| DRC01           | 5.92        | Regular          | 0.0192          | Soft Drinks           | 48.26    |
| FDN15           | 17.50       | Low Fat          | 0.0167          | Meat                  | 141.61   |
| FDX07           | 19.20       | Regular          | 0.0             | Fruits and Vegetables | 182.09   |

---

### 2.3 Programmatic StructType Schema
`StructType([StructField(name, dataType, nullable), ...])`

#### 📐 Syntax
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("column_name", DataType(), nullable=True),
    ...
])
```

#### 📖 Explanation
The programmatic object model for schemas. Essential when building complex nested schemas containing structs, maps, and arrays dynamically.

#### 💻 Code Example
```python
from pyspark.sql.types import *

# Define programmatic StructType schema
my_strct_schema = StructType([
    StructField('Item_Identifier', StringType(), True),
    StructField('Item_Weight', StringType(), True),
    StructField('Item_Fat_Content', StringType(), True),
    StructField('Item_Visibility', StringType(), True),
    StructField('Item_MRP', StringType(), True),
    StructField('Outlet_Identifier', StringType(), True),
    StructField('Outlet_Establishment_Year', StringType(), True),
    StructField('Outlet_Size', StringType(), True),
    StructField('Outlet_Location_Type', StringType(), True),
    StructField('Outlet_Type', StringType(), True),
    StructField('Item_Outlet_Sales', StringType(), True)
])

df = spark.read.format('csv') \
    .schema(my_strct_schema) \
    .option('header', True) \
    .load('/FileStore/tables/BigMart_Sales.csv')

df.printSchema()
```

#### 📊 DataFrame Output
| Field Name        | Type Object  | Nullable Flag |
|-------------------|--------------|---------------|
| Item_Identifier   | StringType() | True          |
| Item_Weight       | StringType() | True          |
| Item_Fat_Content  | StringType() | True          |
| Item_Visibility   | StringType() | True          |
| Item_MRP          | StringType() | True          |
| Outlet_Identifier | StringType() | True          |

---

## Module 3: Column Selection & Aliasing

### 3.1 Selecting Specific Columns
`df.select(col('col1'), col('col2'), ...)`

#### 📐 Syntax
```python
df.select(*cols)
df.select(col("col1"), col("col2"))
df.select("col1", "col2")
```

#### 📖 Explanation
Projects a subset of columns from a DataFrame, discarding unneeded columns. Using `col('name')` enables functional column transformations and expressions inside `select()`.

> 💡 **PRO TIP:** Select only required columns early in your pipeline (projection pushdown) to optimize Spark memory and execution speed.

#### 💻 Code Example
```python
from pyspark.sql.functions import col

# Select 3 specific columns
df.select(
    col('Item_Identifier'),
    col('Item_Weight'),
    col('Item_Fat_Content')
).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content |
|-----------------|-------------|------------------|
| FDA15           | 9.30        | Low Fat          |
| DRC01           | 5.92        | Regular          |
| FDN15           | 17.50       | Low Fat          |
| FDX07           | 19.20       | Regular          |
| NCD19           | 8.93        | Low Fat          |
| FDP10           | 16.20       | Low Fat          |

---

### 3.2 Renaming in Select with alias()
`col('col_name').alias('new_col_name')`

#### 📐 Syntax
```python
df.select(col("existing_column").alias("new_column_name"))
```

#### 📖 Explanation
Assigns a new alias/name to a column expression on-the-fly during a `select()` projection, identical to SQL `SELECT col AS new_name`.

#### 💻 Code Example
```python
# Select and rename column in a single transformation
df.select(
    col('Item_Identifier').alias('Item_ID')
).display()
```

#### 📊 DataFrame Output
| Item_ID |
|---------|
| FDA15   |
| DRC01   |
| FDN15   |
| FDX07   |
| NCD19   |
| FDP10   |

---

### 3.3 Renaming Column with withColumnRenamed()
`df.withColumnRenamed(existingName, newName)`

#### 📐 Syntax
```python
df.withColumnRenamed(existingName: str, newName: str)
```

#### 📖 Explanation
Returns a new DataFrame by renaming an existing column without modifying or dropping any other columns.

#### 💻 Code Example
```python
# Rename Item_Weight to Item_Wt while keeping all other columns intact
df.withColumnRenamed('Item_Weight', 'Item_Wt').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Wt | Item_Fat_Content | Item_Visibility | Item_Type             | Item_MRP |
|-----------------|---------|------------------|-----------------|-----------------------|----------|
| FDA15           | 9.30    | Low Fat          | 0.016           | Dairy                 | 249.8    |
| DRC01           | 5.92    | Regular          | 0.0192          | Soft Drinks           | 48.26    |
| FDN15           | 17.50   | Low Fat          | 0.0167          | Meat                  | 141.61   |
| FDX07           | 19.20   | Regular          | 0.0             | Fruits and Vegetables | 182.09   |
| NCD19           | 8.93    | Low Fat          | 0.0269          | Household             | 53.86    |

---

## Module 4: Filtering & Row Selection

### 4.1 Filter Scenario 1: Single Equality Condition
`df.filter(col('col') == value) / df.where(...)`

#### 📐 Syntax
```python
df.filter(condition)
df.where(condition)  # Alias for filter()
```

#### 📖 Explanation
Filters rows matching a boolean condition. In PySpark, `filter()` and `where()` are exact synonyms.

#### 💻 Code Example
```python
# Filter rows where Item_Fat_Content equals 'Regular'
df.filter(col('Item_Fat_Content') == 'Regular').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type             | Item_MRP | Outlet_Location_Type |
|-----------------|-------------|------------------|-----------------------|----------|----------------------|
| DRC01           | 5.92        | Regular          | Soft Drinks           | 48.26    | Tier 3               |
| FDX07           | 19.20       | Regular          | Fruits and Vegetables | 182.09   | Tier 3               |
| FDH17           | 16.20       | Regular          | Meat                  | 96.44    | Tier 2               |
| FDU28           | 19.20       | Regular          | Frozen Foods          | 190.05   | Tier 2               |
| FDS46           | 17.60       | Regular          | Snack Foods           | 119.67   | Tier 1               |

---

### 4.2 Filter Scenario 2: Multiple Compound Conditions
`df.filter((condition1) & (condition2))`

#### 📐 Syntax
```python
# AND: (cond1) & (cond2)
# OR:  (cond1) | (cond2)
# NOT: ~(cond1)
```

#### 📖 Explanation
Combines multiple filtering predicates using bitwise logical operators (`&`, `|`, `~`).

> ⚠️ **WARNING:** In PySpark, you MUST wrap each individual condition inside parentheses `(cond1) & (cond2)` due to Python operator precedence rules. Do not use Python's `and` / `or` keywords!

#### 💻 Code Example
```python
# Filter rows where Item_Type is 'Soft Drinks' AND Item_Weight is less than 10
df.filter(
    (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10)
).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type   | Item_MRP | Outlet_Identifier |
|-----------------|-------------|------------------|-------------|----------|-------------------|
| DRC01           | 5.92        | Regular          | Soft Drinks | 48.26    | OUT018            |
| DRJ24           | 2.73        | Low Fat          | Soft Drinks | 39.11    | OUT049            |

---

### 4.3 Filter Scenario 3: Null Checking & isin() Membership
`col('col').isNull() & col('col').isin('val1', 'val2')`

#### 📐 Syntax
```python
col("col").isNull()
col("col").isNotNull()
col("col").isin(*values_list)
```

#### 📖 Explanation
Tests for missing null values (`isNull()`) and verifies if a column value matches any element in an allowed list of values (`isin()`).

#### 💻 Code Example
```python
# Find records where Outlet_Size is NULL and Location is in Tier 1 or Tier 2
df.filter(
    (col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))
).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Type    | Outlet_Identifier | Outlet_Size | Outlet_Location_Type |
|-----------------|-------------|--------------|-------------------|-------------|----------------------|
| FDH17           | 16.20       | Meat         | OUT017            | NULL        | Tier 2               |
| FDU28           | 19.20       | Frozen Foods | OUT045            | NULL        | Tier 2               |

---

## Module 5: Adding & Modifying Columns (withColumn)

### 5.1 Scenario 1A: Adding Constant Literal Column
`df.withColumn('col_name', lit(constant_value))`

#### 📐 Syntax
```python
from pyspark.sql.functions import lit
df.withColumn(colName: str, col: Column)
```

#### 📖 Explanation
Adds a new column containing a static literal value across all rows using the `lit()` function.

> 💡 **PRO TIP:** You cannot pass raw Python primitives directly as the second argument; it must always be wrapped in `lit()` or be a Column expression.

#### 💻 Code Example
```python
from pyspark.sql.functions import lit

# Add a static flag column with string value 'new'
df = df.withColumn('flag', lit("new"))
df.display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type             | Item_MRP | Outlet_Identifier | flag |
|-----------------|-----------------------|----------|-------------------|------|
| FDA15           | Dairy                 | 249.8    | OUT049            | new  |
| DRC01           | Soft Drinks           | 48.26    | OUT018            | new  |
| FDN15           | Meat                  | 141.61   | OUT049            | new  |
| FDX07           | Fruits and Vegetables | 182.09   | OUT010            | new  |
| NCD19           | Household             | 53.86    | OUT013            | new  |

---

### 5.2 Scenario 1B: Arithmetic & Mathematical Calculations
`df.withColumn('new_col', col('c1') * col('c2'))`

#### 📐 Syntax
```python
df.withColumn("calculated_col", col("col1") * col("col2"))
```

#### 📖 Explanation
Calculates a new derived metric by applying mathematical expressions across multiple columns row-by-row.

#### 💻 Code Example
```python
# Compute multiplied product of Item_Weight and Item_MRP
df.withColumn('multiply', col('Item_Weight') * col('Item_MRP')).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_MRP | multiply |
|-----------------|-------------|----------|----------|
| FDA15           | 9.30        | 249.8    | 2323.14  |
| DRC01           | 5.92        | 48.26    | 285.699  |
| FDN15           | 17.50       | 141.61   | 2478.175 |
| FDX07           | 19.20       | 182.09   | 3496.128 |
| NCD19           | 8.93        | 53.86    | 480.97   |

---

### 5.3 Scenario 2: String Regex Replacement
`df.withColumn('col', regexp_replace(col('col'), pattern, replacement))`

#### 📐 Syntax
```python
from pyspark.sql.functions import regexp_replace
df.withColumn("col_name", regexp_replace(col("col_name"), "pattern", "replacement"))
```

#### 📖 Explanation
Replaces string substrings or regular expression patterns within a column. Chaining multiple `withColumn()` calls standardizes categorical text values.

#### 💻 Code Example
```python
from pyspark.sql.functions import regexp_replace

# Standardize fat content labels: 'Regular' -> 'Reg', 'Low Fat' -> 'Lf'
df = df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Regular", "Reg")) \
       .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Low Fat", "Lf"))

df.display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type             | Item_MRP |
|-----------------|-------------|------------------|-----------------------|----------|
| FDA15           | 9.30        | Lf               | Dairy                 | 249.8    |
| DRC01           | 5.92        | Reg              | Soft Drinks           | 48.26    |
| FDN15           | 17.50       | Lf               | Meat                  | 141.61   |
| FDX07           | 19.20       | Reg              | Fruits and Vegetables | 182.09   |
| NCD19           | 8.93        | Lf               | Household             | 53.86    |

---

### 5.4 Type Casting Column Data Types
`col('col_name').cast(TargetDataType)`

#### 📐 Syntax
```python
# PySpark Type Classes:
col("col_name").cast(StringType())
col("col_name").cast(IntegerType())
col("col_name").cast(DoubleType())

# SQL String Keywords:
col("col_name").cast("string")
col("col_name").cast("int")
col("col_name").cast("double")
```

#### 📖 Explanation
Converts a column from one data type to another (e.g. string to double, integer to string, string to timestamp). If a value cannot be converted, Spark safely returns `null`.

#### 💻 Code Example
```python
from pyspark.sql.types import StringType

# Cast Item_Weight column from numeric to StringType
df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

df.printSchema()
```

#### 📊 DataFrame Output
| Column Name     | Casted Data Type    | Sample Value |
|-----------------|---------------------|--------------|
| Item_Identifier | StringType          | FDA15        |
| Item_Weight     | StringType (Casted) | '9.30'       |
| Item_MRP        | DoubleType          | 249.80       |

---

## Module 6: Sorting & Ordering Data

### 6.1 Scenario 1: Single Column Descending
`df.sort(col('col').desc()) / df.orderBy(...)`

#### 📐 Syntax
```python
df.sort(col("col_name").desc())
df.orderBy(col("col_name").desc())
```

#### 📖 Explanation
Orders DataFrame rows in descending order (highest to lowest). `sort()` and `orderBy()` behave identically.

#### 💻 Code Example
```python
# Sort records by Item_Weight descending
df.sort(col('Item_Weight').desc()).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type             | Item_MRP |
|-----------------|-------------|------------------|-----------------------|----------|
| FDX07           | 19.20       | Regular          | Fruits and Vegetables | 182.09   |
| FDU28           | 19.20       | Regular          | Frozen Foods          | 190.05   |
| FDS46           | 17.60       | Regular          | Snack Foods           | 119.67   |
| FDN15           | 17.50       | Low Fat          | Meat                  | 141.61   |
| FDP10           | 16.20       | Low Fat          | Snack Foods           | 104.99   |
| FDH17           | 16.20       | Regular          | Meat                  | 96.44    |
| FDA15           | 9.30        | Low Fat          | Dairy                 | 249.8    |

---

### 6.2 Scenario 2: Single Column Ascending
`df.sort(col('col').asc())`

#### 📐 Syntax
```python
df.sort(col("col_name").asc())
```

#### 📖 Explanation
Orders DataFrame rows in ascending order (lowest to highest). Ascending is the default sort direction in PySpark.

#### 💻 Code Example
```python
# Sort records by Item_Visibility ascending
df.sort(col('Item_Visibility').asc()).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Visibility | Item_Type             | Item_MRP | Outlet_Identifier |
|-----------------|-----------------|-----------------------|----------|-------------------|
| FDX07           | 0.0             | Fruits and Vegetables | 182.09   | OUT010            |
| DRJ24           | 0.0142          | Soft Drinks           | 39.11    | OUT049            |
| FDA15           | 0.016           | Dairy                 | 249.8    | OUT049            |
| FDH17           | 0.0166          | Meat                  | 96.44    | OUT017            |
| FDN15           | 0.0167          | Meat                  | 141.61   | OUT049            |

---

### 6.3 Scenario 3: Multi-Column Sorting (All Descending)
`df.sort(['col1', 'col2'], ascending=[0, 0])`

#### 📐 Syntax
```python
df.sort(["col1", "col2"], ascending=[0, 0])
# 0 = False (Descending), 1 = True (Ascending)
```

#### 📖 Explanation
Sorts by primary and secondary keys simultaneously using a list of column names and a corresponding boolean list of direction flags (`0` for Descending, `1` for Ascending).

#### 💻 Code Example
```python
# Sort primarily by Item_Weight (DESC), secondarily by Item_Visibility (DESC)
df.sort(['Item_Weight', 'Item_Visibility'], ascending=[0, 0]).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Visibility | Item_Type             | Item_MRP |
|-----------------|-------------|-----------------|-----------------------|----------|
| FDU28           | 19.20       | 0.0944          | Frozen Foods          | 190.05   |
| FDX07           | 19.20       | 0.0             | Fruits and Vegetables | 182.09   |
| FDS46           | 17.60       | 0.0472          | Snack Foods           | 119.67   |
| FDN15           | 17.50       | 0.0167          | Meat                  | 141.61   |
| FDP10           | 16.20       | 0.0574          | Snack Foods           | 104.99   |
| FDH17           | 16.20       | 0.0166          | Meat                  | 96.44    |

---

### 6.4 Scenario 4: Mixed Multi-Column Sorting
`df.sort(['col1', 'col2'], ascending=[0, 1])`

#### 📐 Syntax
```python
df.sort(["col1", "col2"], ascending=[0, 1])
```

#### 📖 Explanation
Sorts primary column in descending order (`0`) and tie-breaking secondary column in ascending order (`1`).

#### 💻 Code Example
```python
# Sort Item_Weight descending (0), then Item_Visibility ascending (1)
df.sort(['Item_Weight', 'Item_Visibility'], ascending=[0, 1]).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Visibility | Item_Type             | Item_MRP |
|-----------------|-------------|-----------------|-----------------------|----------|
| FDX07           | 19.20       | 0.0             | Fruits and Vegetables | 182.09   |
| FDU28           | 19.20       | 0.0944          | Frozen Foods          | 190.05   |
| FDS46           | 17.60       | 0.0472          | Snack Foods           | 119.67   |
| FDN15           | 17.50       | 0.0167          | Meat                  | 141.61   |
| FDH17           | 16.20       | 0.0166          | Meat                  | 96.44    |
| FDP10           | 16.20       | 0.0574          | Snack Foods           | 104.99   |

---

## Module 7: Limiting, Dropping & Deduplication

### 7.1 Limiting Rows
`df.limit(n)`

#### 📐 Syntax
```python
df.limit(num_rows: int)
```

#### 📖 Explanation
Restricts the DataFrame result to the first `n` rows. Useful for quick sampling and interactive debugging.

#### 💻 Code Example
```python
# Fetch top 5 rows
df.limit(5).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type             | Item_MRP |
|-----------------|-------------|------------------|-----------------------|----------|
| FDA15           | 9.30        | Low Fat          | Dairy                 | 249.8    |
| DRC01           | 5.92        | Regular          | Soft Drinks           | 48.26    |
| FDN15           | 17.50       | Low Fat          | Meat                  | 141.61   |
| FDX07           | 19.20       | Regular          | Fruits and Vegetables | 182.09   |
| NCD19           | 8.93        | Low Fat          | Household             | 53.86    |

---

### 7.2 Dropping a Single Column
`df.drop('col_name')`

#### 📐 Syntax
```python
df.drop(colName: str)
df.drop(col("colName"))
```

#### 📖 Explanation
Removes a specific column from the DataFrame and returns a new DataFrame containing all remaining columns.

#### 💻 Code Example
```python
# Drop Item_Visibility column
df.drop('Item_Visibility').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type             | Item_MRP | Outlet_Identifier |
|-----------------|-------------|------------------|-----------------------|----------|-------------------|
| FDA15           | 9.30        | Low Fat          | Dairy                 | 249.8    | OUT049            |
| DRC01           | 5.92        | Regular          | Soft Drinks           | 48.26    | OUT018            |
| FDN15           | 17.50       | Low Fat          | Meat                  | 141.61   | OUT049            |
| FDX07           | 19.20       | Regular          | Fruits and Vegetables | 182.09   | OUT010            |
| NCD19           | 8.93        | Low Fat          | Household             | 53.86    | OUT013            |

---

### 7.3 Dropping Multiple Columns
`df.drop('col1', 'col2', ...)`

#### 📐 Syntax
```python
df.drop(*colNames: str)
```

#### 📖 Explanation
Drops multiple columns in a single invocation by passing multiple comma-separated column names or unpacking a list.

#### 💻 Code Example
```python
# Drop both Item_Visibility and Item_Type columns
df.drop('Item_Visibility', 'Item_Type').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_MRP | Outlet_Identifier | Outlet_Size |
|-----------------|-------------|------------------|----------|-------------------|-------------|
| FDA15           | 9.30        | Low Fat          | 249.8    | OUT049            | Medium      |
| DRC01           | 5.92        | Regular          | 48.26    | OUT018            | Medium      |
| FDN15           | 17.50       | Low Fat          | 141.61   | OUT049            | Medium      |
| FDX07           | 19.20       | Regular          | 182.09   | OUT010            | NULL        |
| NCD19           | 8.93        | Low Fat          | 53.86    | OUT013            | High        |

---

### 7.4 Dropping Duplicate Rows Across All Columns
`df.dropDuplicates() / df.distinct()`

#### 📐 Syntax
```python
df.dropDuplicates()
df.distinct()  # Exact equivalent when no subset is specified
```

#### 📖 Explanation
Removes fully duplicate rows where all column values are identical. `distinct()` and `dropDuplicates()` produce identical execution plans.

#### 💻 Code Example
```python
# Eliminate completely identical duplicate rows
df.dropDuplicates().display()

# Or using distinct()
df.distinct().display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type             | Item_MRP |
|-----------------|-------------|------------------|-----------------------|----------|
| FDA15           | 9.30        | Low Fat          | Dairy                 | 249.8    |
| DRC01           | 5.92        | Regular          | Soft Drinks           | 48.26    |
| FDN15           | 17.50       | Low Fat          | Meat                  | 141.61   |
| FDX07           | 19.20       | Regular          | Fruits and Vegetables | 182.09   |
| NCD19           | 8.93        | Low Fat          | Household             | 53.86    |
| FDP10           | 16.20       | Low Fat          | Snack Foods           | 104.99   |

---

### 7.5 Dropping Duplicates on Column Subset
`df.drop_duplicates(subset=['col1', 'col2'])`

#### 📐 Syntax
```python
df.dropDuplicates(subset=["col1", "col2"])
df.drop_duplicates(subset=["col1", "col2"])  # Pythonic alias
```

#### 📖 Explanation
Deduplicates data based only on specified key columns in `subset`, retaining the first encountered row for each unique key combination.

#### 💻 Code Example
```python
# Retain only one representative record per unique Item_Type
df.drop_duplicates(subset=['Item_Type']).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type             | Item_Fat_Content | Item_MRP |
|-----------------|-----------------------|------------------|----------|
| FDA15           | Dairy                 | Low Fat          | 249.8    |
| DRC01           | Soft Drinks           | Regular          | 48.26    |
| FDN15           | Meat                  | Low Fat          | 141.61   |
| FDX07           | Fruits and Vegetables | Regular          | 182.09   |
| NCD19           | Household             | Low Fat          | 53.86    |
| FDP10           | Snack Foods           | Low Fat          | 104.99   |
| FDU28           | Frozen Foods          | Regular          | 190.05   |

---

## Module 8: Set Operations (Union & Union By Name)

### 8.1 Positional Union
`df1.union(df2) / df1.unionAll(df2)`

#### 📐 Syntax
```python
df1.union(df2)
# Requires identical column count and matching data types at matching index positions
```

#### 📖 Explanation
Combines rows from two DataFrames into one by positional index matching (Column 1 with Column 1, Column 2 with Column 2), preserving duplicates.

> ⚠️ **WARNING:** Standard `union()` does NOT match columns by name. If `df1` has (name, id) and `df2` has (id, name), values will be swapped into the wrong columns!

#### 💻 Code Example
```python
# Create sample DataFrames
data1 = [('1', 'kad'), ('2', 'sid')]
df1 = spark.createDataFrame(data1, 'id STRING, name STRING')

data2 = [('3', 'rahul'), ('4', 'jas')]
df2 = spark.createDataFrame(data2, 'id STRING, name STRING')

# Positional Union
df_combined = df1.union(df2)
df_combined.display()
```

#### 📊 DataFrame Output
| id | name  |
|----|-------|
| 1  | kad   |
| 2  | sid   |
| 3  | rahul |
| 4  | jas   |

---

### 8.2 Schema-Aware Union by Name
`df1.unionByName(df2, allowMissingColumns=False)`

#### 📐 Syntax
```python
df1.unionByName(df2, allowMissingColumns=False)
```

#### 📖 Explanation
Safely resolves and aligns columns by matching column names rather than column positions. Even if columns appear in reverse order in `df1` vs `df2`, values are mapped into their correct corresponding columns.
- `allowMissingColumns=True`: Automatically injects `null` for columns present in one DataFrame but absent in the other.

> 💡 **PRO TIP:** Always prefer `unionByName()` over `union()` in production ETL pipelines to avoid silent schema misalignment bugs.

#### 💻 Code Example
```python
# df1 has columns in reverse order: (name, id)
data1 = [('kad', '1'), ('sid', '2')]
df1 = spark.createDataFrame(data1, 'name STRING, id STRING')

# df2 has columns: (id, name)
data2 = [('3', 'rahul'), ('4', 'jas')]
df2 = spark.createDataFrame(data2, 'id STRING, name STRING')

# Safe Union by Column Name
df_safe = df1.unionByName(df2)
df_safe.display()
```

#### 📊 DataFrame Output
| name  | id |
|-------|----|
| kad   | 1  |
| sid   | 2  |
| rahul | 3  |
| jas   | 4  |

---

## Module 9: String Manipulation Functions

### 9.1 String Case Formatting (upper, lower, initcap)
`upper(col) / lower(col) / initcap(col)`

#### 📐 Syntax
```python
from pyspark.sql.functions import upper, lower, initcap
upper(col("col_name"))
lower(col("col_name"))
initcap(col("col_name"))  # Capitalizes first letter of each word
```

#### 📖 Explanation
Transforms string casing across columns:
- `upper()`: Converts string to ALL UPPERCASE.
- `lower()`: Converts string to all lowercase.
- `initcap()`: Capitalizes the initial letter of every word (Title Case).

#### 💻 Code Example
```python
from pyspark.sql.functions import upper, lower, initcap

# Transform Item_Type to uppercase
df.select(
    col('Item_Type'),
    upper('Item_Type').alias('upper_Item_Type'),
    lower('Item_Type').alias('lower_Item_Type'),
    initcap('Item_Type').alias('initcap_Item_Type')
).display()
```

#### 📊 DataFrame Output
| Item_Type             | upper_Item_Type       | lower_Item_Type       | initcap_Item_Type     |
|-----------------------|-----------------------|-----------------------|-----------------------|
| Dairy                 | DAIRY                 | dairy                 | Dairy                 |
| Soft Drinks           | SOFT DRINKS           | soft drinks           | Soft Drinks           |
| Meat                  | MEAT                  | meat                  | Meat                  |
| Fruits and Vegetables | FRUITS AND VEGETABLES | fruits and vegetables | Fruits And Vegetables |
| Frozen Foods          | FROZEN FOODS          | frozen foods          | Frozen Foods          |

---

### 9.2 Essential String Functions Reference
`concat / concat_ws / substring / trim / length`

#### 📐 Syntax
```python
from pyspark.sql.functions import concat, concat_ws, substring, trim, length
concat(col1, col2)
concat_ws(separator, col1, col2, ...)
substring(col, pos, len)  # 1-indexed
trim(col)
length(col)
```

#### 📖 Explanation
Core suite of PySpark string utilities for data cleaning, string concatenation, whitespace trimming, and slicing.

#### 💻 Code Example
```python
from pyspark.sql.functions import concat_ws, substring, length

# Construct composite identifier and extract code prefix
df.select(
    col('Item_Identifier'),
    concat_ws(' - ', col('Item_Identifier'), col('Item_Type')).alias('Full_Description'),
    substring(col('Item_Identifier'), 1, 2).alias('Category_Prefix'),
    length(col('Item_Type')).alias('Type_Char_Count')
).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Full_Description    | Category_Prefix | Type_Char_Count |
|-----------------|---------------------|-----------------|-----------------|
| FDA15           | FDA15 - Dairy       | FD              | 5               |
| DRC01           | DRC01 - Soft Drinks | DR              | 11              |
| FDN15           | FDN15 - Meat        | FD              | 4               |
| NCD19           | NCD19 - Household   | NC              | 9               |

---

## Module 10: Date & Timestamp Functions

### 10.1 Current Date & Timestamp
`current_date() / current_timestamp()`

#### 📐 Syntax
```python
from pyspark.sql.functions import current_date, current_timestamp
current_date()      # Returns DateType (YYYY-MM-DD)
current_timestamp() # Returns TimestampType (YYYY-MM-DD HH:mm:ss.SSSSSS)
```

#### 📖 Explanation
Generates the current execution system date or timestamp as a column. Useful for creating ingestion audit timestamps.

#### 💻 Code Example
```python
from pyspark.sql.functions import current_date

# Add execution batch date column
df = df.withColumn('curr_date', current_date())
df.select('Item_Identifier', 'curr_date').display()
```

#### 📊 DataFrame Output
| Item_Identifier | curr_date  |
|-----------------|------------|
| FDA15           | 2026-08-30 |
| DRC01           | 2026-08-30 |
| FDN15           | 2026-08-30 |
| FDX07           | 2026-08-30 |

---

### 10.2 Date Addition & Subtraction
`date_add(col, days) / date_sub(col, days)`

#### 📐 Syntax
```python
from pyspark.sql.functions import date_add, date_sub
date_add(col: Column/str, num_days: int)
date_sub(col: Column/str, num_days: int)
```

#### 📖 Explanation
Adds or subtracts a specified number of calendar days to a date column. Passing negative integers to `date_add(col, -7)` achieves the same effect as `date_sub(col, 7)`.

#### 💻 Code Example
```python
from pyspark.sql.functions import date_add, date_sub

# Add 7 days (next week) and subtract 7 days (previous week)
df = df.withColumn('week_after', date_add('curr_date', 7)) \
       .withColumn('week_before', date_add('curr_date', -7))

df.select('Item_Identifier', 'curr_date', 'week_after', 'week_before').display()
```

#### 📊 DataFrame Output
| Item_Identifier | curr_date  | week_after | week_before |
|-----------------|------------|------------|-------------|
| FDA15           | 2026-08-30 | 2026-09-06 | 2026-08-23  |
| DRC01           | 2026-08-30 | 2026-09-06 | 2026-08-23  |
| FDN15           | 2026-08-30 | 2026-09-06 | 2026-08-23  |

---

### 10.3 Calculating Days Between Dates (datediff)
`datediff(end_date_col, start_date_col)`

#### 📐 Syntax
```python
from pyspark.sql.functions import datediff
datediff(end: Column/str, start: Column/str)
```

#### 📖 Explanation
Computes the integer difference in days between two date columns: `(end_date - start_date)`.

#### 💻 Code Example
```python
from pyspark.sql.functions import datediff

# Compute number of elapsed days between week_after and curr_date
df = df.withColumn('datediff', datediff('week_after', 'curr_date'))

df.select('curr_date', 'week_after', 'datediff').display()
```

#### 📊 DataFrame Output
| curr_date  | week_after | datediff |
|------------|------------|----------|
| 2026-08-30 | 2026-09-06 | 7        |
| 2026-08-30 | 2026-09-06 | 7        |
| 2026-08-30 | 2026-09-06 | 7        |

---

### 10.4 Formatting Dates as Custom Strings
`date_format(col, formatPattern)`

#### 📐 Syntax
```python
from pyspark.sql.functions import date_format
date_format(date: Column/str, format: str)
```

#### 📖 Explanation
Formats a date/timestamp column into a customized string representation using Java SimpleDateFormat patterns (e.g. `dd-MM-yyyy`, `yyyy/MM/dd`).

#### 💻 Code Example
```python
from pyspark.sql.functions import date_format

# Format date column into 'dd-MM-yyyy' format
df = df.withColumn('week_before_formatted', date_format('week_before', 'dd-MM-yyyy'))

df.select('week_before', 'week_before_formatted').display()
```

#### 📊 DataFrame Output
| week_before (Raw Date) | week_before_formatted (String) |
|------------------------|--------------------------------|
| 2026-08-23             | 23-08-2026                     |
| 2026-08-23             | 23-08-2026                     |
| 2026-08-23             | 23-08-2026                     |

---

## Module 11: Handling Missing & Null Data

### 11.1 Dropping Rows with All Nulls
`df.dropna('all')`

#### 📐 Syntax
```python
df.dropna(how="all")
df.na.drop(how="all")
```

#### 📖 Explanation
Drops a row if and only if **ALL** columns in that row contain `null` values.

#### 💻 Code Example
```python
# Drop rows where every single column is NULL
df.dropna('all').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Type             | Outlet_Size |
|-----------------|-------------|-----------------------|-------------|
| FDA15           | 9.30        | Dairy                 | Medium      |
| DRC01           | 5.92        | Soft Drinks           | Medium      |
| FDN15           | 17.50       | Meat                  | Medium      |
| FDX07           | 19.20       | Fruits and Vegetables | NULL        |
| NCD19           | 8.93        | Household             | High        |
| FDP10           | 16.20       | Snack Foods           | Medium      |

---

### 11.2 Dropping Rows with Any Nulls
`df.dropna('any')`

#### 📐 Syntax
```python
df.dropna(how="any")
df.na.drop(how="any")
```

#### 📖 Explanation
Drops a row if **ANY** column in that row contains a `null` value.

#### 💻 Code Example
```python
# Drop any record that has at least one null column
df.dropna('any').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Outlet_Size | Outlet_Location_Type |
|-----------------|-------------|------------------|-------------|----------------------|
| FDA15           | 9.30        | Low Fat          | Medium      | Tier 1               |
| DRC01           | 5.92        | Regular          | Medium      | Tier 3               |
| FDN15           | 17.50       | Low Fat          | Medium      | Tier 1               |
| NCD19           | 8.93        | Low Fat          | High        | Tier 3               |
| FDP10           | 16.20       | Low Fat          | Medium      | Tier 3               |
| DRJ24           | 2.73        | Low Fat          | Medium      | Tier 1               |
| FDS46           | 17.60       | Regular          | Small       | Tier 1               |

---

### 11.3 Dropping Nulls in Specific Columns
`df.dropna(subset=['col1', 'col2'])`

#### 📐 Syntax
```python
df.dropna(subset=["column_name_1", "column_name_2"])
```

#### 📖 Explanation
Restricts null evaluation exclusively to columns specified in `subset`. Rows with nulls in unlisted columns are preserved.

#### 💻 Code Example
```python
# Drop rows only if Outlet_Size is null
df.dropna(subset=['Outlet_Size']).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type   | Item_MRP | Outlet_Identifier | Outlet_Size |
|-----------------|-------------|----------|-------------------|-------------|
| FDA15           | Dairy       | 249.8    | OUT049            | Medium      |
| DRC01           | Soft Drinks | 48.26    | OUT018            | Medium      |
| FDN15           | Meat        | 141.61   | OUT049            | Medium      |
| NCD19           | Household   | 53.86    | OUT013            | High        |
| FDP10           | Snack Foods | 104.99   | OUT027            | Medium      |
| DRJ24           | Soft Drinks | 39.11    | OUT049            | Medium      |
| FDS46           | Snack Foods | 119.67   | OUT046            | Small       |

---

### 11.4 Imputing Nulls Across All Columns
`df.fillna(replacement_value)`

#### 📐 Syntax
```python
df.fillna(value)
df.na.fill(value)
```

#### 📖 Explanation
Replaces all `null` values across all matching data type columns with a supplied default fallback value.

#### 💻 Code Example
```python
# Replace all null string values with 'NotAvailable'
df.fillna('NotAvailable').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type             | Outlet_Identifier | Outlet_Size  | Outlet_Location_Type |
|-----------------|-----------------------|-------------------|--------------|----------------------|
| FDA15           | Dairy                 | OUT049            | Medium       | Tier 1               |
| DRC01           | Soft Drinks           | OUT018            | Medium       | Tier 3               |
| FDX07           | Fruits and Vegetables | OUT010            | NotAvailable | Tier 3               |
| FDH17           | Meat                  | OUT017            | NotAvailable | Tier 2               |
| FDU28           | Frozen Foods          | OUT045            | NotAvailable | Tier 2               |

---

### 11.5 Imputing Nulls for Specific Columns
`df.fillna(value, subset=['col1', 'col2'])`

#### 📐 Syntax
```python
df.fillna(value, subset=["col1", "col2"])
df.fillna({"col1": "DefaultA", "col2": 0.0})
```

#### 📖 Explanation
Targeted imputation for specified columns only, leaving nulls in other columns untouched.

#### 💻 Code Example
```python
# Impute nulls specifically for Outlet_Size column
df.fillna('NotAvailable', subset=['Outlet_Size']).display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type             | Outlet_Identifier | Outlet_Size  |
|-----------------|-----------------------|-------------------|--------------|
| FDA15           | Dairy                 | OUT049            | Medium       |
| DRC01           | Soft Drinks           | OUT018            | Medium       |
| FDX07           | Fruits and Vegetables | OUT010            | NotAvailable |
| FDH17           | Meat                  | OUT017            | NotAvailable |
| FDU28           | Frozen Foods          | OUT045            | NotAvailable |

---

## Module 12: Arrays, Splitting & Exploding

### 12.1 Splitting Strings into Arrays
`split(col, pattern)`

#### 📐 Syntax
```python
from pyspark.sql.functions import split
split(str: Column/str, pattern: str, limit: int = -1)
```

#### 📖 Explanation
Splits a string column around matches of the specified delimiter regex into an `ArrayType(StringType)`.

#### 💻 Code Example
```python
from pyspark.sql.functions import split

# Split Outlet_Type by space character: 'Supermarket Type1' -> ['Supermarket', 'Type1']
df.withColumn('Outlet_Type_Arr', split('Outlet_Type', ' ')).display()
```

#### 📊 DataFrame Output
| Outlet_Identifier | Outlet_Type (Original) | Outlet_Type_Arr (Array)  |
|-------------------|------------------------|--------------------------|
| OUT049            | Supermarket Type1      | ["Supermarket", "Type1"] |
| OUT018            | Supermarket Type2      | ["Supermarket", "Type2"] |
| OUT010            | Grocery Store          | ["Grocery", "Store"]     |
| OUT027            | Supermarket Type3      | ["Supermarket", "Type3"] |

---

### 12.2 Extracting Array Elements by Index
`split(col, ' ')[1] / col.getItem(1)`

#### 📐 Syntax
```python
# Indexing with bracket syntax:
split(col("col_name"), " ")[1]

# Indexing with getItem():
col("array_col").getItem(1)
```

#### 📖 Explanation
Extracts a specific element from an array column by its 0-indexed position.

#### 💻 Code Example
```python
# Extract the second word (index 1) from Outlet_Type
df.withColumn('Outlet_Subtype', split('Outlet_Type', ' ')[1]).display()
```

#### 📊 DataFrame Output
| Outlet_Identifier | Outlet_Type       | Outlet_Subtype |
|-------------------|-------------------|----------------|
| OUT049            | Supermarket Type1 | Type1          |
| OUT018            | Supermarket Type2 | Type2          |
| OUT010            | Grocery Store     | Store          |
| OUT027            | Supermarket Type3 | Type3          |

---

### 12.3 Exploding Arrays into Multiple Rows
`explode(col)`

#### 📐 Syntax
```python
from pyspark.sql.functions import explode
explode(col: Column/str)
```

#### 📖 Explanation
Creates a new row for each element in the given array or map column, duplicating all other column values in the record.

#### 💻 Code Example
```python
from pyspark.sql.functions import split, explode

# 1. Split string into array
df_exp = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))

# 2. Explode array elements into individual rows
df_exp.withColumn('Outlet_Type_Element', explode('Outlet_Type')).display()
```

#### 📊 DataFrame Output
| Outlet_Identifier | Outlet_Type (Array)      | Outlet_Type_Element (Exploded Row) |
|-------------------|--------------------------|------------------------------------|
| OUT049            | ["Supermarket", "Type1"] | Supermarket                        |
| OUT049            | ["Supermarket", "Type1"] | Type1                              |
| OUT018            | ["Supermarket", "Type2"] | Supermarket                        |
| OUT018            | ["Supermarket", "Type2"] | Type2                              |
| OUT010            | ["Grocery", "Store"]     | Grocery                            |
| OUT010            | ["Grocery", "Store"]     | Store                              |

---

### 12.4 Checking Array Membership (array_contains)
`array_contains(array_col, value)`

#### 📐 Syntax
```python
from pyspark.sql.functions import array_contains
array_contains(column: Column/str, value: Any)
```

#### 📖 Explanation
Evaluates whether an array column contains a specified target value, returning `True`, `False`, or `null`.

#### 💻 Code Example
```python
from pyspark.sql.functions import array_contains

# Check if Outlet_Type array contains the element 'Type1'
df_exp.withColumn('Type1_flag', array_contains('Outlet_Type', 'Type1')).display()
```

#### 📊 DataFrame Output
| Outlet_Identifier | Outlet_Type (Array)      | Type1_flag |
|-------------------|--------------------------|------------|
| OUT049            | ["Supermarket", "Type1"] | True       |
| OUT018            | ["Supermarket", "Type2"] | False      |
| OUT010            | ["Grocery", "Store"]     | False      |
| OUT027            | ["Supermarket", "Type3"] | False      |

---

## Module 13: GroupBy & Aggregations

### 13.1 Scenario 1: Group By Single Column with sum()
`df.groupBy('col').agg(sum('metric'))`

#### 📐 Syntax
```python
from pyspark.sql.functions import sum
df.groupBy("group_col").agg(sum("numeric_col"))
```

#### 📖 Explanation
Groups DataFrame rows by distinct values of a category column and calculates the total sum of a numeric column for each group.

#### 💻 Code Example
```python
from pyspark.sql.functions import sum

# Calculate total Item_MRP sum grouped by Item_Type
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()
```

#### 📊 DataFrame Output
| Item_Type             | sum(Item_MRP) |
|-----------------------|---------------|
| Dairy                 | 249.8         |
| Soft Drinks           | 87.37         |
| Meat                  | 238.05        |
| Fruits and Vegetables | 182.09        |
| Household             | 53.86         |
| Snack Foods           | 224.66        |
| Frozen Foods          | 190.05        |

---

### 13.2 Scenario 2: Group By Single Column with avg()
`df.groupBy('col').agg(avg('metric'))`

#### 📐 Syntax
```python
from pyspark.sql.functions import avg
df.groupBy("group_col").agg(avg("numeric_col"))
```

#### 📖 Explanation
Computes the arithmetic mean (average) for each category group.

#### 💻 Code Example
```python
from pyspark.sql.functions import avg

# Compute average Item_MRP per Item_Type
df.groupBy('Item_Type').agg(avg('Item_MRP')).display()
```

#### 📊 DataFrame Output
| Item_Type             | avg(Item_MRP) |
|-----------------------|---------------|
| Dairy                 | 249.8         |
| Soft Drinks           | 43.685        |
| Meat                  | 119.025       |
| Fruits and Vegetables | 182.09        |
| Household             | 53.86         |
| Snack Foods           | 112.33        |
| Frozen Foods          | 190.05        |

---

### 13.3 Scenario 3: Group By Multiple Columns with Aliasing
`df.groupBy('c1', 'c2').agg(sum('metric').alias('Total'))`

#### 📐 Syntax
```python
df.groupBy("col1", "col2").agg(sum("metric").alias("Custom_Alias"))
```

#### 📖 Explanation
Groups by a composite key (multiple dimensions) and assigns a clean business alias to the aggregation output column.

#### 💻 Code Example
```python
# Group by Item_Type and Outlet_Size, compute sum with custom alias
df.groupBy('Item_Type', 'Outlet_Size').agg(
    sum('Item_MRP').alias('Total_MRP')
).display()
```

#### 📊 DataFrame Output
| Item_Type             | Outlet_Size | Total_MRP |
|-----------------------|-------------|-----------|
| Dairy                 | Medium      | 249.8     |
| Soft Drinks           | Medium      | 87.37     |
| Meat                  | Medium      | 141.61    |
| Meat                  | NULL        | 96.44     |
| Fruits and Vegetables | NULL        | 182.09    |
| Household             | High        | 53.86     |
| Snack Foods           | Medium      | 104.99    |
| Snack Foods           | Small       | 119.67    |
| Frozen Foods          | NULL        | 190.05    |

---

### 13.4 Scenario 4: Multiple Aggregation Expressions
`df.groupBy('col').agg(sum('m1'), avg('m2'), min('m3'), max('m4'))`

#### 📐 Syntax
```python
df.groupBy(*group_cols).agg(*agg_expressions)
```

#### 📖 Explanation
Executes multiple distinct aggregation functions across multiple metrics simultaneously in a single distributed pass.

#### 💻 Code Example
```python
# Compute both SUM and AVERAGE in one pass
df.groupBy('Item_Type', 'Outlet_Size').agg(
    sum('Item_MRP').alias('Sum_MRP'),
    avg('Item_MRP').alias('Avg_MRP')
).display()
```

#### 📊 DataFrame Output
| Item_Type   | Outlet_Size | Sum_MRP | Avg_MRP |
|-------------|-------------|---------|---------|
| Dairy       | Medium      | 249.8   | 249.8   |
| Soft Drinks | Medium      | 87.37   | 43.685  |
| Meat        | Medium      | 141.61  | 141.61  |
| Meat        | NULL        | 96.44   | 96.44   |
| Snack Foods | Medium      | 104.99  | 104.99  |
| Snack Foods | Small       | 119.67  | 119.67  |

---

## Module 14: Advanced Aggregations (Collect List & Pivot)

### 14.1 Collecting Grouped Rows into Lists (collect_list)
`df.groupBy('user').agg(collect_list('item'))`

#### 📐 Syntax
```python
from pyspark.sql.functions import collect_list, collect_set
collect_list(col)  # Preserves duplicates
collect_set(col)   # Keeps unique items only
```

#### 📖 Explanation
Gathers grouped scalar values from multiple rows into a single combined Python array/list per key.
- `collect_list()`: Retains all items including duplicates.
- `collect_set()`: Deduplicates items within each group.

#### 💻 Code Example
```python
from pyspark.sql.functions import collect_list

# Sample data: user-book associations
data = [('user1', 'book1'), ('user1', 'book2'), ('user2', 'book2'), ('user2', 'book4'), ('user3', 'book1')]
df_book = spark.createDataFrame(data, 'user STRING, book STRING')

# Aggregate books per user into a list
df_book.groupBy('user').agg(collect_list('book').alias('book_list')).display()
```

#### 📊 DataFrame Output
| user  | book_list          |
|-------|--------------------|
| user1 | ["book1", "book2"] |
| user2 | ["book2", "book4"] |
| user3 | ["book1"]          |

---

### 14.2 Reshaping Rows into Columns (pivot)
`df.groupBy('row_key').pivot('pivot_col').agg(avg('metric'))`

#### 📐 Syntax
```python
df.groupBy("row_dimension") \
  .pivot("column_to_rotate", values_list=None) \
  .agg(aggregation_function)
```

#### 📖 Explanation
Rotates unique values from a categorical column into distinct horizontal columns (cross-tabulation matrix representation).

> 💡 **PRO TIP:** Always provide an explicit list of pivot values (e.g. `.pivot('Outlet_Size', ['High', 'Medium', 'Small'])`) in production to avoid an extra distinct scan.

#### 💻 Code Example
```python
# Pivot Outlet_Size values (High, Medium, Small, null) into columns
df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()
```

#### 📊 DataFrame Output
| Item_Type             | High  | Medium | Small  | null   |
|-----------------------|-------|--------|--------|--------|
| Dairy                 | NULL  | 249.8  | NULL   | NULL   |
| Soft Drinks           | NULL  | 43.685 | NULL   | NULL   |
| Meat                  | NULL  | 141.61 | NULL   | 96.44  |
| Fruits and Vegetables | NULL  | NULL   | NULL   | 182.09 |
| Household             | 53.86 | NULL   | NULL   | NULL   |
| Snack Foods           | NULL  | 104.99 | 119.67 | NULL   |
| Frozen Foods          | NULL  | NULL   | NULL   | 190.05 |

---

## Module 15: Conditional Logic (when - otherwise)

### 15.1 Scenario 1: Binary If-Else Condition
`when(condition, value).otherwise(default_value)`

#### 📐 Syntax
```python
from pyspark.sql.functions import when, col
when(col("column") == "target_val", "matched_val").otherwise("default_val")
```

#### 📖 Explanation
Evaluates a list of conditions and returns one of multiple possible result expressions, equivalent to SQL `CASE WHEN ... THEN ... ELSE ... END`.

#### 💻 Code Example
```python
from pyspark.sql.functions import when, col

# Classify items as 'Non-Veg' if Item_Type is 'Meat', otherwise 'Veg'
df = df.withColumn('veg_flag', when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg'))

df.select('Item_Identifier', 'Item_Type', 'Item_MRP', 'veg_flag').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type             | Item_MRP | veg_flag |
|-----------------|-----------------------|----------|----------|
| FDA15           | Dairy                 | 249.8    | Veg      |
| DRC01           | Soft Drinks           | 48.26    | Veg      |
| FDN15           | Meat                  | 141.61   | Non-Veg  |
| FDX07           | Fruits and Vegetables | 182.09   | Veg      |
| FDH17           | Meat                  | 96.44    | Non-Veg  |

---

### 15.2 Scenario 2: Chained Multi-Branch Conditionals
`when(cond1, v1).when(cond2, v2).otherwise(v3)`

#### 📐 Syntax
```python
when(cond1, val1) \
  .when(cond2, val2) \
  .when(cond3, val3) \
  .otherwise(default_val)
```

#### 📖 Explanation
Chains multiple `when()` conditions sequentially to create multi-tier category segmentations.

#### 💻 Code Example
```python
# Segment items into Veg_Inexpensive, Veg_Expensive, or Non_Veg
df.withColumn('veg_exp_flag',
    when(((col('veg_flag') == 'Veg') & (col('Item_MRP') < 100)), 'Veg_Inexpensive')
    .when(((col('veg_flag') == 'Veg') & (col('Item_MRP') >= 100)), 'Veg_Expensive')
    .otherwise('Non_Veg')
).select('Item_Identifier', 'Item_Type', 'Item_MRP', 'veg_flag', 'veg_exp_flag').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type             | Item_MRP | veg_flag | veg_exp_flag    |
|-----------------|-----------------------|----------|----------|-----------------|
| FDA15           | Dairy                 | 249.8    | Veg      | Veg_Expensive   |
| DRC01           | Soft Drinks           | 48.26    | Veg      | Veg_Inexpensive |
| FDN15           | Meat                  | 141.61   | Non-Veg  | Non_Veg         |
| FDX07           | Fruits and Vegetables | 182.09   | Veg      | Veg_Expensive   |
| FDH17           | Meat                  | 96.44    | Non-Veg  | Non_Veg         |

---

## Module 16: Joining DataFrames

### 16.1 Inner Join
`df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner')`

#### 📐 Syntax
```python
df1.join(df2, on=join_condition, how="inner")
```

#### 📖 Explanation
Returns only rows where the join key matches in **both** the left (`df1`) and right (`df2`) DataFrames. Unmatched rows from either side are discarded.

> 💡 **PRO TIP:** Notice emp_id 6 ('nad' in 'd06') and dept_id 'd04' ('IT') are excluded because they have no counterpart in the other table.

#### 💻 Code Example
```python
# Inner join on department ID
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner').display()
```

#### 📊 DataFrame Output
| emp_id | emp_name | dept_id | department |
|--------|----------|---------|------------|
| 1      | gaur     | d01     | HR         |
| 2      | kit      | d02     | Marketing  |
| 3      | sam      | d03     | Accounts   |
| 4      | tim      | d03     | Accounts   |
| 5      | aman     | d05     | Finance    |

---

### 16.2 Left Outer Join
`df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left')`

#### 📐 Syntax
```python
df1.join(df2, on=join_condition, how="left")  # or "left_outer"
```

#### 📖 Explanation
Preserves all records from the left DataFrame (`df1`). If a matching row is found in `df2`, department details are populated; otherwise right columns receive `null`.

#### 💻 Code Example
```python
# Left join to preserve all employees
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left').display()
```

#### 📊 DataFrame Output
| emp_id | emp_name | dept_id | department |
|--------|----------|---------|------------|
| 1      | gaur     | d01     | HR         |
| 2      | kit      | d02     | Marketing  |
| 3      | sam      | d03     | Accounts   |
| 4      | tim      | d03     | Accounts   |
| 5      | aman     | d05     | Finance    |
| 6      | nad      | d06     | NULL       |

---

### 16.3 Right Outer Join
`df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right')`

#### 📐 Syntax
```python
df1.join(df2, on=join_condition, how="right")  # or "right_outer"
```

#### 📖 Explanation
Preserves all records from the right DataFrame (`df2`). Unmatched departments (e.g. `d04` IT) receive `null` for employee columns.

#### 💻 Code Example
```python
# Right join to preserve all departments
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right').display()
```

#### 📊 DataFrame Output
| emp_id | emp_name | dept_id | department |
|--------|----------|---------|------------|
| 1      | gaur     | d01     | HR         |
| 2      | kit      | d02     | Marketing  |
| 3      | sam      | d03     | Accounts   |
| 4      | tim      | d03     | Accounts   |
| NULL   | NULL     | d04     | IT         |
| 5      | aman     | d05     | Finance    |

---

### 16.4 Left Anti Join (Finding Orphan Records)
`df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti')`

#### 📐 Syntax
```python
df1.join(df2, on=join_condition, how="anti")  # or "left_anti"
```

#### 📖 Explanation
Returns only rows from `df1` that have **NO match** in `df2`. Does not include columns from `df2`. Highly efficient for finding missing or orphan foreign keys.

> 💡 **PRO TIP:** Anti joins are vastly faster than `NOT IN (SELECT ...)` subqueries in Spark because they avoid expensive cross-joins.

#### 💻 Code Example
```python
# Find employees belonging to departments that do not exist in df2
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()
```

#### 📊 DataFrame Output
| emp_id | emp_name | dept_id |
|--------|----------|---------|
| 6      | nad      | d06     |

---

### 16.5 Left Semi Join (Filtering Match Check)
`df1.join(df2, df1['dept_id'] == df2['dept_id'], 'semi')`

#### 📐 Syntax
```python
df1.join(df2, on=join_condition, how="semi")  # or "left_semi"
```

#### 📖 Explanation
Returns rows from `df1` that have a match in `df2`, but without appending any columns from `df2`. Acts as an efficient existence filter without duplicating rows.

#### 💻 Code Example
```python
# Return only df1 records that have a valid department in df2
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'semi').display()
```

#### 📊 DataFrame Output
| emp_id | emp_name | dept_id |
|--------|----------|---------|
| 1      | gaur     | d01     |
| 2      | kit      | d02     |
| 3      | sam      | d03     |
| 4      | tim      | d03     |
| 5      | aman     | d05     |

---

### 16.6 Full Outer Join
`df1.join(df2, df1['dept_id'] == df2['dept_id'], 'full')`

#### 📐 Syntax
```python
df1.join(df2, on=join_condition, how="full")  # or "outer", "full_outer"
```

#### 📖 Explanation
Retains all records from both DataFrames, matching where possible and injecting nulls on either side when a record has no counterpart.

#### 💻 Code Example
```python
# Full outer join
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'full').display()
```

#### 📊 DataFrame Output
| emp_id | emp_name | dept_id | department |
|--------|----------|---------|------------|
| 1      | gaur     | d01     | HR         |
| 2      | kit      | d02     | Marketing  |
| 3      | sam      | d03     | Accounts   |
| 4      | tim      | d03     | Accounts   |
| NULL   | NULL     | d04     | IT         |
| 5      | aman     | d05     | Finance    |
| 6      | nad      | d06     | NULL       |

---

## Module 17: Window Analytical Functions

### 17.1 Row Number (row_number)
`row_number().over(Window.orderBy('col'))`

#### 📐 Syntax
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("part_col").orderBy("order_col")
row_number().over(windowSpec)
```

#### 📖 Explanation
Assigns a sequential unique integer (1, 2, 3, ...) to each row within a window partition based on the order specification.

#### 💻 Code Example
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Assign unique row number ordered by Item_Identifier
df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier'))).select('Item_Identifier', 'Item_Type', 'rowCol').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type   | rowCol |
|-----------------|-------------|--------|
| DRC01           | Soft Drinks | 1      |
| DRJ24           | Soft Drinks | 2      |
| FDA15           | Dairy       | 3      |
| FDH17           | Meat        | 4      |
| FDN15           | Meat        | 5      |
| FDP10           | Snack Foods | 6      |

---

### 17.2 Rank vs Dense Rank Comparison
`rank().over(...) vs dense_rank().over(...)`

#### 📐 Syntax
```python
from pyspark.sql.functions import rank, dense_rank
rank().over(windowSpec)        # Leaves gaps after ties (1, 2, 2, 4)
dense_rank().over(windowSpec)  # No gaps after ties (1, 2, 2, 3)
```

#### 📖 Explanation
Ranks rows based on ordering values when ties occur:
- `rank()`: When two rows share identical values, both receive the same rank, and the next rank skips values (e.g. 1, 2, 2, 4).
- `dense_rank()`: Tied rows receive the same rank, but subsequent ranks continue without gaps (e.g. 1, 2, 2, 3).

#### 💻 Code Example
```python
from pyspark.sql.functions import rank, dense_rank

# Compare rank and dense_rank ordered by Item_Weight descending
df.withColumn('rank', rank().over(Window.orderBy(col('Item_Weight').desc()))) \
  .withColumn('denseRank', dense_rank().over(Window.orderBy(col('Item_Weight').desc()))) \
  .select('Item_Identifier', 'Item_Weight', 'rank', 'denseRank').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | rank (with gaps) | denseRank (no gaps) |
|-----------------|-------------|------------------|---------------------|
| FDX07           | 19.20       | 1                | 1                   |
| FDU28           | 19.20       | 1                | 1                   |
| FDS46           | 17.60       | 3                | 2                   |
| FDN15           | 17.50       | 4                | 3                   |
| FDP10           | 16.20       | 5                | 4                   |
| FDH17           | 16.20       | 5                | 4                   |
| FDA15           | 9.30        | 7                | 5                   |

---

### 17.3 Running Sum with Frame Bounds (rowsBetween)
`sum(col).over(Window.orderBy(...).rowsBetween(unboundedPreceding, currentRow))`

#### 📐 Syntax
```python
Window.orderBy("col").rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

#### 📖 Explanation
Explicitly defines the window frame from the very first row in the partition (`Window.unboundedPreceding`) up to the current row (`Window.currentRow`) to calculate an accurate cumulative running total.

#### 💻 Code Example
```python
# Calculate cumulative running total of Item_MRP ordered by Item_Identifier
df.withColumn('running_total',
    sum('Item_MRP').over(
        Window.orderBy('Item_Identifier')
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
).select('Item_Identifier', 'Item_MRP', 'running_total').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_MRP | running_total |
|-----------------|----------|---------------|
| DRC01           | 48.26    | 48.26         |
| DRJ24           | 39.11    | 87.37         |
| FDA15           | 249.8    | 337.17        |
| FDH17           | 96.44    | 433.61        |
| FDN15           | 141.61   | 575.22        |

---

### 17.4 Total Partition Sum (unboundedFollowing)
`sum(col).over(Window.rowsBetween(unboundedPreceding, unboundedFollowing))`

#### 📐 Syntax
```python
Window.partitionBy("category").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
```

#### 📖 Explanation
Spans the full window from start to end (`unboundedFollowing`), broadcasting the grand total of the entire partition onto each individual row without collapsing rows like a standard `groupBy`.

#### 💻 Code Example
```python
# Attach overall grand total MRP across all rows
df.withColumn('totalsum',
    sum('Item_MRP').over(
        Window.orderBy('Item_Type')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
).select('Item_Identifier', 'Item_Type', 'Item_MRP', 'totalsum').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Type             | Item_MRP | totalsum |
|-----------------|-----------------------|----------|----------|
| FDA15           | Dairy                 | 249.8    | 1152.0   |
| DRC01           | Soft Drinks           | 48.26    | 1152.0   |
| FDN15           | Meat                  | 141.61   | 1152.0   |
| FDX07           | Fruits and Vegetables | 182.09   | 1152.0   |

---

## Module 18: User Defined Functions (UDF)

### 18.1 Defining & Registering Python UDFs
`udf(python_function, ReturnType())`

#### 📐 Syntax
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType

# Method 1: Functional wrapper
def my_func(x):
    return x * x if x else None

my_udf = udf(my_func, DoubleType())

# Method 2: Decorator syntax
@udf(returnType=DoubleType())
def square_udf(x):
    return x * x if x else None
```

#### 📖 Explanation
Allows standard custom Python functions to be executed distributedly across worker nodes on DataFrame columns.
- Always specify an explicit `returnType` (defaults to StringType).

> ⚠️ **WARNING:** Standard Python UDFs incur serialization overhead between the JVM and Python worker processes. Whenever possible, prefer built-in Spark SQL functions or Pandas Vectorized UDFs (PyArrow).

#### 💻 Code Example
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# 1. Define standard Python logic
def my_func(x):
    return x * x if x is not None else None

# 2. Register as PySpark UDF
my_udf = udf(my_func, DoubleType())
```

#### 📊 DataFrame Output
| UDF Name | Input Type            | Return Type | Execution Target              |
|----------|-----------------------|-------------|-------------------------------|
| my_udf   | DoubleType (Item_MRP) | DoubleType  | Worker Node JVM/Python Bridge |

---

### 18.2 Applying UDF to DataFrame Columns
`df.withColumn('new_col', my_udf('col'))`

#### 📐 Syntax
```python
df.withColumn("target_column", my_udf("source_column"))
```

#### 📖 Explanation
Executes the custom UDF on every record of the source column and attaches the resulting value to a new column.

#### 💻 Code Example
```python
# Apply custom UDF to square the Item_MRP column
df.withColumn('mrp_squared', my_udf('Item_MRP')) \
  .select('Item_Identifier', 'Item_MRP', 'mrp_squared').display()
```

#### 📊 DataFrame Output
| Item_Identifier | Item_MRP | mrp_squared |
|-----------------|----------|-------------|
| DRC01           | 48.26    | 2329.0276   |
| DRJ24           | 39.11    | 1529.5921   |
| NCD19           | 53.86    | 2900.8996   |
| FDH17           | 96.44    | 9300.6736   |

---

## Module 19: Data Writing & Persistence

### 19.1 Writing Data to CSV Files
`df.write.format('csv').option('header', True).save(path)`

#### 📐 Syntax
```python
df.write.format("csv") \
  .mode("overwrite") \
  .option("header", True) \
  .save(destination_path)
```

#### 📖 Explanation
Persists the distributed DataFrame back to disk/cloud storage in CSV format with optional header row inclusion.

#### 💻 Code Example
```python
# Write DataFrame out to CSV storage directory
df.write.format('csv') \
  .mode('overwrite') \
  .option('header', True) \
  .save('/FileStore/tables/CSV/data.csv')
```

#### 📊 DataFrame Output
| Target Storage Path            | Format | Write Mode | Status                    |
|--------------------------------|--------|------------|---------------------------|
| /FileStore/tables/CSV/data.csv | CSV    | overwrite  | SUCCESS (Files committed) |

---

### 19.2 Save Modes Comparison (append, overwrite, error, ignore)
`df.write.mode('append' | 'overwrite' | 'error' | 'ignore')`

#### 📐 Syntax
```python
df.write.format("parquet").mode("append").save(path)
df.write.format("parquet").mode("overwrite").save(path)
df.write.format("parquet").mode("error").save(path)       # or "errorifexists"
df.write.format("parquet").mode("ignore").save(path)
```

#### 📖 Explanation
Controls the behavior when destination files or tables already exist:
- `append`: Adds new output files to existing directory without modifying existing files.
- `overwrite`: Completely wipes out existing directory contents and replaces them with new data.
- `error` (default): Throws an AnalysisException if target path already exists.
- `ignore`: Silently skips writing if destination already exists without erroring.

#### 💻 Code Example
```python
# Overwrite mode
df.write.format('csv').mode('overwrite').save('/FileStore/tables/CSV/data.csv')

# Append mode
df.write.format('csv').mode('append').save('/FileStore/tables/CSV/data.csv')

# Error if exists mode (Default)
df.write.format('csv').mode('error').save('/FileStore/tables/CSV/data.csv')

# Ignore mode
df.write.format('csv').mode('ignore').save('/FileStore/tables/CSV/data.csv')
```

#### 📊 DataFrame Output
| Save Mode Keyword     | Behavior if Target Exists                           | Behavior if Target Does Not Exist |
|-----------------------|-----------------------------------------------------|-----------------------------------|
| append                | Appends new part-files to existing directory        | Creates new directory and saves   |
| overwrite             | Deletes existing content and replaces with new data | Creates new directory and saves   |
| error / errorifexists | Throws AnalysisException error (Safe mode)          | Creates new directory and saves   |
| ignore                | Silently does nothing (No-op)                       | Creates new directory and saves   |

---

### 19.3 Parquet Format & Saving as Table
`df.write.format('parquet').saveAsTable('my_table')`

#### 📐 Syntax
```python
# Save to Parquet directory:
df.write.format("parquet").mode("overwrite").save(path)

# Save as Managed Metastore / Unity Catalog Table:
df.write.format("parquet").mode("overwrite").saveAsTable("table_name")
```

#### 📖 Explanation
Parquet is a columnar storage format with snappy compression, drastically reducing storage costs and boosting read query performance by 10x-100x through column pruning and statistics pushdown. `saveAsTable()` registers metadata in Databricks Hive Metastore or Unity Catalog.

#### 💻 Code Example
```python
# Write optimized Parquet files
df.write.format('parquet') \
  .mode('overwrite') \
  .save('/FileStore/tables/Parquet/sales.parquet')

# Save directly as queryable table in metastore
df.write.format('parquet') \
  .mode('overwrite') \
  .saveAsTable('my_bigmart_table')
```

#### 📊 DataFrame Output
| Table / File Name | Storage Format         | Catalog Registration           | Compression      |
|-------------------|------------------------|--------------------------------|------------------|
| sales.parquet     | Apache Parquet         | DBFS Path                      | Snappy (Default) |
| my_bigmart_table  | Apache Parquet / Delta | Hive Metastore / Unity Catalog | Snappy (Default) |

---

## Module 20: Spark SQL & Temporary Views

### 20.1 Creating Temporary Views
`df.createOrReplaceTempView('my_view')`

#### 📐 Syntax
```python
df.createTempView("view_name")
df.createOrReplaceTempView("view_name")  # Safe: overwrites if view exists
df.createGlobalTempView("global_view_name") # Shared across Spark sessions
```

#### 📖 Explanation
Registers a DataFrame as a session-scoped SQL temporary view, enabling direct SQL querying using standard SQL syntax.

#### 💻 Code Example
```python
# Register DataFrame as a SQL temporary view
df.createOrReplaceTempView('my_view')
```

#### 📊 DataFrame Output
| View Name | Scope                | Persistence                                 |
|-----------|----------------------|---------------------------------------------|
| my_view   | Current SparkSession | In-memory metadata pointer (Zero data copy) |

---

### 20.2 Executing SQL Queries (spark.sql & %sql)
`spark.sql('SELECT * FROM my_view WHERE ...')`

#### 📐 Syntax
```python
# Python API:
df_result = spark.sql("SELECT column1, column2 FROM my_view WHERE condition")

# Databricks Magic Command:
# %sql
# SELECT * FROM my_view WHERE Item_Fat_Content = 'Lf'
```

#### 📖 Explanation
Executes ANSI SQL queries against registered views or catalog tables, returning a standard PySpark DataFrame for downstream programmatic manipulation.

#### 💻 Code Example
```python
# 1. Using Python spark.sql()
df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Lf'")
df_sql.display()

# 2. Or using Databricks %sql cell magic:
# %sql
# SELECT * FROM my_view WHERE Item_Fat_Content = 'Lf'
```

#### 📊 DataFrame Output
| Item_Identifier | Item_Weight | Item_Fat_Content | Item_Type   | Item_MRP |
|-----------------|-------------|------------------|-------------|----------|
| FDA15           | 9.30        | Lf               | Dairy       | 249.8    |
| FDN15           | 17.50       | Lf               | Meat        | 141.61   |
| NCD19           | 8.93        | Lf               | Household   | 53.86    |
| FDP10           | 16.20       | Lf               | Snack Foods | 104.99   |
| DRJ24           | 2.73        | Lf               | Soft Drinks | 39.11    |

---
