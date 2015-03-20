# --------------------------------------------------------------------
# NEW PROGRAM
# --------------------------------------------------------------------
# 1. ETL -> Create RDD from external file
# create RDD in DriverProgram
nums = sc.parallelize([1, 2, 3, 4])

# --------------------------------------------------
# 2. Create RDD via Transformation of previous RDD
# map() transformation
squared  = nums.map(lambda x: x*x).collect()
for num in squared:
	print "%i" %(num)
	
# --------------------------------------------------
# 3. store in memory, to enable multiple Actions!


# 4. Call an ACTION! Evaluate above Transformations!


