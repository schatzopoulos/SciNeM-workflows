from pyspark.sql.functions import lit

# from https://stackoverflow.com/a/40403773/6938911
def harmonize_schemas_and_combine(df_left, df_right):
	left_types = {f.name: f.dataType for f in df_left.schema}
	right_types = {f.name: f.dataType for f in df_right.schema}
	left_fields = set((f.name, f.dataType, f.nullable) for f in df_left.schema)
	right_fields = set((f.name, f.dataType, f.nullable) for f in df_right.schema)

	# First go over left-unique fields
	for l_name, l_type, l_nullable in left_fields.difference(right_fields):
		if l_name in right_types:
			r_type = right_types[l_name]
			if l_type != r_type:
				raise TypeError("Union failed. Type conflict on field %s. left type %s, right type %s" % (l_name, l_type, r_type))
			else:
				raise TypeError("Union failed. Nullability conflict on field %s. left nullable %s, right nullable %s"  % (l_name, l_nullable, not(l_nullable)))
		df_right = df_right.withColumn(l_name, lit(None).cast(l_type))

	# Now go over right-unique fields
	for r_name, r_type, r_nullable in right_fields.difference(left_fields):
		if r_name in left_types:
			l_type = left_types[r_name]
			if r_type != l_type:
				raise TypeError("Union failed. Type conflict on field %s. right type %s, left type %s" % (r_name, r_type, l_type))
			else:
				raise TypeError("Union failed. Nullability conflict on field %s. right nullable %s, left nullable %s" % (r_name, r_nullable, not(r_nullable)))
		df_left = df_left.withColumn(r_name, lit(None).cast(r_type))    

	# Make sure columns are in the same order
	df_left = df_left.select(df_right.columns)

	return df_left.union(df_right)

def toCSVLine(data):
	# remove first character from id as it represents node type
	return str(data[0]) + "\t" + str(data[1])
