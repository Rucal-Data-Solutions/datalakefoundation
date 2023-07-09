# Datalake Foundation

Datalake Foundation is a library to process data, making it ready for transformation (your business logic)
It takes a data slice (parquet) from bronze and processes the data according to the config (Metadata)

Slice (Bronze) location:<br>
<root_folder>/bronze/\<connection>/\<entity_name>/<slice_file>

Silver location:<br>
<root_folder>/silver/\<connection>/\<entity_name>/<slice_file>

