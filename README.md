# Datalake Foundation

Datalake Foundation is a library to process data, making it ready for transformation (your business logic), it is founded on DataLakehouse principals, therefor fits perfect within DataLakehouse architecture. 
It takes a data slice (parquet) from bronze and processes the data according to the config (Metadata)

Features that are implemented:
- Processing
  - Full load
  - Delta processing

- Metadata
  - Json Metadata (JsonMetadataSettings)
  - Sql server database (SqlMetadataSettings)
 
- Data factory
  - Item generator (for loops)



Slice (Bronze) location:<br>
<root_folder>/bronze/\<connection>/\<entity_name>/<slice_file>

Silver location:<br>
<root_folder>/silver/\<connection>/\<entity_name>/<slice_file>

