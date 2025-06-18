# RSS-Export
The purpose of the Build Raster Soil Survey (RSS) Database tool is to build the required RSS ESRI file geodatabase with the gSSURGO template and an open-source package required to publish RSSs. This tool ensures standardized products are posted to the NRCS Data Gateway (Box) for users according to the National Soil Survey Handbook, Part 648. The standardized databases closely resemble a gSSURGO dataqbase and include both spatial and tabular data.  The largest difference is there are no vector layers.  Aside from standardization, the other principal benefit is these databases are honored in the Soil Data Development Toolbox > gSSURGO Mapping Toolset > Create Soil Map tool.  This allows users to retrieve soil properties and interpretations for the RSS raster layer.  
This tool creates a Raster Soil Survey (RSS) database as a File Geodatabase with a gSSURGO template (relational database) and as a SSURGO Download Folder

This toolset combines the three tools from the original ArcMap Desktop version into a single tool.

This tool imports the text files and the raster into the file geodatabase with relationship classes and indices define. It also updates the metadata. 

The raster is imported and named MURASTER_10m_<State abbreviation>_<yyyy>.

It then creates an open-soucre package structured like a SSURGO Download file with the text files in the tabular subdirectory and the raster as a GeoTIFF into the spatial subdirectory.

A second tool, Validate RSS Datasets, is also included
It validates RSS packages prior to uploading to Box

This tool will walk through a top directory and check for:

a properly formatted package (e.g. RSS_FL and RSS_FL.gdb)
a suitably named MURASTER_10m__ (gdb and tif)
metadata for tif as MURASTER_10m__.xml
MURASTER_10m__ is unsigned 32 bit
MURASTER_10m__ has NoData value 2147483647 (tif only, gdb raster does not have NoData value exposed)
proper spatial reference (32161, 5070, 3338, 4326)
required text files in open source package (no extraneous)
required gdb tables (no extraneous)
mukeys in raster (tif/gdb) match mukeys in text/gdb tables
