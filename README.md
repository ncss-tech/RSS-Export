# RSS-Export
The purpose of the Build Raster Soil Survey (RSS) Database tool is to build the required RSS ESRI file geodatabase with the gSSURGO template and an open-source package required to publish RSSs. This tool ensures standardized products are posted to the NRCS Data Gateway (Box) for users according to the National Soil Survey Handbook, Part 648. The standardized databases closely resemble a gSSURGO dataqbase and include both spatial and tabular data.  The largest difference is there are no vector layers.  Aside from standardization, the other principal benefit is these databases are honored in the Soil Data Development Toolbox > gSSURGO Mapping Toolset > Create Soil Map tool.  This allows users to retrieve soil properties and interpretations for the RSS raster layer.  
This tool creates a Raster Soil Survey (RSS) database as a File Geodatabase with a gSSURGO template (relational database) and as a SSURGO Download Folder

This toolset combines the three tools from the original ArcMap Desktop version into a single tool.

This tool imports the text files and the raster into the file geodatabase with relationship classes and indices define. It also updates the metadata. 

The raster is imported and named MURASTER_10m_<State abbreviation>_<yyyy>.

It then creates an open-soucre package structured like a SSURGO Download file with the text files in the tabular subdirectory and the raster as a GeoTIFF into the spatial subdirectory.
