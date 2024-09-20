#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build Raster Soil Survey (RSS) Database for ArcGIS Pro creates 
the RSS database, both the gSSURGO templated version and the SSURGO download
file structure with geoTIFF.
Created on: 09/19/2024

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov

@modified 09/19/2024
    @by: Alexnder Stum
@version: 1.1

# ---
The orginal tool this is base off of is from the ArcMap Desktop toolbox
ArcGIS Desktop Build RSS gdb. It will create a new file geodatabase and 
SSURGO download file directory the the propertly projected and aligned
geoTIFF.

Numpy Dostring format
"""

# https://pro.arcgis.com/en/pro-app/latest/arcpy/geoprocessing_and_python/a-template-for-python-toolboxes.htm
import arcpy
import os
from importlib import reload


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "RSS_SSURGO_Template_Tool"
        self.alias = 'RSS SSURGO Template Tools'

        # List of tool classes associated with this toolbox
        self.tools = [buildFGDB]


class buildFGDB(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create RSS Datasets"
        self.description = (
            "Create File Geodatabase with gSSURGO template"
        )
        self.states = [
        'AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
        'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
        'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
        'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX',
        'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
        ]

    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="Tabular Folder of Exported SSURGO Textfiles",
            name="inputFolder",
            direction="Input",
            parameterType="Required",
            datatype="Folder"
        )]

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="Input Raster",
            name="raster_p",
            direction="Input",
            parameterType="Required",
            datatype="DERasterDataset"
        ))

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Output Folder",
            name="out_p",
            direction="Input",
            parameterType="Required",
            datatype="DEFolder",
            enabled=True
        ))

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="Select State",
            name="state",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[-1].filter.type = "ValueList"
        params[-1].filter.list = self.states

        # parameter 4
        params.append(arcpy.Parameter(
            displayName="Fiscal Year of Publication",
            name="fy",
            direction="Input",
            parameterType="Required",
            datatype="GPLong",
            enabled=True
        ))

        # parameter 5
        params.append(arcpy.Parameter(
            displayName="gSSURGO Version",
            name="gSSURGO_v",
            direction="Input",
            parameterType="Optional",
            datatype="String",
            enabled=False
        ))
        params[-1].filter.type = "ValueList"
        params[-1].filter.list = ["gSSURGO traditional", "gSSURGO 2.0"]

        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        for i in range(6):
            params[i].clearMessage()

        # Raster must have an mukey field
        if params[1].value:
            rast_d = arcpy.Describe(params[1].value)
            rast_f = {
                f.name for f in rast_d.fields if f.name.lower() == "mukey"
            }
            if not rast_f:
                params[1].setErrorMessage(
                    f"{rast_d.name} does not have an mukey field"
                )

        # Year must be in range of 2024 - 2050
        if (fy := params[4].value):
            if (fy < 2024) or (fy > 2050):
                params[4].setErrorMessage(
                    "Fiscal year is not range (2024 - 2050)"
                )
            
        return

    def execute(self, params, messages):
        """The source code of the tool."""
        import SSURGO_Convert_to_Geodatabase
        # reload(SSURGO_Convert_to_Geodatabase)
        rast_d = arcpy.Describe(params[1].value)
        gdb_p = SSURGO_Convert_to_Geodatabase.main([
            params[0].valueAsText, # 0: input folder
            params[2].valueAsText, # output path
            params[3].value, # State
            params[4].value, # fiscal year
            '1.0', # gSSURGO template version
            # 14: module path
            os.path.dirname(SSURGO_Convert_to_Geodatabase.__file__) 
        ])
        if gdb_p:
            # import raster
            import import_raster_fgdb
            # reload(import_raster_fgdb)
            rast_n = import_raster_fgdb.main([
                gdb_p, # newly created RSS fgdb
                rast_d.catalogPath, # raster path
                params[3].value, # State
                params[4].value, # fiscal year
                # 14: module path
                os.path.dirname(SSURGO_Convert_to_Geodatabase.__file__) 
            ])
        else:
            arcpy.AddError(f"{gdb_p} was not successfully created")
        if rast_n:
            arcpy.AddMessage(f"\n{gdb_p} and {rast_n} were successfully created")
            # export package
            import export_package
            # reload(export_package)
            export_p = export_package.main([
                gdb_p, # newly created RSS fgdb
                params[0].valueAsText, # input folder
                params[3].value, # State
                params[4].value, # fiscal year
                rast_n # MURASTER name
            ])

            if export_p:
                arcpy.AddMessage(f"Package successfully exported to {export_p}")
            else:
                arcpy.AddError(f"Package unsuccessfully exported to {export_p}")
        else:
            arcpy.AddError(f"{rast_n} was not successfully created")
            
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return