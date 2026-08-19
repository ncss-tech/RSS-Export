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

@modified 8/17/2025
    @by: Alexnder Stum
@version: 2.0

# ---
- Modified to accomodate the inlcusion of PARASTER and SARASTER
- User now specifies whether it is the first year of publication
- If not 1st year, parameter for previous year's database so that metadata is
ported over
# ---
version 1.2, Updated 6/17/2025 - Alexander Stum
1) Added validator scirpt execution
2) Dynmically constrain fiscal year option

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
from datetime import datetime


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "RSS_SSURGO_Template_Tool"
        self.alias = 'RSS SSURGO Template Tools'

        # List of tool classes associated with this toolbox
        self.tools = [buildFGDB, validator]


class buildFGDB(object):
    def __init__(self):
        """This tool builds a Raster Soil Survey (RSS) package, including 
        open-source SSURGO package and File geodatabse built with a SSURGO
        template"""
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
            displayName="Tabular Folder of Exported SSURGO Text files",
            name="inputFolder",
            direction="Input",
            parameterType="Required",
            datatype="Folder"
        )]

        # parameter 1
        params.append(arcpy.Parameter(
            displayName="Output Folder",
            name="out_p",
            direction="Input",
            parameterType="Required",
            datatype="DEFolder"
        ))

        # parameter 2
        params.append(arcpy.Parameter(
            displayName="Select State",
            name="state",
            direction="Input",
            parameterType="Required",
            datatype="String"
        ))
        params[-1].filter.type = "ValueList"
        params[-1].filter.list = self.states

        # parameter 3
        params.append(arcpy.Parameter(
            displayName="Fiscal Year of Publication",
            name="fy",
            direction="Input",
            parameterType="Required",
            datatype="GPLong"
        ))
        params[-1].value = datetime.now().year + 1

        # parameter 4
        params.append(arcpy.Parameter(
            displayName="First year of publication?",
            name="pub1st",
            direction="Input",
            parameterType="Required",
            datatype="GPBoolean"
        ))
        params[-1].value = False

        # parameter 5
        params.append(arcpy.Parameter(
            displayName="Last FY's RSS FGDB",
            name="rss_db",
            direction="Input",
            parameterType="Optional",
            datatype="DEWorkspace"
        ))
        params[-1].filter.list = ["Local Database"]

        # parameter 6
        params.append(arcpy.Parameter(
            displayName="Updates to MURASTER (as .tif)",
            name="muraster",
            direction="Input",
            parameterType="Optional",
            datatype="DERasterDataset",
            category='Rasters',
            enabled=True,
            multiValue=True
        ))

        # parameter 7
        params.append(arcpy.Parameter(
            displayName="Updates to PARASTER (as .tif)",
            name="paraster",
            direction="Input",
            parameterType="Optional",
            datatype="DERasterDataset",
            category='Rasters',
            enabled=True,
            multiValue=True
        ))

        # parameter 8
        params.append(arcpy.Parameter(
            displayName="Updates to SARASTER (provided by NSSC)",
            name="saraster",
            direction="Input",
            parameterType="Optional",
            datatype="DERasterDataset",
            category='Rasters',
            enabled=True
        ))

        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        # if First year pub, disable RSS Database
        if params[4].value == True:
            params[5].enabled = False
        else:
            params[5].enabled = True

        params[6].enabled = True

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        for i in range(9):
            params[i].clearMessage()

        # MURASTER must have an mukey field
        if params[6].value:
            rasts = params[6].valueAsText.split(';')
            for rast in rasts:
                if rast[-4:] != '.tif':
                    params[6].setErrorMessage(f'{rast_d.name} is not a GeoTIFF')
                rast_d = arcpy.Describe(rast)
                rast_f = {
                    f.name for f in rast_d.fields if f.name.lower() == "mukey"
                }
                if not rast_f:
                    params[6].setWarningMessage(
                        f"{rast_d.name} does not have an mukey field"
                    )
        # PARASTER must have required fields
        if params[7].value:
            req_flds = {'UPROJID', 'SPATIALVER', 'AREATYPE'}
            rasts = params[7].valueAsText.split(';')
            for rast in rasts:
                if rast[-4:] != '.tif':
                    params[7].setErrorMessage(f'{rast_d.name} is not a GeoTIFF')
                rast_d = arcpy.Describe(rast)
                rast_f = {f.name for f in rast_d.fields if f.name in req_flds}
                if rast_f != req_flds:
                    params[7].setWarningMessage(
                        f"{rast_d.name} does not have "
                        f"{req_flds - rast_f} fields"
                    )
        # SARASTER must have required fields
        if params[8].value:
            req_flds = {'AREASYMBOL', 'SPATIALVER', 'AREATYPE'}
            rast_d = arcpy.Describe(params[8].value)
            rast_f = {f.name for f in rast_d.fields if f.name in req_flds}
            if rast_f != req_flds:
                params[8].setWarningMessage(
                    f"{rast_d.name} does not have {req_flds - rast_f} fields"
                )

        # Year must be + or - 1 year from current year
        cy = datetime.now().year
        if (fy := params[3].value):
            cyi = cy - 1
            cyf = cy + 1
            if (fy < cyi) or (fy > cyf):
                params[3].setErrorMessage(
                    f"Fiscal year is not range ({cyi} - {cyf})"
                )
        # first year publication require all three rasters
        if params[4].value == True and not params[6].value:
            params[6].setErrorMessage(
                'All three rasters must be provided '
                'the first year of publication'
            )
        if params[4].value == True and not params[7].value:
            params[7].setErrorMessage(
                'All three rasters must be provided '
                'the first year of publication'
            )
        if params[4].value == True and not params[8].value:
            params[8].setErrorMessage(
                'All three rasters must be provided '
                'the first year of publication'
            )

        # else make sure selected State abbreviaion in name of RSS db
        if params[4].value == False and params[2].value and params[5].value:
            st = params[2].valueAsText
            gdb = os.path.basename(params[5].valueAsText)
            if st not in gdb:
                params[2].setWarningMessage(
                    "RSS Database and selected state don't algin"
                )
                params[5].setWarningMessage(
                    "RSS Database and selected state don't algin"
                )
        
        return

    def execute(self, params, messages):
        """The source code of the tool."""
        import SSURGO_Convert_to_Geodatabase
        reload(SSURGO_Convert_to_Geodatabase)

        # Build gSSURGO templated FGDB
        if not params[4].value:
            prev_fgdb = params[5].valueAsText
        else:
            prev_fgdb = ''
        gdb_p = SSURGO_Convert_to_Geodatabase.main([
            params[0].valueAsText, # 0: input folder
            params[1].valueAsText, # output path
            params[2].value, # State
            params[3].value, # fiscal year
            prev_fgdb, # previous year's fgdb
            # 5: module path
            os.path.dirname(SSURGO_Convert_to_Geodatabase.__file__) 
        ])

        # import rasters
        if gdb_p:
            import import_raster_fgdb
            reload(import_raster_fgdb)
            if params[6].value:
                murasts = params[6].valueAsText.split(';')
            else:
                murasts = []
            if params[7].value:
                parasts = params[7].valueAsText.split(';')
            else:
                parasts = []
            if params[8].value:
                sarast = [params[8].valueAsText]
            else:
                sarast = []

            rast_n = import_raster_fgdb.main([
                gdb_p, # New FGDB path
                params[2].value, # State
                params[3].value, # fiscal year
                prev_fgdb, # previous year's fgdb
                murasts, # updates to MURASTER to be appended
                parasts, # updates to PARASTER to be appended
                sarast, # updated version of SARASTER
                # 7: module path
                os.path.dirname(SSURGO_Convert_to_Geodatabase.__file__) 
            ])
        else:
            arcpy.AddError(f"FGDB {gdb_p} was not successfully created")
            return
        if rast_n:
            arcpy.AddMessage(
                f"\n{gdb_p} and {rast_n} were successfully created"
            )
            # export package
            import export_package
            # reload(export_package)
            export_p = export_package.main([
                gdb_p, # newly created RSS fgdb
                params[0].valueAsText, # input folder
                params[2].value, # State
            ])
            if export_p:
                arcpy.AddMessage(f"Package successfully exported to {export_p}")
            else:
                arcpy.AddError(f"Failed to export Open Source package")
        else:
            arcpy.AddError(f"Error exporting rasters")
        # exit()
        arcpy.ResetProgressor()
            
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
    

class validator(object):
    def __init__(self):
        """This tool validates the contents of a RSS package"""
        self.label = "Validate RSS Datasets"
        self.description = (
            "Validate contents of of RSS packages"
        )
        self.states = [
            'AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL',
            'FM', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA',
            'MA', 'MD', 'ME', 'MH', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'MX',
            'NC', 'ND','NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR',
            'PA', 'PR', 'PW', 'RI', 'SC', 'SD', 'TN', 'TX', 'US', 'UT', 'VA',
            'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
        ]

    def getParameterInfo(self):
        """Define parameter definitions"""
        # parameter 0
        params = [arcpy.Parameter(
            displayName="Raster Soil Survey Directory",
            name="out_p",
            direction="Input",
            parameterType="Required",
            datatype="DEFolder",
            multiValue=True
        )]
        return params

    def updateParameters(self, params):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        return

    def updateMessages(self, params):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        params[0].clearMessage()
        msg = []
        if params[0].values:
            for st_dir in params[0].values:
                if os.path.basename(st_dir.value) not in self.states:
                    if not msg:
                        msg.append(
                            "RSS directories should be a state acronymn."
                        )
                    msg.append(
                        f"Not properly named: {st_dir.value}"
                    )
            if msg:
                msgs = '\n'.join(msg)  
                params[0].setErrorMessage(msgs)
        return

    def execute(self, params, messages):
        """The source code of the tool."""
        import RSS_validator
        reload(RSS_validator)

        states_comp = []
        states_incomp = []
        for i, st_dir in enumerate(params[0].values):
            st = os.path.basename(st_dir.value)
            state = RSS_validator.main([st_dir.value, st, i])
            if state in self.states:
                states_comp.append(state)
            else:
                states_incomp.append(state[:-1])
        if states_comp:
            arcpy.AddMessage(
                f"\n\nThese states were successfully validated {states_comp}"
            )
        if states_incomp:
            arcpy.AddError(
                f"\n\nThese states were not successfully validated {states_incomp}"
            )
            
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return