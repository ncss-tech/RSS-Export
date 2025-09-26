#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One of three scripts called by the Create gSSURGO File Geodatabase tool
from the RSS SSURGO Export Tool arctoolbox
This tool creates a SSURGO folder structure and exports the raster 
as a geoTIFF.
Created on: 09/19/2024

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov

@modified Update 9/26/2025
    @by: Alexnder Stum
@version: 1.2.1

# --- Update 10/16/24 v 1.2.1
- Checks files found to tabs_req variable and messages out names of 
    extraneous files
# --- Update 10/16/24 v 1.2
- Add README to text file list

# ---
- The orginal tool this is base off of is from the ArcMap Desktop toolbox
    ArcGIS Desktop Build RSS gdb: Open Source Export. The tool creates
    SSURGO Download structured directory package. It copies the contents of
    tabular folder and creates a spatial sub directory and exports the raster
    in the RSS database as geoTIFF.

Numpy Dostring format

"""


import arcpy
import os
import sys
import traceback
import shutil


def pyErr(func: str = None) -> str:
    """When a python exception is raised, this funciton 
    formats the traceback message.

    Parameters
    ----------
    func : str
        The function that raised the python error exception

    Returns
    -------
    str
        Formatted python error message
    """

    try:
        etype, exc, tb = sys.exc_info()
        
        tbinfo = traceback.format_tb(tb)[0]
        tbinfo = '\t\n'.join(tbinfo.split(','))
        msgs = (f"PYTHON ERRORS:\nIn function: {func}"
                f"\nTraceback info:\n{tbinfo}\nError Info:\n\t{exc}")
        return msgs
    except:
        return "Error in pyErr method"


def arcpyErr(func: str) -> str:
    """When an arcpy by exception is raised, this function formats the 
    message returned by arcpy.

    Parameters
    ----------
    func : str
        The function that raised the arcpy error exception

    Returns
    -------
    str
        Formatted arcpy error message
    """

    try:
        etype, exc, tb = sys.exc_info()
        line = tb.tb_lineno
        msgs = (f"ArcPy ERRORS:\nIn function: {func}\non line: {line}"
                f"\n\t{arcpy.GetMessages(2)}\n")
        return msgs
    except:
        return "Error in arcpyErr method"
    

def main(args: list[str, str, str, int, str]) ->str:
    """This function packages the tif version of the RSS with the accompnaying
    textfiles in a SSURGO export folder structure.

    Parameters
    ----------
    args : list[str, str, str, int, str]
        The arguments sent from main:
            - Path of the newly created RSS FGDB
            - Path of the input tabular directory with textefiles
            - State abbreviation
            - Fiscal year of publication
            - Name of the MURASTER feature in the FGDB

    Returns
    -------
    str
        If successful, returns the path of the newly created export
        directory. Otherwise returns an empty string.
    """
    try:
        v = '1.2'
        arcpy.AddMessage(f"\nExport Package, {v = !s}")
        gdb_p = args[0] # input RSS gdb
        input_p = args[1] # input tablular folder
        st = args[2] # State
        fy = args[3] # fiscal year of publication
        raster_n = args[4] # MURASTER name

        # Make export directory
        out_p = os.path.dirname(gdb_p)
        export_p = f"{out_p}/RSS_{st}"
        os.mkdir(export_p)
        # Add spatial and taubular sub directories
        dirs = ['spatial', 'tabular']
        for d in dirs:
            os.mkdir(f"{export_p}/{d}")

        # Export MURASTER as tif
        out_r = f"{export_p}/spatial/{raster_n}.tif"
        arcpy.management.CopyRaster(
            f"{gdb_p}/{raster_n}", out_r, None, None, None, None, None, 
            "32_BIT_UNSIGNED"
        )

        # Copy over tabular textfiles from source
        tab_out = f"{export_p}/tabular"
        tabs_req = [
            'ccancov.txt', 'ccrpyd.txt', 'cdfeat.txt', 'cecoclas.txt',
            'ceplants.txt', 'cerosnac.txt', 'cfprod.txt', 'cfprodo.txt',
            'cgeomord.txt', 'chaashto.txt', 'chconsis.txt', 'chdsuffx.txt',
            'chfrags.txt', 'chorizon.txt', 'chpores.txt', 'chstr.txt',
            'chstrgrp.txt', 'chtexgrp.txt', 'chtexmod.txt', 'chtext.txt',
            'chtextur.txt', 'chunifie.txt', 'chydcrit.txt', 'cinterp.txt',
            'cmonth.txt', 'comp.txt', 'cpmat.txt', 'cpmatgrp.txt',
            'cpwndbrk.txt', 'crstrcts.txt', 'csfrags.txt', 'csmoist.txt',
            'csmorgc.txt', 'csmorhpp.txt', 'csmormr.txt', 'csmorss.txt',
            'cstemp.txt', 'ctext.txt', 'ctreestm.txt', 'ctxfmmin.txt',
            'ctxfmoth.txt', 'ctxmoicl.txt', 'distimd.txt', 'distlmd.txt',
            'distmd.txt', 'lareao.txt', 'legend.txt', 'ltext.txt',
            'mapunit.txt', 'msdomdet.txt', 'msdommas.txt', 'msidxdet.txt',
            'msidxmas.txt', 'msrsdet.txt', 'msrsmas.txt', 'mstab.txt',
            'mstabcol.txt', 'muaggatt.txt', 'muareao.txt', 'mucrpyd.txt',
            'mutext.txt', 'sacatlog.txt', 'sainterp.txt', 'sdvalgorithm.txt',
            'sdvattribute.txt', 'sdvfolder.txt', 'sdvfolderattribute.txt',
            'version.txt', 'README.txt'
        ]

        for f in os.scandir(input_p):
            if f.is_file() and f.name in tabs_req:
                shutil.copy(f.path, f"{tab_out}/{f.name}")
                tabs_req.remove(f.name)
            elif f.is_file():
                arcpy.AddWarning(
                    f"\tAn unexpected file found in {input_p}: {f.name}"
                )
            
        if tabs_req:
            arcpy.AddWarning(
                f"\tThe following text files were not copied over to {tab_out}:"
                )
            for t in tabs_req:
                arcpy.AddWarning(f"\t\t{t}")
        return export_p

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return ''
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return ''


if __name__ == '__main__':
    main(*sys.argv[1:])