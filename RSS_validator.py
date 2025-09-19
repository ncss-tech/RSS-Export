#! /usr/bin/env python3
# # -*- coding: utf-8 -*-
"""
This scrip validates Raster Soil Survey (RSS) state packages
Created on Wed Sep  7 08:43:35 2022

@author: Charles.Ferguson
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov
@modified 6/18/2025
    @by: Alexnder Stum
@version: 1.2

# ---
version 1.2, Updated 09/18/2025 - Alexander Stum
- Can handle if raster band already renamed
- Added multiband check
# ---
version 1.1, Updated 06/18/2025 - Alexander Stum
- Cleaned up code formatting
- Added main function and set it up to be called one state at a time from the 
    Build Raster Soil Survery (RSS) Database.pyt
- Set up logic to clearly convey whether a state database passed validations
    by passing back a state abbrevion <ST> if successful or the abbreviation
    with the underscore if unsuccessful <ST_> back to the .pyt
- Added clarifying language in arcpy messages to user.
# ---
Updated 09/19/2024 - Alexander Stum
- change MapunitRaster to MURASTER
- Struck featdesc from list of gdb tables as RSS do not have special features
- Added version to the gdb tables, this is a new table to gSSURGO
- Changed time stamp check from six digit mmyyyy to four digit yyyy
- Struck out reference to README.txt from text file list. This gets saved
        outside of dataset.
- Tif Band name should now be MUKEY
- Added error messaging funcions to handle exceptions

"""

import os
import sys
import traceback
from datetime import datetime
from typing import TextIO

import arcpy
import pandas as pd


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
    

def insstatedir(dire: str, logf: TextIO) -> bool:
    """Function performs all the validation checks on the RSS package
    and writes out result to log file.

    Parameters
    ----------
    dire : str
        RSS package directory with the SSURGO open source package and FGDB
    logf : TextIO
        The log file where validation results are written

    Returns
    -------
    bool
        True if a validation error is raised, otherwise False
    """
    try:
        fail = 'HARD FAIL- '
        success = 'SUCCESS- '
        errored = False

        states = [
            'AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL',
            'FM', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA',
            'MA', 'MD', 'ME', 'MH', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'MX',
            'NC', 'ND','NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR',
            'PA', 'PR', 'PW', 'RI', 'SC', 'SD', 'TN', 'TX', 'US', 'UT', 'VA',
            'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
        ]

        textTables = {
            'ccancov.txt', 'ccrpyd.txt', 'cdfeat.txt', 'cecoclas.txt',
            'ceplants.txt', 'cerosnac.txt', 'cfprod.txt', 'cfprodo.txt',
            'cgeomord.txt', 'chaashto.txt', 'chconsis.txt', 'chdsuffx.txt',
            'chfrags.txt', 'chorizon.txt', 'chpores.txt', 'chstr.txt',
            'chstrgrp.txt', 'chtexgrp.txt', 'chtexmod.txt',
            'chtext.txt', 'chtextur.txt', 'chunifie.txt', 'chydcrit.txt',
            'cinterp.txt', 'cmonth.txt', 'comp.txt', 'cpmat.txt',
            'cpmatgrp.txt', 'cpwndbrk.txt', 'crstrcts.txt', 'csfrags.txt',
            'csmoist.txt', 'csmorgc.txt', 'csmorhpp.txt', 'csmormr.txt',
            'csmorss.txt', 'cstemp.txt', 'ctext.txt', 'ctreestm.txt',
            'ctxfmmin.txt', 'ctxfmoth.txt', 'ctxmoicl.txt', 'distimd.txt',
            'distlmd.txt', 'distmd.txt', 'lareao.txt', 'legend.txt',
            'ltext.txt', 'mapunit.txt', 'msdomdet.txt', 'msdommas.txt',
            'msidxdet.txt', 'msidxmas.txt', 'msrsdet.txt', 'msrsmas.txt',
            'mstab.txt', 'mstabcol.txt', 'muaggatt.txt', 'muareao.txt',
            'mucrpyd.txt', 'mutext.txt', 'sacatlog.txt', 'sainterp.txt', 
            'sdvalgorithm.txt', 'sdvattribute.txt', 'sdvfolder.txt',
            'sdvfolderattribute.txt', 'version.txt'
        }

        ssurgTables = {
            'chaashto', 'chconsistence', 'chdesgnsuffix', 'chfrags', 'chorizon', 
            'chpores', 'chstruct', 'chstructgrp','chtext', 'chtexture',
            'chtexturegrp', 'chtexturemod', 'chunified', 'cocanopycover',
            'cocropyld', 'codiagfeatures', 'coecoclass', 'coeplants',
            'coerosionacc', 'coforprod', 'coforprodo', 'cogeomordesc',
            'cohydriccriteria', 'cointerp', 'comonth', 'component', 'copm',
            'copmgrp', 'copwindbreak', 'corestrictions', 'cosoilmoist', 
            'cosoiltemp', 'cosurffrags', 'cosurfmorphgc', 'cosurfmorphhpp',
            'cosurfmorphmr', 'cosurfmorphss', 'cotaxfmmin', 'cotaxmoistcl',
            'cotext', 'cotreestomng', 'cotxfmother', 'distinterpmd',
            'distlegendmd', 'distmd', 'laoverlap', 'legend', 'legendtext',
            'mapunit', 'mdstatdomdet', 'mdstatdommas', 'mdstatidxdet',
            'mdstatidxmas', 'mdstatrshipdet', 'mdstatrshipmas', 'mdstattabcols',
            'mdstattabs', 'month', 'muaggatt', 'muaoverlap', 'mucropyld',
            'mutext', 'sacatalog', 'sainterp', 'sdvalgorithm', 'sdvattribute',
            'sdvfolder', 'sdvfolderattribute', 'version'
        }

        txtColNames = [
            'musym', 'muname', 'mukind', 'mustatus', 'muacres', 'mapunitlfw_l',
            'mapunitlfw_r', 'mapunitlfw_h', 'mapunitpfa_l', 'mapunitpfa_r',
            'mapunitpfa_h', 'farmlndcl', 'muhelcl', 'muwathelcl', 'muwndhelcl',
            'interpfocus', 'invesintens', 'iacornsr', 'nhiforsoigrp', 
            'nhspiagr', 'vtsepticsyscl', 'mucertstat', 'lkey', 'mukey'
        ]

        state = os.path.basename(dire)
        if state in states:
            # arcpy.AddMessage(f"in {state=}")
            msg2 = '\nValidataing RSS package for ' + state
            logf.write(msg2 + '\n')
            osp = f"{dire}/RSS_{state}"

            # Check for two major components: 
            #   1) The open source SSURGO directory
            #   2) File Geodatabase
            #   3) README.txt
            direchk = {'RSS_' + state,  'RSS_' + state + '.gdb', 'README.txt'}
            req = {'RSS_' + state,  'RSS_' + state + '.gdb'}
            contents = {f for f in os.listdir(dire) if '.zip' not in f}
            arcpy.AddMessage(contents)
            if not contents == direchk:
                if contents == req:
                    msg2b = (
                        f"\t{success} Top level state folder {state} is valid"
                    )
                    logf.write(msg2b + '\n')
                    arcpy.AddWarning("Missing README.txt from state directory")
                else:
                    msg2b = (
                        f"\t{fail} Top level state {state} is missing the open "
                        "source package, the FGDB, or has extraneous files"
                    )
                    logf.write(msg2b + '\n')
                    errored = True
                    return errored
            else:
                msg2b = f"\t{success} Top level state folder {state} is valid"
                logf.write(msg2b + '\n')
            
            # Check contents of open source SSURGO directory
            if not os.path.isdir(osp):
                msg3 = fail + 'open source SSURGO package not located\n'
                logf.write('\tOpen Source Package: ' + msg3)
                errored = True
                return errored
            else:
                osd = os.listdir(osp)
                osdreq = ['spatial', 'tabular']
                # Check presence of 
                #   1) spatial directory
                #   2) tabular directory
                if osd != osdreq:
                    msg3 = (
                        f"{fail} structure of open source SSURGO package "
                        "inconsistent. Missing spatial and/or tabular directory "
                        "or an extra file and/or directory was found\n"
                    )
                    logf.write('\tOpen Source Package: ' + msg3)
                    errored = True
                else:
                    osdspatial = os.path.join(dire, osp, 'spatial')
                    osdtabular = os.path.join(dire, osp, 'tabular')

                    # Check for raster in spatial directory
                    osrfiles = os.listdir(osdspatial)
                    osraster = [f for f in osrfiles if f.endswith('.tif')]
                    if not osraster:
                        msg3 = "A MURASTER_10m .tif file not found"
                        logf.write('\tOpen Source Package: ' + msg3)
                        errored = True
                    # More than one tif file found
                    elif len(osraster) != 1:
                        msg3 = (
                            f"{fail} unable to pinpoint the .tif MURASTER_10m."
                            f"Rasters found {osraster}"
                        )
                        logf.write('\tOpen Source Package: ' + msg3)
                        errored = True
                    # Does is start with standard name trunk
                    elif not osraster[0].startswith('MURASTER_10m'):
                        msg3 = fail + 'unable to locate the MURASTER_10m\n'
                        logf.write('\tOpen Source Package: ' + msg3)
                        errored = True
                    # needs three '_' to distinguish:
                    # prefix, resolution, state, year
                    elif osraster[0].count("_") != 3:
                        msg3 = (
                            f"{fail}Raster is not named correctly"
                            f" MURASTER_10m_{state}_<FY>.tif\n"
                        )
                        logf.write('\tOpen Source Package: ' + msg3)
                        errored = True
                    # Look for FY: yyyy
                    cy = datetime.now().year
                    cyi = cy - 1
                    cyf = cy + 1
                    try:

                        fy = int(osraster[0][-8:-4])
                        if (fy > cyi) or (fy < cyf):
                            msg3 = (
                                f"{success}located properly named tif "
                                f"raster {osraster[0]}\n"
                            )
                            logf.write('\tOpen Source Package: ' + msg3)
                        else:
                            msg3 = (f"{fail}unable validate Fiscal Year {fy} "
                                    f"for {osraster[0]}\n"
                            )
                            logf.write('\tOpen Source Package: ' + msg3)
                            errored = True
                    except:
                        msg3 = (f"{fail}unable validate date stamp {fy}"
                                f"for {osraster[0]}\n"
                            )
                        logf.write('\tOpen Source Package: ' + msg3)
                        errored = True
                    
                    # Raster metadata
                    meta = [f for f in osrfiles 
                            if f.endswith('.tif.xml')]
                    if len(meta) == 0:
                        msg4 = f"{fail}unable to locate a xml metadata file\n"
                        logf.write('\tOpen Source Package: ' + msg4)
                        errored = True
                    else:
                        msg4 = f"{success}found a .tif.xml metadata file\n"
                        logf.write('\tOpen Source Package: ' + msg4)

                    # Raster spatial reference
                    path = os.path.join(dire, osdspatial, osraster[0])
                    print(path)
                    desc = arcpy.Describe(path)
                    sr = desc.spatialReference
                    if sr.PCSCode != 0:
                        if sr.PCSCode not in [5070, 3338, 32161]:
                            msg5 = (
                                f"{fail} tif raster has unknown or "
                                "unsupported spatial reference\n"
                            )
                            logf.write('\tOpen Source Package: ' + msg5)
                            errored = True
                        else:
                            msg5 = (f"{success}{osraster[0]} has valid "
                                    "spatial reference\n"
                            )
                            logf.write('\tOpen Source Package: ' + msg5)
                    elif not sr.name == 'Hawaii_Albers_Equal_Area_Conic':
                        msg5 = (
                            f"{fail}{osraster[0]} has unknown or "
                            f"unsupported spatial reference: {sr.name}\n"
                        )
                        logf.write('\tOpen Source Package: ' + msg5)
                        errored = True
                    else:
                        msg5 = (
                            f"{success}{osraster[0]} has valid spatial "
                            f"reference: {sr.name}\n"
                        )
                        logf.write('\tOpen Source Package: ' + msg5)
                    
                    # Raster band info
                    # Band pixel data type
                    band = os.path.join(
                        dire, osdspatial, osraster[0], 'MUKEY'
                    )
                    bDepth = arcpy.Describe(band).pixelType
                    if bDepth == 'U32':
                        msg6 = (
                            f"{success}{osraster[0]} has unsigned "
                            "32 bit depth \n"
                        )
                        logf.write('\tOpen Source Package: ' + msg6)
                    else:
                        msg6 = (
                            f"{fail}{osraster[0]} DOES NOT have unsigned "
                            "32 bit depth \n"
                        )
                        logf.write('\tOpen Source Package: ' + msg6)
                        errored = True
                    # Band nodata
                    nodata = arcpy.Describe(band).noDataValue
                    if str(nodata) == '2147483647':
                        msg7 = (
                            f"{success}{osraster[0]} has the proper NoData"
                            f" value {str(nodata)}\n"
                        )
                        logf.write('\tOpen Source Package: ' + msg7)
                    else:
                        msg7 = (
                            f"{fail}{osraster[0]} has an INCORRECT NoData "
                            f"value of: {str(nodata)}\n"
                        )
                        logf.write('\tOpen Source Package: ' + msg7)
                        errored = True

                    # Verify the txt tables
                    ostables = set(os.listdir(osdtabular))
                    # Okay if present or missing README.txt
                    ostables.remove('README.txt')
                    if ostables == textTables:
                        msg8 = (success + os.path.basename(osp) + 
                                ' has the required txt tables \n'
                        )
                        logf.write('\tOpen Source Package: ' + msg8)

                        # Compare Raster and mapunit mukeys
                        df = pd.read_csv(
                            os.path.join(osdtabular, 'mapunit.txt'),
                            sep = '|',
                            names = txtColNames)
                        df['mukey'] = df['mukey'].astype('string')
                        txtkeys = set(df['mukey'].tolist())

                        with (arcpy.da.SearchCursor(
                            os.path.join(osdspatial, osraster[0]),
                            'MUKEY') 
                        as rows):
                            rasterkeys = {row for row, in rows}

                        if txtkeys == rasterkeys:
                            msg9 = (
                                f"{success} mukeys are identical in  "
                                f"{osraster[0]} and the mapunit txt file\n"
                            )
                            logf.write('\tOpen Source Package: ' + msg9)
                        else:
                            not_rastk = txtkeys - rasterkeys or "None"
                            not_txtk = rasterkeys - txtkeys or "None"
                            msg9 = (
                                f"{fail} mukeys ARE NOT identical in "
                                f"{osraster[0]} and the mapunit.txt file\n"
                                f"\tMUKEYs missing from raster: {not_rastk}\n\t"
                                f"MUKEYs missing from mapunit.txt: {not_txtk}\n"
                            )
                            logf.write('\tOpen Source Package: ' + msg9)
                            errored = True
                    # Identify mismatch in text files
                    else:
                        miss_txt = ostables - textTables
                        extra_txt = textTables - ostables
                        msg6 = (
                            f"{fail}{os.path.basename(osp)} is missing or "
                            "has extraneous txt tables\n"
                            f"\t\tMissing text files: {miss_txt}\n"
                            f"\t\tExtra text files found: {extra_txt}\n"
                        )
                        logf.write('\tOpen Source Package: ' + msg6)
                        errored = True

            # Check FGDB
            esridir = dire + os.sep + 'RSS_' + state + ".gdb"
            if not arcpy.Exists(esridir):
                msg12 = f'{fail}ESRI geodatabase {esridir} not located\n'
                logf.write('\tESRI GDB: ' + msg12)
                errored = True
                return errored
            else:
                # Check raster in FGDB
                arcpy.env.workspace = esridir
                esriraster = arcpy.ListRasters()
                # Only one raster should be present
                if not esriraster:
                    msg13 = fail + 'unable to find a MURASTER_10m'
                    logf.write('\tESRI GDB: ' + msg13)
                    errored = True
                elif len(esriraster) > 1:
                    msg13 = f'{fail}More than one raster found in {esridir}'
                    logf.write('\tESRI GDB: ' + msg13)
                    errored = True
                # Does is start with standard name trunk
                elif not esriraster[0].startswith('MURASTER_10m'):
                    msg13 = fail + 'unable to locate the MURASTER_10m\n'
                    logf.write('\tESRI GDB: ' + msg13)
                    errored = True
                # needs three '_' to distinguish:
                # prefix, resolution, state, year
                elif esriraster[0].count("_") != 3:
                    msg13 = (fail + 
                            'unable to locate a properly identified '
                            'MURASTER_10m_date-stamp tif raster\n')
                    logf.write('\tESRI GDB: ' + msg13)
                    errored = True
                # Look for FY: yyyy
                esriraster = esriraster[0]
                try:
                    fy = int(esriraster[-4:])
                    if (fy > cyi) or (fy < cyf):
                        msg13 = (
                                f"{success}located properly named tif "
                                f"raster {esriraster}\n"
                            )
                        logf.write('\tESRI GDB: ' + msg13)
                    else:
                        msg13 = (f"{fail}unable validate Fiscal Year {fy} "
                                f"for {esriraster}\n"
                        )
                        logf.write('\tESRI GDB: ' + msg13)
                        errored = True
                except:
                    msg13 = (f"{fail}unable validate date stamp {fy}"
                                f"for {esriraster}\n"
                            )
                    logf.write('\tESRI GDB: ' + msg13)
                    errored = True

                # Raster spatial reference
                murasdesc = arcpy.Describe(esriraster)
                sr = murasdesc.spatialReference
                if sr.PCSCode != 0:
                    if sr.PCSCode not in [5070, 3338, 32161]:
                        msg15 = (
                            f"{fail}gdb raster has unknown or unsupported "
                            "spatial reference\n"
                        )
                        logf.write('\tESRI GDB: ' + msg15)
                        errored = True
                    else:
                        msg15 = (
                            f"{success}{esriraster} has valid spatial "
                            "reference\n"
                        )
                        logf.write('\tESRI GDB: ' + msg15)
                elif not sr.name == 'Hawaii_Albers_Equal_Area_Conic':
                    msg15 = (
                        f"{fail}{esriraster} has unknown or "
                        "unsupported spatial reference\n"
                    )
                    logf.write('\tESRI GDB: ' + msg15)
                    errored = True
                else:
                    msg15 = (
                        f"{success}{esriraster} has valid "
                        "spatial reference\n"
                    )
                    logf.write('\tESRI GDB: ' + msg15)

                # Raster band info
                # Band pixel data type
                # Get band name
                rast = arcpy.Raster(esriraster)
                bands = rast.bandNames
                if len(bands) > 1:
                    msg15_5 = (f"{fail}gdb raster is multiband "
                               f"and has {len(bands)} bands")
                    logf.write('\tESRI GDB: ' + msg15_5)
                    del rast
                    raise
                band = bands[0]
                del rast

                esriband = os.path.join(esriraster, band)
                esriDepth = arcpy.Describe(esriband).pixelType
                if esriDepth == 'U32':
                    msg16 = (
                        f"{success}{esriraster} has unsigned "
                        "32 bit depth \n"
                    )
                    logf.write('\tESRI GDB: ' + msg16)
                else:
                    msg16 = (
                        f"{fail}{esriraster} DOES NOT have unsigned "
                        "32 bit depth \n"
                    )
                    logf.write('\tESRI GDB: ' + msg16)
                    errored = True

                # Validate FGDB
                # only the right txt tables
                fgdb = f"{dire}/RSS_{state}.gdb"
                arcpy.env.workspace = fgdb
                gdbtables = set(arcpy.ListTables())
                if gdbtables == ssurgTables:
                    msg18 = (
                        f"{success}{fgdb} has the "
                        "required gdb tables \n"
                    )
                    logf.write('\tESRI GDB: ' + msg18)

                    # Compuare raster and mapunit mukeys
                    with(
                        arcpy.da.SearchCursor(fgdb + '/mapunit', 'mukey') 
                        as rows):
                        gdbkeys = {row for row, in rows}

                    with(
                        arcpy.da.SearchCursor(f"{fgdb}/{esriraster}", 'MUKEY')
                        as rows):
                        rasterkeys = {row for row, in rows}

                    if gdbkeys == rasterkeys:
                        msg20 = (
                            f"{success}mukeys are identical in "
                            f"{esriraster} and the mapunit gdb table\n"
                        )
                        logf.write('\tESRI GDB: ' + msg20)
                    else:
                        not_rastk = gdbkeys - rasterkeys or "None"
                        not_gdbk = rasterkeys - gdbkeys or "None"
                        msg20 = (
                            f"{fail} mukeys ARE NOT identical in "
                            f"{esriraster} and the mapunit table\n"
                            f"\t\tMUKEYs missing from raster: {not_rastk}\n\t\t"
                            f"MUKEYs missing from mapunit table: {not_gdbk}\n"
                        )
                        logf.write('\tESRI GDB: ' + msg20)
                        errored = True
                # Identify mismatch in FGDB tables
                else:
                    miss_gdb = ssurgTables - gdbtables
                    extra_tabs = gdbtables - ssurgTables
                    msg19 = (
                        f"{fail}{os.path.basename(osp)} is missing or "
                        "has extraneous txt tables\n"
                        f"\t\tMissing files within FGDB: {miss_gdb}\n"
                        f"\t\tExtra files found in FGDB: {extra_tabs}\n"
                    )
                    logf.write('\tESRI GDB: ' + msg19)
                    errored = True
            logf.write('\n')
        return errored
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def main(args: list[str, str, int]) -> str:
    """Main function meant to be called for one RSS state package at a time

    Parameters
    ----------
    args : list[str, str, int]
        This list should contain
        1) File path directory of the RSS state package
        2) State abbreviation
        3) Indicate whether this is the first state being run from the .pyt

    Returns
    -------
    str
        state abbrevion <ST> if successful or the abbreviation
    with the underscore if unsuccessful <ST_>
    """
    try:
        v = '1.2'
        rss_dir = args[0]
        st = args[1]
        i = args[2]
        if not i:
            arcpy.AddMessage(f"Validate RSS Datasets: {v}\n")
        
        user = os.environ.get('USERNAME')
        now = datetime.now()
        now_str = now.strftime("%d/%m/%Y %H:%M:%S")

        dir_contents = os.listdir(rss_dir)
        # Is it Double bagged?
        double_bagged = [d for d in dir_contents 
                         if os.path.isdir(f"{rss_dir}/{d}") and (d == st)]
        if double_bagged:
            log = os.path.join(rss_dir, f'log_{st}.log')
            rss_dir = os.path.join(rss_dir, st)
        else:
            log = os.path.join(os.path.dirname(rss_dir), f'log_{st}.log')
        # open log file
        with open(log, 'w') as logf:
            logf.write("User: " + user + "\n")
            logf.write("Time: " + now_str + "\n\n\n")

            msg1 = f"Validation report for {st} ({rss_dir}):"
            logf.write(msg1)
            arcpy.AddMessage(f"\nValidating {st} ({rss_dir})")

            errored = insstatedir(rss_dir, logf)
            if errored:
                arcpy.AddError(
                    f"\tA validation error was found, see {log}"
                )
                st = st + '_'

        logf.close()
        return st

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return st + '_'
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return st + '_'


if __name__ == '__main__':
    main(sys.argv[1:])