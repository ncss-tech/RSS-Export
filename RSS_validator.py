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
@modified 7/20/2026
    @by: Alexnder Stum
@version: 2.0

# --- 
version 2.0, Updated 7/20/2026 - Alexander Stum
- Apply checks to SARASTER as are applied to MURASTER
- Compare to LAO table
- Check that MURASTER and PARASTER perfectly overlap
- Check that MURASTER and PARASTER are within SARASTER
- accept 0 as nodata value
# ---
version 1.2.1, Updated 9/26/2025 - Alexander Stum
- Messaging about missing or extraneous messages was swapped
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
v = '2.0'

import os
import sys
import traceback
from datetime import datetime
from typing import TextIO

import arcpy
import pandas as pd


def pyErr(func: str = '') -> str:
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
    

def insstatedir(base_p: str, logf: TextIO) -> bool:
    """Function performs all the validation checks on the RSS package
    and writes out result to log file.

    Parameters
    ----------
    base_p : str
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

        mu_cols = [
            'musym', 'muname', 'mukind', 'mustatus', 'muacres', 'mapunitlfw_l',
            'mapunitlfw_r', 'mapunitlfw_h', 'mapunitpfa_l', 'mapunitpfa_r',
            'mapunitpfa_h', 'farmlndcl', 'muhelcl', 'muwathelcl', 'muwndhelcl',
            'interpfocus', 'invesintens', 'iacornsr', 'nhiforsoigrp', 
            'nhspiagr', 'vtsepticsyscl', 'mucertstat', 'lkey', 'mukey'
        ]

        lao_cols = [
            'areatypename', 'areasymbol', 'areaname', 'areaovacres', 'lkey',
            'lareaovkey'
        ]

        l_cols = [
            'areatypename', 'areasymbol', 'areaname', 'areaacres', 'mlraoffice',
            'legenddesc', 'ssastatus', 'mouagncyresp', 'projectscale', 
            'cordate', 'ssurgoarchived', 'legendsuituse', 'legendcertstat', 
            'lkey'
        ]

        state = os.path.basename(base_p)
        fgdb = f"{base_p}/RSS_{state}.gdb"

        # Get State, FY from Version table
        with arcpy.da.SearchCursor(
            fgdb + '/version', 'name', where_clause="type = 'Edition'"
        ) as sCur:
            rss_ed = sCur.next()[0]
        st = rss_ed[:2]
        fy = rss_ed[3:]

        if state in states:
            arcpy.AddMessage(f"in {state=}")
            msg2 = '\nValidataing RSS package for ' + state
            if state != st:
                msg2 += ('\n\t\tDiscrepancy between database name '
                'and version table')

            logf.write(msg2)
            osrc_p = f"{base_p}/RSS_{state}"
            # Check for two major components: 
            #   1) The open source SSURGO directory
            #   2) File Geodatabase
            #   3) README.txt
            direchk = {'RSS_' + state,  f"RSS_{state}.gdb", 'README.txt'}
            req = {'RSS_' + state,  f"RSS_{state}.gdb"}
            contents = {f for f in os.listdir(base_p) if '.zip' not in f}
            if not contents == direchk:
                if contents == req:
                    msg2b = (
                        f"\n\t{success} Top level state folder {state} "
                        "is valid"
                    )
                    logf.write(msg2b)
                #     arcpy.AddWarning("" \
                #     "\tMissing README.txt from state directory"
                # )
                else:
                    msg2b = (
                        f"\n\t{fail} Top level state {state} is missing the "
                        "open source package, the FGDB, or has extraneous files"
                    )
                    logf.write(msg2b)
                    errored = True
                    return errored
            else:
                msg2b = f"\n\t\t{success} Top level directory: NOMINAL"
                logf.write(msg2b)

            # Check contents of open source SSURGO directory
            msg3 = ''
            if not os.path.isdir(osrc_p):
                msg3 = fail + f'\n\t\tMissing the open source director {osrc_p}'
                logf.write('\n\tOpen Source Package: ' + msg3)
                errored = True
                return errored
            else:
                osd = os.listdir(osrc_p)
                osdreq = ['spatial', 'tabular']
                # Check presence of 
                #   1) spatial directory
                #   2) tabular directory
                if osd != osdreq:
                    msg3 = (
                        f"{fail} structure of open source SSURGO package "
                        "inconsistent. Missing spatial and/or tabular directory"
                        " or an extra file and/or directory was found\n"
                    )
                    logf.write('\tOpen Source Package: ' + msg3)
                    errored = True
                    arcpy.AddError('--Validation incomplete--\n' + msg3)
                    return errored
                else:
                    # Naming Conventions
                    osrc_spatial = osrc_p + "/spatial"
                    osrc_tabular = osrc_p + "/tabular"
                    # Check for raster in spatial directory
                    arcpy.env.workspace = osrc_spatial
                        # MURASTER
                    mu_rast = f'MURASTER_10m_{st}_{fy}.tif'
                    if not arcpy.ListRasters(mu_rast):
                        msg3 += f'\n\t\t\tMissing {mu_rast} in {osrc_spatial}'
                        # PARASTER
                    pa_rast = f'PARASTER_10m_{st}_{fy}.tif'
                    if not arcpy.ListRasters(pa_rast):
                        msg3 += f'\n\t\t\tMissing {pa_rast} in {osrc_spatial}'
                        # SARASTER
                    sa_rast = f'SARASTER_10m_{st}_{fy}.tif'
                    if not arcpy.ListRasters(sa_rast):
                        msg3 += f'\n\t\t\tMissing {sa_rast} in {osrc_spatial}'

                    stem = "Raster Naming Convetion:" 
                    if msg3:
                        msg3 = '\n\tOpen Source Package: ' \
                            f'\n\t\t{stem:<{25}}FAILED' + msg3
                        errored = True
                    else:
                        msg3 = '\n\tOpen Source Package: ' \
                            f'\n\t\t{stem:<{25}}NOMINAL' + msg3
                    logf.write(msg3)
                    
                    # Raster metadata
                    msg3b = ''
                    sp_files = os.listdir(osrc_spatial)

                    if mu_rast + '.xml' not in sp_files:
                        msg3b = f'\n\t\t\tMissing metadata file for {mu_rast}'
                    if pa_rast + '.xml' not in sp_files:
                        msg3b += f'\n\t\t\tMissing metadata file for {pa_rast}'
                    if sa_rast + '.xml' not in sp_files:
                        msg3b += f'\n\t\t\tMissing metadata file for {sa_rast}'
                    stem = 'Metadata: '
                    if msg3b:
                        msg3b = f'\n\t\t{stem:<{25}}FAILED' + msg3b
                        errored = True
                    else:
                        msg3b = f'\n\t\t{stem:<{25}}NOMINAL'
                    logf.write(msg3b)

                    # Raster spatial reference
                    # will need to be updated once RSS are 
                    # published beyond CONUS
                    msg4 = '' # Spatial Reference
                    msg5 = '' # band count
                    msg5a = '' # data type
                    msg5b = '' # no data value
                    msg5c = '' # Band name
  
                    rast_d = arcpy.Describe(f"{osrc_spatial}/{mu_rast}")
                    sr = rast_d.spatialReference
                    if sr.PCSCode != 5070:
                        msg4 += (
                            f"\n\t\t\t{mu_rast} has incorrect coordinate system"
                        )
                    band_d = rast_d.children
                    if len(band_d) != 1:
                        msg5 += f"\n\t\t\t{mu_rast} has more than one band"
                    band_d = band_d[0]
                    if band_d.pixelType != 'U32':
                        msg5a += (
                            f"\n\t\t\t{mu_rast} pixel data type is not "
                            "Unsigned 32-bit Integer"
                        )
                    if band_d.noDataValue != 0:
                        msg5b += (
                            f"\n\t\t\t{mu_rast} NoData Value is not 0"
                        )
                    if band_d.name != 'MUKEY':
                        msg5c += (
                            f"\n\t\t\t{mu_rast} band is not named 'MUKEY'"
                        )
                    
                    rast_d = arcpy.Describe(f"{osrc_spatial}/{pa_rast}")
                    sr = rast_d.spatialReference
                    if sr.PCSCode != 5070:
                        msg4 += (
                            f"\n\t\t\t{pa_rast} has incorrect coordinate system"
                        )
                    band_d = rast_d.children
                    if len(band_d) != 1:
                        msg5 += f"\n\t\t\t{pa_rast} has more than one band"
                    band_d = band_d[0]
                    if band_d.pixelType != 'U32':
                        msg5a += (
                            f"\n\t\t\t{pa_rast} pixel data type is not "
                            "Unsigned 32-bit Integer"
                        )
                    if band_d.noDataValue != 0:
                        msg5b += (
                            f"\n\t\t\t{pa_rast} NoData Value is not 0"
                        )
                    if band_d.name != 'LAOKEY':
                        msg5c += (
                            f"\n\t\t\t{pa_rast} band is not named 'LAOKEY'"
                        )

                    rast_d = arcpy.Describe(f"{osrc_spatial}/{sa_rast}")
                    sr = rast_d.spatialReference
                    if sr.PCSCode != 5070:
                        msg4 += (
                            f"\n\t\t\t{sa_rast} has incorrect coordinate system"
                        )
                    band_d = rast_d.children
                    if len(band_d) != 1:
                        msg5 += f"\n\t\t\t{sa_rast} has more than one band"
                    band_d = band_d[0]
                    if band_d.pixelType != 'U32':
                        msg5a += (
                            f"\n\t\t\t{sa_rast} pixel data type is not "
                            "Unsigned 32-bit Integer"
                        )
                    if band_d.noDataValue != 0:
                        msg5b += (
                            f"\n\t\t\t{sa_rast} NoData Value is not 0"
                        )
                    if band_d.name != 'LKEY':
                        msg5c += (
                            f"\n\t\t\t{sa_rast} band is not named 'LKEY'"
                        )
                    cs = "Coordinate System: "
                    bc = "Band Count: "
                    dt = "Data Type: "
                    nd = "NoData Value: "
                    bn = "Band Name: "
                    if msg4:
                        msg4 = f'\n\t\t{cs:<{25}}FAILED' + msg4
                        errored = True
                    else:
                        msg4 = f'\n\t\t{cs:<{25}}NOMINAL'
                    logf.write(msg4)
                    if msg5:
                        msg5 = f'\n\t\t{bc:<{25}}FAILED' + msg5
                        errored = True
                    else:
                        msg5 = f'\n\t\t{bc:<{25}}NOMINAL'
                    logf.write(msg5)
                    if msg5a:
                        msg5a = f'\n\t\t{dt:<{25}}FAILED' + msg5a
                        errored = True
                    else:
                        msg5a = f'\n\t\t{dt:<{25}}NOMINAL'
                    logf.write(msg5a)
                    if msg5b:
                        msg5b = f'\n\t\t{nd:<{25}}FAILED' + msg5b
                        errored = True
                    else:
                        msg5b = f'\n\t\t{nd:<{25}}NOMINAL'
                    logf.write(msg5b)
                    if msg5c:
                        msg5c = f'\n\t\t{bn:<{25}}FAILED' + msg5c
                        errored = True
                    else:
                        msg5c = f'\n\t\t{bn:<{25}}NOMINAL'
                    logf.write(msg5c)

                    # Verify the txt tables
                    ostables = set(os.listdir(osrc_tabular))
                    # Okay if present or missing README.txt
                    ostables.remove('README.txt')
                    msg8 = ''
                    if missing := textTables - ostables:
                        msg8 += ("\n\t\t\tMissing text tables: "
                                f"{', '.join(missing)}"
                        )
                    if extra := ostables - textTables:
                        msg8 += ("\n\t\t\tExtra text tables: "
                                f"{', '.join(extra)}"
                        )
                    stem = "Text Files: "
                    if msg8:
                        msg8 = f'\n\t\t{stem:<{25}}FAILED ' + msg8
                        errored = True
                    else:
                        msg8 = f'\n\t\t{stem:<{25}}NOMINAL'
                    logf.write(msg8)

                    # Compare Raster and mapunit mukeys in mapunit.txt
                    msg9 = ''
                    df = pd.read_csv(
                        os.path.join(osrc_tabular, 'mapunit.txt'),
                        sep = '|',
                        names = mu_cols)
                    df['mukey'] = df['mukey'].astype(int)
                    txtkeys = set(df['mukey'].tolist())
                    mu_rast_p = f"{osrc_spatial}/{mu_rast}"
                    with arcpy.da.SearchCursor(mu_rast_p, 'Value') as rows:
                        rasterkeys = {mk for mk, in rows}

                    if missing := txtkeys - rasterkeys:
                        msg9 += ("\n\t\t\tMissing MUKEY's: "
                                f"{', '.join(map(str, missing))}"
                        )
                    if extra := rasterkeys - txtkeys:
                        msg9 += ("\n\t\t\tExtra MUKEYS: "
                                f"{', '.join(map(str, extra))}"
                        )

                    df = pd.read_csv(
                        os.path.join(osrc_tabular, 'lareao.txt'),
                        sep = '|',
                        names = lao_cols)
                    sub_df = df[df['areatypename'] == 'Raster Soil Survey Project']
                    keys = sub_df['lareaovkey'].astype(int)
                    txtkeys = set(keys.tolist())
                    pa_rast_p = f"{osrc_spatial}/{pa_rast}"
                    with arcpy.da.SearchCursor(pa_rast_p, 'Value') as rows:
                        rasterkeys = {lk for lk, in rows}
                    if missing := txtkeys - rasterkeys:
                        msg9 += ("\n\t\t\tMissing LAOKEY's: "
                                f"{', '.join(map(str, missing))}"
                        )
                    if extra := rasterkeys - txtkeys:
                        msg9 += ("\n\t\t\tExtra LAOKEYS: "
                                f"{', '.join(map(str, extra))}"
                        )

                    df = pd.read_csv(
                        os.path.join(osrc_tabular, 'legend.txt'),
                        sep = '|',
                        names = l_cols)
                    df['lkey'] = df['lkey'].astype(int)
                    txtkeys = set(df['lkey'].tolist())
                    sa_rast_p = f"{osrc_spatial}/{sa_rast}"
                    with arcpy.da.SearchCursor(sa_rast_p, 'Value') as rows:
                        rasterkeys = {lk for lk, in rows}
                    if missing := txtkeys - rasterkeys:
                        msg9 += ("\n\t\t\tMissing LKEY's: "
                                f"{', '.join(map(str, missing))}"
                        )
                    if extra := rasterkeys - txtkeys:
                        msg9 += ("\n\t\t\tExtra LKEYS: "
                                f"{', '.join(map(str, extra))}"
                        )
                    stem = "Raster Keys: "
                    if msg9:
                        msg9 = f'\n\t\t{stem:<{25}}FAILED ' + msg9
                        errored = True
                    else:
                        msg9 = f'\n\t\t{stem:<{25}}NOMINAL'

                    logf.write(msg9)

            # Check File Geodatabase
            arcpy.env.workspace = fgdb
                # MURASTER
            msg3 = ''
            mu_rast = f'MURASTER_10m_{st}_{fy}'
            if not arcpy.ListRasters(mu_rast):
                msg3 += f'\n\t\t\tMissing {mu_rast} in {fgdb}'
                # PARASTER
            pa_rast = f'PARASTER_10m_{st}_{fy}'
            if not arcpy.ListRasters(pa_rast):
                msg3 += f'\n\t\t\tMissing {pa_rast} in {fgdb}'
                # SARASTER
            sa_rast = f'SARASTER_10m_{st}_{fy}'
            if not arcpy.ListRasters(sa_rast):
                msg3 += f'\n\t\t\tMissing {sa_rast} in {fgdb}'
            stem = 'Naming Convention: '
            if msg3:
                msg3 = '\n\tFile Geodatabase: ' \
                    f'\n\t\t{stem:<{25}}FAILED' + msg3
                errored = True
            else:
                msg3 = '\n\tFile Geodatabase: ' \
                    f'\n\t\t{stem:<{25}}NOMINAL' + msg3
            logf.write(msg3)

            # Raster spatial reference
            # will need to be updated once RSS are 
            # published beyond CONUS
            msg4 = '' # Spatial Reference
            msg5 = '' # band count
            msg5a = '' # data type
            msg5b = '' # no data value
            msg5c = '' # Band name

            # FGDB rasters don't seem to hold the NoData value
            rast_d = arcpy.Describe(f"{fgdb}/{mu_rast}")
            sr = rast_d.spatialReference
            if sr.PCSCode != 5070:
                msg4 += (
                    f"\n\t\t\t{mu_rast} has incorrect coordinate system"
                )
            band_d = rast_d.children
            if len(band_d) != 1:
                msg5 += f"\n\t\t\t{mu_rast} has more than one band"
            band_d = band_d[0]
            if band_d.pixelType != 'U32':
                msg5a += (
                    f"\n\t\t\t{mu_rast} pixel data type is not "
                    "Unsigned 32-bit Integer"
                )
            # if band_d.noDataValue != 0:
            #     msg5b += (
            #         f"\n\t\t\t{mu_rast} NoData Value is not 0"
            #     )
            if band_d.name != 'MUKEY':
                msg5c += (
                    f"\n\t\t\t{mu_rast} band is not named 'MUKEY'"
                )
            
            rast_d = arcpy.Describe(f"{fgdb}/{pa_rast}")
            sr = rast_d.spatialReference
            if sr.PCSCode != 5070:
                msg4 += (
                    f"\n\t\t\t{pa_rast} has incorrect coordinate system"
                )
            band_d = rast_d.children
            if len(band_d) != 1:
                msg5 += f"\n\t\t\t{pa_rast} has more than one band"
            band_d = band_d[0]
            if band_d.pixelType != 'U32':
                msg5a += (
                    f"\n\t\t\t{pa_rast} pixel data type is not "
                    "Unsigned 32-bit Integer"
                )
            # if band_d.noDataValue != 0:
            #     msg5b += (
            #         f"\n\t\t\t{pa_rast} NoData Value is not 0"
            #     )
            if band_d.name != 'LAOKEY':
                msg5c += (
                    f"\n\t\t\t{pa_rast} band is not named 'LAOKEY'"
                )

            rast_d = arcpy.Describe(f"{fgdb}/{sa_rast}")
            sr = rast_d.spatialReference
            if sr.PCSCode != 5070:
                msg4 += (
                    f"\n\t\t\t{sa_rast} has incorrect coordinate system"
                )
            band_d = rast_d.children
            if len(band_d) != 1:
                msg5 += f"\n\t\t\t{sa_rast} has more than one band"
            band_d = band_d[0]
            if band_d.pixelType != 'U32':
                msg5a += (
                    f"\n\t\t\t{sa_rast} pixel data type is not "
                    "Unsigned 32-bit Integer"
                )
            # if band_d.noDataValue != 0:
            #     msg5b += (
            #         f"\n\t\t\t{sa_rast} NoData Value is not 0"
            #     )
            if band_d.name != 'LKEY':
                msg5c += (
                    f"\n\t\t\t{sa_rast} band is not named 'LKEY'"
                )

            if msg4:
                msg4 = f'\n\t\t{cs:<{25}}FAILED' + msg4
                errored = True
            else:
                msg4 = f'\n\t\t{cs:<{25}}NOMINAL'
            logf.write(msg4)
            if msg5:
                msg5 = f'\n\t\t{bc:<{25}}FAILED' + msg5
                errored = True
            else:
                msg5 = f'\n\t\t{bc:<{25}}NOMINAL'
            logf.write(msg5)
            if msg5a:
                msg5a = f'\n\t\t{dt:<{25}}FAILED' + msg5a
                errored = True
            else:
                msg5a = f'\n\t\t{dt:<{25}}NOMINAL'
            logf.write(msg5a)
            # if msg5b:
            #     msg5b = f'\n\t\t{nd:<{25}}FAILED' + msg5b
            #     errored = True
            # else:
            #     msg5b = f'\n\t\t{nd:<{25}}NOMINAL'
            # logf.write(msg5b)
            if msg5c:
                msg5c = f'\n\t\t{bn:<{25}}FAILED' + msg5c
                errored = True
            else:
                msg5c = f'\n\t\t{bn:<{25}}NOMINAL'
            logf.write(msg5c)

            # Verify the FGDB tables
            fgb_tabs = set(arcpy.ListTables())
            msg8 = ''
            if missing := ssurgTables - fgb_tabs:
                msg8 += ("\n\t\t\tMissing text tables: "
                        f"{', '.join(missing)}"
                )
            if extra := fgb_tabs - ssurgTables:
                msg8 += ("\n\t\t\tExtra text tables: "
                        f"{', '.join(extra)}"
                )
            stem = "Text Tables: "
            if msg8:
                msg8 = f'\n\t\t{stem:<{25}}FAILED ' + msg8
                errored = True
            else:
                msg8 = f'\n\t\t{stem:<{25}}NOMINAL'
            logf.write(msg8)

            # Compare Raster and mapunit mukeys in mapunit.txt
            msg9 = ''
            with arcpy.da.SearchCursor(fgdb + '/mapunit', 'mukey') as rows:
                tabkeys = {int(mk) for mk, in rows}
            mu_rast_p = f"{fgdb}/{mu_rast}"
            with arcpy.da.SearchCursor(mu_rast_p, 'Value') as rows:
                rasterkeys = {mk for mk, in rows}

            if missing := tabkeys - rasterkeys:
                msg9 += ("\n\t\t\tMissing MUKEY's: "
                        f"{', '.join(map(str, missing))}"
                )
            if extra := rasterkeys - tabkeys:
                msg9 += ("\n\t\t\tExtra MUKEYS: "
                        f"{', '.join(map(str, extra))}"
                )

            with arcpy.da.SearchCursor(
                fgdb + '/laoverlap', 'lareaovkey', 
                where_clause="areatypename='Raster Soil Survey Project'"
                ) as rows:
                tabkeys = {int(lk) for lk, in rows}
            pa_rast_p = f"{fgdb}/{pa_rast}"
            with arcpy.da.SearchCursor(pa_rast_p, 'Value') as rows:
                rasterkeys = {lk for lk, in rows}
            if missing := tabkeys - rasterkeys:
                msg9 += ("\n\t\t\tMissing LAOKEY's: "
                        f"{', '.join(map(str, missing))}"
                )
            if extra := rasterkeys - tabkeys:
                msg9 += ("\n\t\t\tExtra LAOKEYS: "
                        f"{', '.join(map(str, extra))}"
                )

            with arcpy.da.SearchCursor(fgdb + '/legend', 'lkey') as rows:
                tabkeys = {int(lk) for lk, in rows}
            sa_rast_p = f"{fgdb}/{sa_rast}"
            with arcpy.da.SearchCursor(sa_rast_p, 'Value') as rows:
                rasterkeys = {lk for lk, in rows}
            if missing := tabkeys - rasterkeys:
                msg9 += ("\n\t\t\tMissing LKEY's: "
                        f"{', '.join(map(str, missing))}"
                )
            if extra := rasterkeys - tabkeys:
                msg9 += ("\n\t\t\tExtra LKEYS: "
                        f"{', '.join(map(str, extra))}"
                )
            stem = "Raster Keys: "
            if msg9:
                msg9 = f'\n\t\t{stem:<{25}}FAILED ' + msg9
                errored = True
            else:
                msg9 = f'\n\t\t{stem:<{25}}NOMINAL'
            logf.write(msg9)

            # Raster topology
            # raster calculator and isnull and union of extent
            arcpy.env.workspace = osrc_spatial
            msg10 = ''
            p_rast = arcpy.Raster(f"{osrc_spatial}/{pa_rast}.tif")
            m_rast = arcpy.Raster(f"{osrc_spatial}/{mu_rast}.tif")
            s_rast = arcpy.Raster(f"{osrc_spatial}/{sa_rast}.tif")
            
            try:
                out_r = osrc_spatial + "/PARASTER_exceed_MU.tif"
                with arcpy.EnvManager(extent=p_rast.extent):
                    pa_within = arcpy.sa.IsNull(m_rast) & p_rast
                pa_within.save(out_r)
                del pa_within
                gm = arcpy.management.GetRasterProperties(
                        in_raster=out_r,
                        property_type="MAXIMUM"
                    )
                if int(gm[0]):
                    msg10 += ("\n\t\t\tNot all pixels of PARASTER "
                    f"covered by MURASTER, see {out_r}")
                else:
                    arcpy.Delete_management(out_r)

                out_r = osrc_spatial + "/MURASTER_exceed_PA.tif"
                with arcpy.EnvManager(extent=m_rast.extent):
                    mu_within = arcpy.sa.IsNull(p_rast) & m_rast
                mu_within.save(out_r)
                del mu_within
                gm = arcpy.management.GetRasterProperties(
                        in_raster=out_r,
                        property_type="MAXIMUM"
                    )
                if int(gm[0]):
                    msg10 += ("\n\t\t\tNot all pixels of MURASTER "
                    f"covered by PARASTER, see {out_r}")
                else:
                    arcpy.Delete_management(out_r)  
            except:
                etype, exc, tb = sys.exc_info()
                if "Invalid output extent" in str(exc):
                    msg10 += "\n\t\t\tPARASTER and MURASTER do not overlap"
                else:
                    func = sys._getframe().f_code.co_name
                    arcpy.AddError(pyErr(func))
                    
            try:
                out_r = osrc_spatial + "/PARASTER_exceed_SA.tif"
                with arcpy.EnvManager(extent=p_rast.extent):
                    pa_within = arcpy.sa.IsNull(s_rast) & p_rast
                pa_within.save(out_r)
                del pa_within
                gm = arcpy.management.GetRasterProperties(
                        in_raster=out_r,
                        property_type="MAXIMUM"
                    )
                if int(gm[0]):
                    msg10 += ("\n\t\t\tNot all pixels of PARASTER "
                    f"covered by SARASTER, see {out_r}")
                else:
                    arcpy.Delete_management(out_r) 
            except:
                etype, exc, tb = sys.exc_info()
                if "Invalid output extent" in str(exc):
                    msg10 += "\n\t\t\tPARASTER and SARASTER do not overlap"
                else:
                    func = sys._getframe().f_code.co_name
                    arcpy.AddError(pyErr(func))

            try:
                out_r = osrc_spatial + "/MURASTER_exceed_SA.tif"
                with arcpy.EnvManager(extent=m_rast.extent):
                    mu_within = arcpy.sa.IsNull(s_rast) & m_rast
                mu_within.save(out_r)
                del mu_within
                gm = arcpy.management.GetRasterProperties(
                        in_raster=out_r,
                        property_type="MAXIMUM"
                    )
                if int(gm[0]):
                    msg10 += ("\n\t\t\tNot all pixels of MURASTER "
                    f"covered by SARASTER, see {out_r}")
                else:
                    arcpy.Delete_management(out_r) 
            except:
                etype, exc, tb = sys.exc_info()
                if "Invalid output extent" in str(exc):
                    msg10 += "\n\t\t\tSARASTER and MURASTER do not overlap"
                else:
                    func = sys._getframe().f_code.co_name
                    arcpy.AddError(pyErr(func))

            del s_rast, p_rast, m_rast
            stem = "Raster Topology: "
            if msg10:
                msg10 = f'\n\t\t{stem:<{25}}FAILED ' + msg10
                errored = True
            else:
                msg10 = f'\n\t\t{stem:<{25}}NOMINAL'
            logf.write(msg10)

        return errored
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return True
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return True


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
        
        rss_dir = args[0]
        st = args[1]
        i = args[2]
        if not i:
            arcpy.AddMessage(f"Validate RSS Datasets: {v}\n")
        
        user = os.environ.get('USERNAME')
        now = datetime.now()
        now_str = now.strftime("%m/%d/%Y %H:%M:%S")

        dir_contents = os.listdir(rss_dir)
        # Is it Double bagged? As in directory <ST>/<ST>
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
                arcpy.AddWarning(
                    f"\tA validation error(s) found, see {log}"
                )
                st = st + '_'
            else:
                arcpy.AddMessage("\tValidation results are Nominal")

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