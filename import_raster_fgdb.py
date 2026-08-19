#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One of three scripts called by the Create gSSURGO File Geodatabase tool
from the RSS SSURGO Export Tool arctoolbox
This tool imports the raster into the RSS database.
Created on: 09/19/2024

https://desktop.arcgis.com/en/arcmap/latest/map/projections/pdf/geographic_transformations.pdf

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov

@modified 07/08/2026
    @by: Alexnder Stum
@version: 2.0

# Proposed upgrades
- Run the three rasters in parallel

# --- Updated 07/08/2026, v 2.0
- This version produces RSS version 2.1. Version 2.0 had the inclusion 
of the SARASTER and PARASTER. 2.1 has refined and complete metadata and 
adds a Spatial Version column to MURASTER.
- Builds RAT's from SSURGO tables
- Performs mosaic for user
# --- version 1.2, Updated 09/18/2025 - Alexander Stum
- Can handle if raster band already renamed
# ---
The orginal tool this is base off of is from the ArcMap Desktop toolbox
ArcGIS Desktop Build RSS gdb: Import Raster to RSS db. This tool will 
project the raster in to the 'NAD_1983_Contiguous_USA_Albers' (5070)
coordinate system if necessary using the datum transformation 
WGS_1984_(ITRF00)_To_NAD_1983.
Rasters will be resampled to 10 meter resolution with Nearest Neighbor 
and snap raster to align with gSSURGO (target alinged to 5 meter). Input 
raster must have an mukey field that reflects the raster value which point
the map unit table.
"""
v = '2.0'

import arcpy
import gc
import sys
import os
import traceback
from datetime import datetime
from pathlib import Path
import platform
import re
import shutil
import subprocess
import xml.etree.cElementTree as ET

import numpy as np
from osgeo import gdal

try:
    from pyproj import CRS
except:
    arcpy.AddError(
        'This tool requires the installation of the ESRI Deep Learning Library'
    )


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


def extCoord(coord: float, cell_r: float, offset=0) -> float:
    """Calculates coordinate component to snap extent
Number of cells from snap point to corner coordinate times
resolution equals new extent coordinate component.

    Parameters
    ----------
    coord : float
        Either the X or Y coordinate 
    cell_r : float
        Raster cell size
    offset : float
        Offset factor
    Returns
    -------
    float
        Coordinate componet for new raster extent. 
        Returns the string 'Error' if an exception is raised.
    """

    try:
        coord = coord + offset
        coord_n = (
            coord // cell_r + round((coord % cell_r) / cell_r)
        ) * cell_r
        return coord_n - offset

    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return 'Error'
    

def nd_type_check(input_r: str, ri: int, tmp_p: str) -> str:
    """Checks snap raster and coordinate system
    Sets the raster nodata pixel values to 0 and sets the pixel data
    type to UInt32.
    I understand that gdal uses Position Vector convention, so angular Helmert
    paramters have been inverted from the WGS_1984_(ITRF00)_To_NAD_1983
    ESRI transform.
    
    Parameters
    ----------
    input_r : str
        Path of the input raster to be checked, and if needed modified.
    ri : int
        Raster index iteration, used to uniquely name vrt files
    tmp_p : str
        Path where the vrt files will be saved

    Returns
    -------
    str
        String is the path of the orignial input or amended as a vrt.
    """
    try:
        # gdal.Unlink() (or gdal.VSIUnlink()) to remove the file. 
        # To completely free up the memory, ensure you also set the dataset object 
        # to None in your code (e.g., ds = None) so GDAL’s garbage collector can 
        # completely purge it
        info = gdal.Info(input_r, wktFormat='WKT2', format='json')
        nd = info['bands'][0]['noDataValue']
        dt = info['bands'][0]['type']
        vrt = None

        # Check spatial reference
        rast_d = arcpy.Describe(input_r)
        # band_d = rast_d.children[0]
        input_sr = rast_d.spatialReference
        poly_ext = rast_d.extent
        snapped = (poly_ext.XMin % 5 or poly_ext.YMin % 5 or poly_ext.XMax % 5 
                or poly_ext.YMax % 5)
        
        if input_sr.factoryCode != 5070:
            # only encoding to Warp with NAD83 or WGS84
            arcpy.AddWarning(
                "\t\tNot correct coordinate system 5070. "
                "Will attempt to project raster"
            )
            crs = CRS.from_wkt(input_sr.exportToString())
            crs_dict = crs.to_json_dict()
            input_datum = crs_dict['base_crs']['name']
            
            if input_datum not in ('NAD83', 'WGS84'):
            # wkt = input_sr.exportToString()
            # nad_patt = r"North.*?America.*?1983"
            # wgs_patt = r"WGS.*?1984"
            # if not re.search(nad_patt, wkt) and not re.search(wgs_patt, wkt):
                arcpy.AddError(
                    f"Could not project {input_r}\n"
                    "Project raster using the Project Raster tool:\n"
                    "\tOutput Coordinate System: "
                    "NAD_1983_Contiguous_USA_Albers\n"
                    "Geographic Transformation: specify approprieate one\n"
                    "\tEnvironments: Snap Raster: set to a gSSURGO MURASTER"
                )
                return ''
            
            out_sr = arcpy.SpatialReference(5070)
            tm = "WGS_1984_(ITRF00)_To_NAD_1983"
            poly_ext_5070 = poly_ext.polygon.projectAs(out_sr, tm)
            rast_ext = poly_ext_5070.extent
            # set raster extent
            rast_lr = rast_ext.lowerRight
            rast_ul = rast_ext.upperLeft
            rast_lrx = extCoord(rast_lr.X, 10, 5)
            rast_lry = extCoord(rast_lr.Y, 10, 5)
            rast_ulx = extCoord(rast_ul.X, 10, 5)
            rast_uly = extCoord(rast_ul.Y, 10, 5)

            wkt_out = (
                'PROJCS['
                    '"NAD_1983_Contiguous_USA_Albers",'
                    'GEOGCS["GCS_North_American_1983",'
                        'DATUM["D_North_American_1983",'
                            'SPHEROID["GRS_1980",6378137.0,298.257222101],'
                            'TOWGS84['
                                '0.9956, -1.9013, -0.5215,'
                                '-0.025915, -0.009426, -0.011599, 0.00062]],'
                        'PRIMEM["Greenwich",0.0],'
                            'UNIT["Degree",0.0174532925199433]],'
                    'PROJECTION["Albers"],'
                    'PARAMETER["False_Easting",0.0],'
                    'PARAMETER["False_Northing",0.0],'
                    'PARAMETER["Central_Meridian",-96.0],'
                    'PARAMETER["Standard_Parallel_1",29.5],'
                    'PARAMETER["Standard_Parallel_2",45.5],'
                    'PARAMETER["Latitude_Of_Origin",23.0],'
                    'UNIT["Meter",1.0],'
                    'ID["EPSG",5070]]')

            # Set outputBoundsSRS (-te_srs) and outputBounds (-te)
            # Set dstSRS (-t_srs)
            # Set resampleAlg (-r)
            vrt_a = f"{tmp_p}/temp{ri}a.vrt"
            command = [
                "gdalwarp",
                "-te", rast_ulx, rast_lry, rast_lrx, rast_uly,
                "-te_srs", wkt_out,
                "-t_srs", wkt_out,
                "-tr", "10", "10",
                "-r", "near",
                "-multi",
                "-wm", "1049",
                "-wo", "NUM_THREADS=2",
                "-co", "overwrite",
                input_r, vrt_a
            ]

            try:
                # Run the subprocess and capture output for debugging
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=True,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                # arcpy.AddMessage(result.stdout)
            except subprocess.CalledProcessError as e:
                arcpy.AddError(f"Error executing gdalwarp: {e}")
                arcpy.AddError(f"GDAL Error: {e.stderr}")
                return ''
            input_r = vrt_a

        elif snapped:
            arcpy.AddWarning(
                f"\t\tNot snapped, off by {5 - snapped} meters: realigning"
            )
            # set raster extent
            rast_lr = poly_ext.lowerRight
            rast_ul = poly_ext.upperLeft
            rast_lrx = str(extCoord(rast_lr.X, 10, 5))
            rast_lry = str(extCoord(rast_lr.Y, 10, 5))
            rast_ulx = str(extCoord(rast_ul.X, 10, 5))
            rast_uly = str(extCoord(rast_ul.Y, 10, 5))

            vrt_a = f"{tmp_p}/temp{ri}d.vrt"
            command = [
                "gdalwarp",
                "-te", rast_ulx, rast_lry, rast_lrx, rast_uly,
                "-tr", "10", "10",
                "-r", "near",
                "-multi",
                "-wm", "1049",
                "-wo", "NUM_THREADS=2",
                "-co", "overwrite",
                input_r, vrt_a
            ]

            try:
                # Run the subprocess and capture output for debugging
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=True,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                # arcpy.AddMessage(result.stdout)
            except subprocess.CalledProcessError as e:
                arcpy.AddError(f"Error executing gdalwarp: {e}")
                arcpy.AddError(f"GDAL Error: {e.stderr}")
                return ''
            input_r = vrt_a

        if nd != 0:
            # Change no data value to 0
            vort = gdal.BuildVRTOptions(
                VRTNodata=0, creationOptions=['overwrite']
            )
            vrt_b = f"{tmp_p}/temp{ri}b.vrt"
            vrt = gdal.BuildVRT(vrt_b, input_r, options=vort)
            vrt.Close()
            vrt = None
            input_r = vrt_b

        if dt != 'UInt32':
            # translate to convert data type Unsigned 32-bit Integer
            vrt_c = f"{tmp_p}/temp{ri}c.vrt"
            command = [
                "gdal_translate",
                "-ot", "UInt32",
                "-co", "overwrite",
                input_r, vrt_c
            ]
            try:
                # Run the subprocess and capture output for debugging
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    check=True,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
                # arcpy.AddMessage(result.stdout)
            except subprocess.CalledProcessError as e:
                arcpy.AddError(f"Error executing gdalwarp: {e}")
                arcpy.AddError(f"GDAL Error: {e.stderr}")
                return ''
            input_r = vrt_c

        return input_r
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return ''
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return ''


def calculateStatistics(
        rast_p: str, n_cls: int = 256
    ) -> str: # src_ds: Dataset, out_band: Band, 
    """Calculate raster statistics to populate the .aux.xml file

    Parameters
    ----------
    rast_p : str
        Full path of the raster for which statistis will be calculated
    src_ds : Dataset
        GDAL dataset object of the raster for which statistics 
        will be calculated
    out_band : Band
        GDAL Band of the src_ds for which statistics will be calculated. 
        Assumed to be only one band.
    n_cls : int, optional
        Number of histogram bins, by default 256

    Returns
    -------
    str
        An empty string if successful, otherwise an error message.
    """
    try:
        out_ds = gdal.Open(rast_p)
        out_band = out_ds.GetRasterBand(1)
        #is approximate calculation okay (BOOL: default=False), 
        #force recalculation if stats already exist (BOOL: default=False)
        #Rtns list: Min, Max, Mean, StdDev
        stats = out_band.GetStatistics(False, True)

        #GetStatistics wasn't writing the .aux.xml stats file immediately. 
        # so I added this.
        #Not too far down, there is code to modify the stats  
        out_ds.FlushCache()

        #gdal.org/doxygen/classGDALRasterBand.html#aa21dcb3609bff012e8f217ebb7c81953
        # buckets is number of bins, default is 256. For nominal want 
        # number of unique values
        if stats[0] != stats[1]:
            histogram = out_band.GetHistogram(
                min=stats[0], max=stats[1], approx_ok=False, buckets=n_cls
            )
        else:
            histogram = out_band.GetHistogram(
                min=stats[0], max=stats[0] + .5, approx_ok=False, buckets=n_cls
            )

        #open the statistics file that GetStatistics creates automatically 
        xmlpath = rast_p + '.aux.xml'
        meta_tree = ET.parse(xmlpath)
        xml_root = meta_tree.getroot()

        # Create the <Histograms> block
        histograms = ET.Element("Histograms")
        hist_item = ET.SubElement(histograms, "HistItem")
        ET.SubElement(hist_item, 'HistMin').text = str(int(stats[0]))
        ET.SubElement(hist_item, 'HistMax').text = str(int(stats[1]))
        ET.SubElement(hist_item, 'BucketCount').text = '256'
        ET.SubElement(hist_item, 'IncludeOutOfRange').text = '1'
        ET.SubElement(hist_item, 'Approximate').text = '0'
        ET.SubElement(
            hist_item, 'HistCounts').text = ' | '.join(map(str, histogram)
        )

        pam_band = xml_root.find("PAMRasterBand")
        pam_band.insert(0, histograms)

        meta_tree.write(xmlpath)
        out_ds.Close()
        out_ds = None
        del(xml_root, meta_tree, out_band, out_ds)

        return ''
    except:
        func = sys._getframe().f_code.co_name
        return pyErr(func)
    

def buildPyramids(rast_p: str, method: str) -> str:
    """Build pyramids for the raster dataset. Calculates for all zoom levels
     with >= 512 rows or columns plus two more levels. Pyramid file is
     written out as an .ovr file.

    Parameters
    ----------
    src_ds : Dataset
        GDAL dataset object of the raster for which pyramids 
        will be built
    method : str
        Resampling method to be used, typical methods are NEAREST or BILINEAR,
        NEAREST is default
        https://gdal.org/en/stable/programs/gdal_raster_overview_add.html

    Returns
    -------
    str
        An empty string if successful, otherwise an error message.
    """
    try:
        src_ds = gdal.Open(rast_p)

        # DEFLATE or ZSTD
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        max_dim = max([src_ds.RasterXSize, src_ds.RasterYSize])
        if max_dim >= 1024:
            n2 = np.array([2])**np.arange(1, 30)
            r2 = np.array([max_dim]) // n2
            pyr_idx = np.where(r2 <= 512)[0][0]
            levels = n2[:pyr_idx + 2]
            src_ds.BuildOverviews(method, levels.tolist())
        
        src_ds.Close()
        src_ds = None
        return ''

    except:
        func = sys._getframe().f_code.co_name
        return pyErr(func)
    

def build_RAT(
        rast_p: str, input_rasts: dict, prefix: str, 
        new_gdb_p: str, st: str, spat_v: int
    ) -> int:
    """Builds and populates the Raster Attribute Table (RAT) as dbf 
    using ESRI functions

    Parameters
    ----------
    rast_p : str
        Path of the raster for which the RAT is being built
    input_rasts : dict
        This dictionary carries the attribute field information specific for
        each RSS raster dataset
    prefix : str
        RSS raster dataset prefix
    new_gdb_p : str
        The output File Geodatabase
    st : str
        State abbreviation
    spat_v : int
        Spatial version of the RSS raster dataset

    Returns
    -------
    int
        0 if successful, otherwise a negative integer.
    """
    try:
        # Build RAT
        arcpy.management.BuildRasterAttributeTable(rast_p, "Overwrite")
        arcpy.management.AddFields(
            in_table=rast_p,
            field_description= input_rasts[prefix][1],
            template=None
        )
        # Populate RAT
        # raster specific
        if prefix == 'MU':
            arcpy.management.CalculateField(rast_p, "MUKEY", "!Value!")
            arcpy.management.CalculateField(rast_p, "SPATIALVER", spat_v)
        elif prefix == 'PA':
            # get User Project ID from LAO table
            sCur = arcpy.da.SearchCursor(
                in_table=new_gdb_p + '/laoverlap', 
                field_names=['lareaovkey', 'areasymbol'], 
                where_clause="areatypename='Raster Soil Survey Project'"
            )
            lao = {int(k): area for k, area in sCur}
            del sCur
            uCur = arcpy.da.UpdateCursor(rast_p, '*')
            for row in uCur:
                uproj = lao.get(row[1])
                if not uproj:
                    arcpy.AddError(
                        'Values in the PARASTER are not found in the '
                        'Legend Area Overalp Table (laoverlap)'
                    )
                    return -1
                uCur.updateRow(
                    row[:3] + [uproj, 'Raster Soil Survey Project', spat_v]
                )
            del uCur
        else:
            arcpy.management.CalculateField(rast_p, "AREASYMBOL", f"'{st}'")
            arcpy.management.CalculateField(
                rast_p, "AREATYPE", "'Raster Soil Survey Area'"
            )
            arcpy.management.CalculateField(rast_p, "SPATIALVER", spat_v)

        return 0

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return -2
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return -3

    
def UpdateMetadata(prev_gdb_p: str,
                   new_gdb_p: str,
                   st: str,
                   fy: str,
                   module_p: str,
                   prefix: str

    ) -> list[str]:
    """ Used for featureclass and geodatabase metadata. Does not do individual 
    tables. Reads and edits the original metadata object and then exports the 
    edited version back to the featureclass or geodatabase.

    Parameters
    ----------
    prev_gdb_p : str
        Path of the previous FY's RSS geodatabase.
    new_gdb_p : str
        Path of the current FY's RSS geodatabase.
    rast_p : str
        Path of the raster
    st : str
        Abbreviation of the state
    fy: str
        Fiscal year of publication

    Returns
    -------
    list[str]
        Collection of messages, no messages means function was completely 
        successful.
    """
    try:
        msg = []
        # gdb_n = os.path.basename(new_gdb_p)[:-4]
        msgAppend = msg.append

        fyi = int(fy)

        # initial metadata exported from current target featureclass
        meta_export = arcpy.env.scratchFolder + f"/xxExport_{prefix}.xml"
        # the metadata xml that will provide the updated info
        meta_import_fgdb = arcpy.env.scratchFolder + f"/xxImport_{prefix}1.xml"
        meta_import_tiff = arcpy.env.scratchFolder + f"/xxImport_{prefix}2.xml"
        # Cleanup XML files from previous runs
        if os.path.isfile(meta_import_fgdb):
            os.remove(meta_import_fgdb)
        if os.path.isfile(meta_import_tiff):
            os.remove(meta_import_tiff)
        if os.path.isfile(meta_export):
            os.remove(meta_export)
        # Edit copy existing base raster
        if prev_gdb_p:
            prev_r = (f"{os.path.dirname(prev_gdb_p)}/RSS_{st}/spatial/"
                  f"{prefix}RASTER_10m_{st}_{fyi - 1}.tif")
            prev_r_meta = arcpy.metadata.Metadata(prev_r)
            prev_r_meta.exportMetadata(meta_export, "ISO19115_3")
            del prev_r_meta
        # Make copy of .xml file from module if new
        else:
            base_xml_p = f"{module_p}/{prefix}RASTER_meta.xml"
            shutil.copyfile(base_xml_p, meta_export)

        # timing       
        target_dt = datetime(fyi - 1, 10, 1, 12, 0)
        now_dt = datetime.now()
        now = now_dt.isoformat(timespec='seconds')
        if now_dt > target_dt:
            pub_dt = now
        else:
            pub_dt = target_dt.isoformat(timespec='seconds')
        
        states = {
            'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 
            'AS': 'American Samoa', 'AZ': 'Arizona', 'CA': 'California', 
            'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia',
            'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'GU': 'Guam',
            'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois',
            'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 
            'LA': 'Louisiana', 'MA': 'Massachusetts', 'MD': 'Maryland',
            'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota',
            'MO': 'Missouri', 'MS': 'Mississippi', 'MT': 'Montana',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'NE': 'Nebraska',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
            'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania',
            'PRUSVI': "Puerto Rico and U.S. Virgin Islands",
            'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota',
            'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia',
            'VT': 'Vermont', 'WA': 'Washington', 'WI': 'Wisconsin',
            'WV': 'West Virginia', 'WY': 'Wyoming'
        }
        state = states[st]

        # Edit the XML
        tree = ET.parse(meta_export)
        root = tree.getroot()
        iso = '{http://standards.iso.org/iso/19115/-3/'

        # Purpose (Summary) element
        purpose = root.find(
            iso + 'mdb/1.0}identificationInfo/'
            + iso + 'mri/1.0}MD_DataIdentification/'
            + iso + 'mri/1.0}purpose/'
            + iso + 'gco/1.0}CharacterString'
        )
        i = purpose.text.index(' Raster Soil Survey (RSS)')
        purpose.text = f"The {state} {fy}{purpose.text[i:]}"

        # Keywords
        keywords = root.findall(
            iso + 'mdb/1.0}identificationInfo/'
            + iso + 'mri/1.0}MD_DataIdentification/'
            + iso + 'mri/1.0}descriptiveKeywords'
        )

        address = (
                iso + 'mri/1.0}MD_Keywords/'
                + iso + 'mri/1.0}type/'
                + iso + 'mri/1.0}MD_KeywordTypeCode'
            )
        for kw in keywords:
            if kw.find(address) is None:
                continue
            type_code = kw.find(address).text
            # only for new databases
            if type_code == 'place':
                words = kw.findall(
                    iso + 'mri/1.0}MD_Keywords/'
                    + iso + 'mri/1.0}keyword/'
                )
                for word in words:
                    if word.text == 'xxST':
                        word.text = st
                    elif word.text == 'xxSTATE':
                        word.text = state
            # temporal keyword, need to replace previous year's
            elif type_code == 'temporal':
                words = kw.findall(
                    iso + 'mri/1.0}MD_Keywords/'
                    + iso + 'mri/1.0}keyword/'
                )
                for word in words:
                    if word.text == 'xxFYxx' or word.text.startswith('20'):
                        word.text = fy

        # Processing Environment
        env = (f"Microsoft {platform.system()} {platform.version()}; "
            f"Python {platform.python_version()}")
        address = (
            iso + 'mdb/1.0}identificationInfo/'
            + iso + 'mri/1.0}MD_DataIdentification/'
            + iso + 'mri/1.0}environmentDescription/'
            + iso + 'gco/1.0}CharacterString'
        )
        root.find(address).text = env

        # Citation elements
        cit_add = (
                iso + 'mdb/1.0}identificationInfo/'
                + iso + 'mri/1.0}MD_DataIdentification/'
                + iso + 'mri/1.0}citation/'
                + iso + 'cit/1.0}CI_Citation')
        cit_rt = root.find(cit_add)
            # title
        title = cit_rt.find(
            iso + 'cit/1.0}title/'
            + iso + 'gco/1.0}CharacterString'
        )
        title.text = f"Map Unit Raster 10m - {state} {fy}"
            # edition
        edition = cit_rt.find(
            iso + 'cit/1.0}edition/'
            + iso + 'gco/1.0}CharacterString'
        )
        edition.text = f"{st} {fy}"
            # edition date
        edition_dt = cit_rt.find(
            iso + 'cit/1.0}editionDate/'
            + iso + 'gco/1.0}DateTime'
        )
        edition_dt.text = pub_dt
            # series
        series_n = cit_rt.find(
            iso + 'cit/1.0}series/'
            + iso + 'cit/1.0}CI_Series/'
            + iso + 'cit/1.0}name/'
            + iso + 'gco/1.0}CharacterString'
        )
        series_n.text = 'Raster Soil Survey (RSS)'

        series_id = cit_rt.find(
            iso + 'cit/1.0}series/'
            + iso + 'cit/1.0}CI_Series/'
            + iso + 'cit/1.0}issueIdentification/'
            + iso + 'gco/1.0}CharacterString'
        )
        series_id.text = '2.1'

            # dates
        date_type_add = (
            iso + 'cit/1.0}dateType/'
            + iso + 'cit/1.0}CI_DateTypeCode'
        )
        date_add = (
            iso + 'cit/1.0}date/'
            + iso + 'gco/1.0}DateTime'
        )
        for date_rt in cit_rt.iter(iso + 'cit/1.0}CI_Date'):
            dt = date_rt.find(date_type_add).text
                # creation date
            if dt == 'creation' and not prev_gdb_p:
                date_rt.find(date_add).text = now
                # udate date
            elif dt == 'revision':
                date_rt.find(date_add).text = now
                # publication date
            elif dt == 'publication':
                date_rt.find(date_add).text = pub_dt

        # Process Step 2
        proc_add = (
            iso + 'mdb/1.0}resourceLineage/'
            + iso + 'mrl/1.0}LI_Lineage/'
            + iso + 'mrl/1.0}processStep/'
            + iso + 'mrl/1.0}LI_ProcessStep'
        )

        proc_rts = root.findall(proc_add)
        p_title_add = (
            iso + 'mrl/1.0}source/'
            + iso + 'mrl/1.0}LI_Source/'
            + iso + 'mrl/1.0}sourceCitation/'
            + iso + 'cit/1.0}CI_Citation/'
            + iso + 'cit/1.0}title/'
            + iso + 'gco/1.0}CharacterString'
        )
        for proc_rt in proc_rts:
            if (proc_title := proc_rt.find(p_title_add)) is not None:
                if 'Build Raster Soil Survey (RSS)' in proc_title.text:
            # Process Step Date
                    dt_add = (
                        iso + 'mrl/1.0}stepDateTime/'
                        + '{http://www.opengis.net/gml/3.2}TimeInstant/'
                        + '{http://www.opengis.net/gml/3.2}timePosition'
                    )
                    proc_rt.find(dt_add).text = now
            # Tool version
                    ed_add = (
                        iso + 'mrl/1.0}source/'
                        + iso + 'mrl/1.0}LI_Source/'
                        + iso + 'mrl/1.0}sourceCitation/'
                        + iso + 'cit/1.0}CI_Citation/'
                        + iso + 'cit/1.0}edition/'
                        + iso + 'gco/1.0}CharacterString'
                    )
                    proc_rt.find(ed_add).text = v

        # Set up as ESRI Raster format type
        format = root.find(
            iso + 'mdb/1.0}distributionInfo/'
            + iso + 'mrd/1.0}MD_Distribution/'
            + iso + 'mrd/1.0}distributionFormat/'
            + iso + 'mrd/1.0}MD_Format/'
            + iso + 'mrd/1.0}formatSpecificationCitation/'
            + iso + 'cit/1.0}CI_Citation/'
            + iso + 'cit/1.0}title/'
            + iso + 'gco/1.0}CharacterString'
        )
        format.text = 'ESRI File Geodatabase Raster'
        format_v = root.find(
            iso + 'mdb/1.0}distributionInfo/'
            + iso + 'mrd/1.0}MD_Distribution/'
            + iso + 'mrd/1.0}distributionFormat/'
            + iso + 'mrd/1.0}MD_Format/'
            + iso + 'mrd/1.0}formatSpecificationCitation/'
            + iso + 'cit/1.0}CI_Citation/'
            + iso + 'cit/1.0}edition/'
            + iso + 'gco/1.0}CharacterString'
        )
        format_v.text = '10.0'

        # write FGDB raster import file
        tree.write(
            meta_import_fgdb, 
            encoding = "utf-8", 
            xml_declaration = None, 
            default_namespace = None, 
            method = "xml"
        )

        # Set up as GeoTIFF format type
        format.text = 'GeoTIFF Raster'
        format_v.text = '1.1'

        # write GeoTIFF raster import file
        tree.write(
            meta_import_tiff, 
            encoding = "utf-8", 
            xml_declaration = None, 
            default_namespace = None, 
            method = "xml"
        )

        fgdb_r = f"{new_gdb_p}/{prefix}RASTER_10m_{st}_{fyi}"
        tiff_r = (f"{os.path.dirname(new_gdb_p)}/RSS_{st}/spatial/"
                  f"{prefix}RASTER_10m_{st}_{fyi}.tif")
        fgdb_r_meta = arcpy.metadata.Metadata(fgdb_r)
        tiff_r_meta = arcpy.metadata.Metadata(tiff_r)

        fgdb_r_meta.importMetadata(meta_import_fgdb, "ISO19115_3")
        tiff_r_meta.importMetadata(meta_import_tiff, "ISO19115_3")
        fgdb_r_meta.deleteContent('GPHISTORY')
        tiff_r_meta.deleteContent('GPHISTORY')
        fgdb_r_meta.save()
        tiff_r_meta.save()
        # 
        tiff_r_meta = arcpy.metadata.Metadata(tiff_r)
        fgdb_r_meta = arcpy.metadata.Metadata(fgdb_r)
        tiff_r_meta.synchronize()
        fgdb_r_meta.synchronize()

        # clean up
        del(root, tree, fgdb_r_meta, tiff_r_meta)
        # delete the temporary xml metadata files
        if os.path.isfile(meta_import_fgdb):
            os.remove(meta_import_fgdb)
        if os.path.isfile(meta_import_tiff):
            os.remove(meta_import_tiff)
        if os.path.isfile(meta_export):
            os.remove(meta_export)

         # if SARASTER, update FGDB extent
        if prefix == 'SA':
            # set up metadata file of FGDB
            fgdb_meta = arcpy.metadata.Metadata(new_gdb_p)
            fgdb_meta.exportMetadata(meta_export, "ISO19115_3")
            tree = ET.parse(meta_export)
            root = tree.getroot()

            # get extent from SARASTER
            ext = arcpy.Describe(tiff_r).extent
            # project to NAD83
            ext_p = ext.projectAs(arcpy.SpatialReference(4269))
            # Western Longitude
            wl = root.find(
                iso + 'mdb/1.0}identificationInfo/'
                + iso + 'mri/1.0}MD_DataIdentification/'
                + iso + 'mri/1.0}extent/'
                + iso + 'gex/1.0}EX_Extent/'
                + iso + 'gex/1.0}geographicElement/'
                + iso + 'gex/1.0}EX_GeographicBoundingBox/'
                + iso + 'gex/1.0}westBoundLongitude/'
                + iso + 'gco/1.0}Decimal'
            )
            wl.text = str(ext_p.XMin)
            # Eastern Longitude
            el = root.find(
                iso + 'mdb/1.0}identificationInfo/'
                + iso + 'mri/1.0}MD_DataIdentification/'
                + iso + 'mri/1.0}extent/'
                + iso + 'gex/1.0}EX_Extent/'
                + iso + 'gex/1.0}geographicElement/'
                + iso + 'gex/1.0}EX_GeographicBoundingBox/'
                + iso + 'gex/1.0}eastBoundLongitude/'
                + iso + 'gco/1.0}Decimal'
            )
            el.text = str(ext_p.XMax)
            # Southern Latitude
            sl = root.find(
                iso + 'mdb/1.0}identificationInfo/'
                + iso + 'mri/1.0}MD_DataIdentification/'
                + iso + 'mri/1.0}extent/'
                + iso + 'gex/1.0}EX_Extent/'
                + iso + 'gex/1.0}geographicElement/'
                + iso + 'gex/1.0}EX_GeographicBoundingBox/'
                + iso + 'gex/1.0}southBoundLatitude/'
                + iso + 'gco/1.0}Decimal'
            )
            sl.text = str(ext_p.YMin)
            # Northern Latitude
            nl = root.find(
                iso + 'mdb/1.0}identificationInfo/'
                + iso + 'mri/1.0}MD_DataIdentification/'
                + iso + 'mri/1.0}extent/'
                + iso + 'gex/1.0}EX_Extent/'
                + iso + 'gex/1.0}geographicElement/'
                + iso + 'gex/1.0}EX_GeographicBoundingBox/'
                + iso + 'gex/1.0}northBoundLatitude/'
                + iso + 'gco/1.0}Decimal'
            )
            nl.text = str(ext_p.YMax)

            # Write and cleanup
            tree.write(
                meta_import_fgdb, 
                encoding = "utf-8", 
                xml_declaration = None, 
                default_namespace = None, 
                method = "xml"
            )
            fgdb_meta.importMetadata(meta_import_fgdb, "ISO19115_3")
            fgdb_meta.deleteContent('GPHISTORY')
            fgdb_meta.save()

            del root, tree, fgdb_meta
            if os.path.isfile(meta_import_fgdb):
                os.remove(meta_import_fgdb)

        return ''
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(arcpyErr(func)))
        return msg
    except:
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(pyErr(func)))
        return msg

        
def main(args: list[str, str, str, int, str]) ->str:
    """This tool imports the raster into the RSS

    Parameters
    ----------
    args : list[str, str, str, int, str]
        This tool needs
            - path of the RSS file geodatabse
            - path of the input raster
            - State abbreviation
            - Fiscal year of publication
            - Module path to access metadata xml template

    Returns
    -------
    str
        The name of the imported raster if successful, empty string otherwise.
    """
    try:
        arcpy.AddMessage(f"\nImport Raster FGDB, {v = !s}")

        # Output Folder
        gdb_p = args[0]
        # State
        st = args[1]
        # fiscal year
        fy = args[2]
        # Previous year's RSS database
        prev_gdb_p = args[3]
        # updates to MURASTER to be appended
        mu_rasters = args[4]
        # updates to PARASTER to be appended
        pa_rasters = args[5]
        # updated version of SARASTER
        sa_raster = args[6]
        # RSS module path
        module_p = args[7]

        rss_p = os.path.dirname(gdb_p)
        input_rasts = {
            'MU': [mu_rasters, [['MUKEY', 'TEXT', '', 30],
                                ['SPATIALVER', 'LONG']]
                    ],
            'PA': [pa_rasters, [['UPROJID', 'TEXT', '', 254],
                                ['AREATYPE', 'TEXT', '', 254],
                                ['SPATIALVER', 'LONG']],
                    ],
            'SA': [sa_raster, [['AREASYMBOL', 'TEXT', '', 20],
                               ['AREATYPE', 'TEXT', '', 254],
                               ['SPATIALVER', 'LONG']]
                ]}
        
        for prefix in input_rasts.keys():
            arcpy.AddMessage(f"\tWorking on the {prefix}RASTER")
            # First year publication, no former base raster to mosaic to
            if not prev_gdb_p:
                input_files = input_rasts[prefix][0]
                base_r = ''
            # Mosaic to previous year's base raster, if no new rasters input
            # input will be an empty list
            else:
                # build path of previous years base raster
                base_r = (f"{os.path.dirname(prev_gdb_p)}/RSS_{st}/spatial/"
                          f"{prefix}RASTER_10m_{st}_{fy - 1}.tif")
                input_files = [base_r] + input_rasts[prefix][0]
            
            # temp location of vrt's
            tmp_p = f'{rss_p}/RSS_{st}/spatial/tmp'
            Path(tmp_p).mkdir(parents=True, exist_ok=True)

            input_paths = []
            for ri, input_r in enumerate(input_files):
                # check/amend raster csr, nd value, data type
                rast_p = nd_type_check(input_r, ri, tmp_p)
                input_paths.append(rast_p)
                if not rast_p:
                    return ''

            # Create new raster mosaic
            nested_vrt = [vrt for vrt in input_paths if '.vrt' in vrt]
            # are there more than one inputs?
            if len(input_paths) > 1:
                vrt_p = tmp_p + "/temp_mos.vrt"
                vort = gdal.BuildVRTOptions(VRTNodata=0)
                vrt2 = gdal.BuildVRT(vrt_p, input_paths, options=vort)
                vrt2.FlushCache()
                vrt2.Close()
                vrt2 = None
            else:
                vrt_p = input_paths[0]

            out_p = (f"{rss_p}/RSS_{st}/spatial/"
                     f"{prefix}RASTER_10m_{st}_{fy}.tif")
            
            # processing nested vrt's seems to lead to memory leak. 
            # Calling a subprocess shuts those down. But its slower.
            b = True
            if not b and not nested_vrt:
                wo = gdal.WarpOptions(
                    multithread=True,
                    warpMemoryLimit='1049',
                    warpOptions='NUM_THREADS=2',#ALL_CPUS',
                    creationOptions=[
                        'overwrite', 'compress=ZSTD', 'PREDICTOR=2', 'ZLEVEL=1', 
                        'TILED=YES', 'BLOCKXSIZE=512', 'BLOCKYSIZE=512'
                    ]
                )
                out_ds = gdal.Warp(out_p, vrt_p, options=wo)
                out_ds.FlushCache()
                out_ds.Close()
                out_ds = None
            else: 
                command = [
                    "gdalwarp",
                    "-multi",
                    "-wm", "1049",
                    "-wo", "NUM_THREADS=2",
                    "-co", "overwrite",
                    "-co", "compress=ZSTD",
                    "-co", "PREDICTOR=2",
                    "-co", "ZLEVEL=1",
                    "-co", "TILED=YES",
                    "-co", "BLOCKXSIZE=512",
                    "-co", "BLOCKYSIZE=512",
                    vrt_p, out_p
                ]

                try:
                    # Run the subprocess and capture output for debugging
                    result = subprocess.run(
                        command,
                        capture_output=True,
                        text=True,
                        check=True,
                        creationflags=subprocess.CREATE_NO_WINDOW
                    )
                    # arcpy.AddMessage(result.stdout)
                    arcpy.AddMessage(f"\t\t{prefix}RASTER successfully created")
                except subprocess.CalledProcessError as e:
                    arcpy.AddError(f"Error executing gdalwarp: {e}")
                    arcpy.AddError(f"GDAL Error: {e.stderr}")
                    return ''

            gc.collect()
            shutil.rmtree(tmp_p)

            arcpy.AddMessage("\t\tCalculating Statistics..")
            msg = calculateStatistics(out_p)#, out_ds, out_band)
            if msg:
                msg = "Unable to Calculate Statistics: \n" + msg
                arcpy.AddWarning(msg)
            arcpy.AddMessage("\t\tBuilding Pyramids...")
            msg = buildPyramids(out_p, 'NEAREST')
            if msg:
                msg = "Failed to Build Pyramids: \n" + msg
                arcpy.AddWarning(msg)

            out_ds = gdal.Open(out_p)
            out_band = out_ds.GetRasterBand(1)
            # Rename bands
            if prefix == 'MU':
                out_band.SetDescription('MUKEY')
            elif prefix == 'PA':
                out_band.SetDescription('LAOKEY')
            else:
                out_band.SetDescription('LKEY')
            out_ds.Close()
            out_ds = out_band = None

            # Build RAT's
            # Get Spatial Version
            if not prev_gdb_p:
                spat_v = 1
            else:
                sCur = arcpy.da.SearchCursor(base_r, 'SPATIALVER')
                vers = {v for v, in sCur}
                del sCur
                # was raster updated?
                if input_rasts[prefix][0]:
                    spat_v = max(vers) + 1
                else:
                    spat_v = max(vers)

            build_RAT(out_p, input_rasts, prefix, gdb_p, st, spat_v)

            # Copy Rasters
            rast_p = f"{rss_p}/RSS_{st}.gdb/{prefix}RASTER_10m_{st}_{fy}"
            # Seems FGDB wont compress a raster with ZTSD
            arcpy.management.CopyRaster(
                in_raster=out_p,
                out_rasterdataset=rast_p,
                config_keyword="",
                background_value=None,
                nodata_value="0",
                onebit_to_eightbit="NONE",
                colormap_to_RGB="NONE",
                pixel_type="32_BIT_UNSIGNED",
                scale_pixel_value="NONE",
                RGB_to_Colormap="NONE",
                format="Esri Grid format",
                transform="NONE",
                process_as_multidimensional="CURRENT_SLICE",
                build_multidimensional_transpose="NO_TRANSPOSE"
            )

            # Metadata
            meta_b = UpdateMetadata(
                prev_gdb_p, gdb_p, st, str(fy), module_p, prefix
            )
            if not meta_b:
                arcpy.AddMessage(
                    f"\t\t{prefix}RASTER metadata successfully updated"
                )
            else:
                arcpy.AddWarning(
                    f"\t\t{prefix}RASTER metadata unsuccessfully updated"
                )

            # Rename bands
            rast = arcpy.Raster(rast_p)
            if prefix == 'MU':
                b1_n = rast.bandNames[0]
                if 'MUKEY' != b1_n:
                    rast.renameBand(b1_n, 'MUKEY')
            elif prefix == 'PA':
                b1_n = rast.bandNames[0]
                if 'LAOKEY' != b1_n:
                    rast.renameBand(b1_n, 'LAOKEY')
            else:
                b1_n = rast.bandNames[0]
                if 'LKEY' != b1_n:
                    rast.renameBand(b1_n, 'LKEY')
            del rast

        gc.collect()
        return 'rasters'
        
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
