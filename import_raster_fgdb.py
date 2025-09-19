#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One of three scripts called by the Create gSSURGO File Geodatabase tool
from the RSS SSURGO Export Tool arctoolbox
This tool imports the raster into the RSS database.
Created on: 09/19/2024

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov

@modified 09/19/2025
    @by: Alexnder Stum
@version: 1.2

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


import arcpy
import sys
import os
import traceback
import datetime
import platform
from arcpy import env
import xml.etree.cElementTree as ET


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
    

def UpdateMetadata(
        wksp: str, target: str, resolution: str, 
        script_p: str, ver: str, st: str
        ) ->str:
    """Creates metadata for the newly created gSSURGO raster from 
        FGDC_CSDGM xml template

    Replaces xx<keywords>xx in template
    xxSTATExx : State or states from legend overlap table (laoverlap)
    xxSURVEYSxx : String listing all the soil survey areas
    xxTODAYxx : Todays date
    xxFYxx : mmyyyy format, to signify vintage of SSURGO data
    xxENVxx : Windows, ArcGIS Pro, and Python version information
    xxNAMExx : Name of the gSSURGO raster dataset
    xxDBxx : Database the SSURGO data was sourced from
    xxVERxx : Version of that database

        Parameters
        ----------
        wksp : str
            Source path of the SSURGO database.
        target : str
            Name of the created gSSURGO raster.
        resolution : str
            sSSURGO cell size formatted with units.
        script_p : str
            Path to the SDDT/construct submodule where xml metadata 
            template is found.
        ver : str
            Tool version
        st: str
            State abbreviaiton

        Returns
        -------
        bool
            Returns empty string if successful, otherwise returns a message.
        """
    try:
        # Define input and output XML files
        # the metadata xml that will provide the updated info
        meta_import = env.scratchFolder + "/xxImport.xml"
        # original template metadata in script directory
        meta_export = f"{script_p}/RSS_ClassRaster.xml"
        # Cleanup output XML files from previous runs
        if os.path.isfile(meta_import):
            os.remove(meta_import)

        states = {
                'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas',
                'AS': 'American Samoa', 'AZ': 'Arizona', 'CA': 'California',
                'CO': 'Colorado', 'CT': 'Connecticut', 
                'DC': 'District of Columbia', 'DE': 'Delaware', 'FL': 'Florida',
                'FM': 'Federated States of Micronesia', 'GA': 'Georgia',
                'GU': 'Guam', 'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho',
                'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas',
                'KY': 'Kentucky', 'LA': 'Louisiana', 'MA': 'Massachusetts',
                'MD': 'Maryland', 'ME': 'Maine', 
                'MH': 'Republic of the Marshall Islands', 'MI': 'Michigan',
                'MN': 'Minnesota', 'MO': 'Missouri',
                'MP': 'Commonwealth of the Northern Mariana Islands',
                'MS': 'Mississippi', 'MT': 'Montana', 'NC': 'North Carolina',
                'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire',
                'NJ': 'New Jersey', 'NM': 'New Mexico', 'NV': 'Nevada',
                'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma',
                'OR': 'Oregon', 'PA': 'Pennsylvania', 'PR': 'Puerto Rico',
                'PW': 'Republic of Palau', 'RI': 'Rhode Island',
                'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
                'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 
                'VI': 'U.S. Virgin Islands', 'VT': 'Vermont',
                'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia',
                'WY': 'Wyoming'
            }
        state = states[st]

        sacat_p = f"{wksp}/sacatalog"
        with arcpy.da.SearchCursor(
            sacat_p, ("AREASYMBOL") #, "SAVEREST")
            ) as sCur:
            # f"{rec[0]} ({str(rec[1]).split()[0]})"
            exp_l = [rec[0] for rec in sCur]
        survey_i = ", ".join(exp_l)

        # System Environment
        esri_i = arcpy.GetInstallInfo()
        sys_env = (
            f"Microsoft {platform.system()} {platform.release()} "
            f"Version {platform.version()}; "
            f"ESRI {esri_i['ProductName']} {esri_i['Version']}; "
            f"Python {platform.python_version()}"
            )

        # Database
        wksp_d = arcpy.Describe(wksp)
        wksp_ext = wksp_d.extension
        if wksp_ext == 'gdb':
            db = 'ESRI File Geodatabase'
            tool = 'Soil Data Development Toolbox in ArcGIS Pro'
            ver = str(int(wksp_d.release.split(',')[0]) + 7)
        else:
            db = 'database'
            tool = ''
            ver = ''

        # Set date based upon today's date
        d = datetime.date.today()
        today = str(d.isoformat().replace("-",""))
        # As of July 2020, switch gSSURGO version format to YYYYMM
        fy = d.strftime('%Y%m')

        # Process gSSURGO_MapunitRaster.xml from script directory
        tree = ET.parse(meta_export)
        root = tree.getroot()

        # new citeInfo has title.text, edition.text, serinfo/issue.text
        citeinfo = root.findall('idinfo/citation/citeinfo/')
        if citeinfo is not None:
            # Process citation elements
            # title, edition, issue
            for child in citeinfo:
                if child.tag == "title":
                    newTitle = f"Map Unit Raster {resolution} {state}"
                    child.text = newTitle
                elif child.tag == "edition":
                    if child.text == 'xxFYxx':
                        child.text = fy
                elif child.tag == "serinfo":
                    for subchild in child.iter('issue'):
                        if subchild.text == "xxFYxx":
                            subchild.text = fy

        # Update place keywords
        place = root.find('idinfo/keywords/place')
        if place is not None:
            for child in place.iter('placekey'):
                if child.text == "xxSTATExx":
                    child.text = state
                elif child.text == "xxSURVEYSxx":
                    child.text = survey_i

        # Update credits
        idinfo = root.find('idinfo')
        if idinfo is not None:
            for child in idinfo.iter('datacred'):
                text = child.text
                if text.find("xxSTATExx") >= 0:
                    text = text.replace("xxSTATExx", state)
                if text.find("xxFYxx") >= 0:
                    text = text.replace("xxFYxx", fy)
                if text.find("xxTODAYxx") >= 0:
                    text = text.replace("xxTODAYxx", today)
                child.text = text

        purpose = root.find('idinfo/descript/purpose')
        if purpose is not None:
            text = purpose.text
            if text.find("xxFYxx") >= 0:
                purpose.text = text.replace("xxFYxx", fy)

        # Update process steps
        procstep = root.findall('dataqual/lineage/procstep')
        if procstep:
            for child in procstep:
                for subchild in child.iter('procdesc'):
                    text = subchild.text
                    if text.find('xxTODAYxx') >= 0:
                        text = text.replace("xxTODAYxx", d.strftime('%Y-%m-%d'))
                    if text.find("xxSTATExx") >= 0:
                        text = text.replace("xxSTATExx", state)
                    if text.find("xxFYxx") >= 0:
                        text = text.replace("xxFYxx", fy)
                    if text.find("xxRESxx") >= 0:
                        text = text.replace('xxRESxx', resolution)
                    if text.find("xxDBxx") >= 0:
                        text = text.replace('xxDBxx', db)
                    if text.find("xxTOOLxx") >= 0:
                        text = text.replace('xxTOOLxx', tool)
                    subchild.text = text

        # Update VAT name
        enttypl = root.find('eainfo/detailed/enttyp/enttypl')
        if enttypl is not None:
            text = enttypl.text
            if text.find("xxNAMExx") >= 0:
                enttypl.text = text.replace(
                    "xxNAMExx", os.path.basename(target))

        # Update OS, ESRI, Python system information
        native = root.find('idinfo/native')
        if native is not None:
            text = native.text
            if text == "xxENVxx":
                native.text = sys_env
        envirDesc = root.find('dataIdInfo/envirDesc')
        if envirDesc is not None:
            text = envirDesc.text
            if text == "xxENVxx":
                envirDesc.text = sys_env

        # update raster resoluton
        stepDesc = root.find('dqInfo/dataLineage/prcStep/stepDesc')
        if stepDesc is not None:
            text = stepDesc.text
            if text.find('xxRESxx') >= 0:
                text = text.replace('xxRESxx', resolution)
            if text.find("xxDBxx") >= 0:
                text = text.replace('xxDBxx', db)
            if text.find("xxTOOLxx") >= 0:
                text = text.replace('xxTOOLxx', tool)
            stepDesc.text = text

        # Update database information
        formname = root.find('distinfo/stdorder/digform/digtinfo/formname')
        if formname is not None:
            if formname.text == "xxDBxx":
                formname.text = db
        formvern = root.find('distinfo/stdorder/digform/digtinfo/formvern')
        if formvern is not None:
            if formvern.text == "xxVERxx":
                formvern.text = ver

        formatName = root.find('distInfo/distributor/distorFormat/formatName')
        if formatName is not None:
            if formatName.text == "xxDBxx":
                formatName.text = db
        formatVer = root.find('distInfo/distributor/distorFormat/formatVer')
        if formatVer is not None:
            if formatVer.text == "xxVERxx":
                formatVer.text = ver

        #  create new xml file which will be imported, 
        # thereby updating the table's metadata
        tree.write(
            meta_import, 
            encoding="utf-8", 
            xml_declaration=None, 
            default_namespace=None, 
            method="xml")

        # Save changes
        meta_src = arcpy.metadata.Metadata(target)
        meta_src.importMetadata(meta_import, "FGDC_CSDGM")
        meta_src.deleteContent('GPHISTORY')
        meta_src.save()

        # delete the temporary xml metadata file
        # if os.path.isfile(meta_import):
        #     os.remove(meta_import)
        return True

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False

        
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
        v = '1.2'
        arcpy.AddMessage(f"\nImport Raster FGDB, {v = !s}")

        out_p = args[0]
        input_r = args[1]
        st = args[2]
        fy = args[3]
        module_p = args[4]

        cell_r = 10
        env.cellSize = cell_r
        mem_r = 'memory/tmpRSS'
        rast_n = f"MURASTER_{cell_r}m_{st}_{fy}"
        out_r = f"{out_p}/{rast_n}"

        #resampling method
        env.resamplingMethod = "NEAREST"

        #spatial reference
        out_sr = arcpy.SpatialReference(5070)
        env.outputCoordinateSystem = out_sr

        rast_d = arcpy.Describe(input_r)
        input_sr = rast_d.spatialReference

        if input_sr.factoryCode == 5070:
            rast_ext = rast_d.extent
            tm = None
            project = False
        else:
            project = True
            if input_sr.GCS.name == "GCS_WGS_1984":
                tm = "WGS_1984_(ITRF00)_To_NAD_1983"
            else:
                tm = None
            # convert extent object to polygon and project to 5070
            poly_ext = rast_d.extent.polygon
            poly_ext_5070 = poly_ext.projectAs(out_sr, tm)
            rast_ext = poly_ext_5070.extent

        # set raster extent
        rast_lr = rast_ext.lowerRight
        rast_ul = rast_ext.upperLeft
        rast_lrx = extCoord(rast_lr.X, cell_r, 5)
        rast_lry = extCoord(rast_lr.Y, cell_r, 5)
        rast_ulx = extCoord(rast_ul.X, cell_r, 5)
        rast_uly = extCoord(rast_ul.Y, cell_r, 5)
        rast_ext = arcpy.Extent(rast_ulx, rast_lry, rast_lrx, rast_uly)
        # Set environment to new extent.
        env.extent = rast_ext
        
        # Specify resampling method
        # if rast_d.meanCellHeight < cell_r or rast_d.meanCellWidth < cell_r:
        #     # if input less than ouput cell size, use majority
        #     resamp = "MAJORITY" # this gets awkward with unprojected cell size
        #     project = True
        # else:
        resamp = "NEAREST"

        # Project raster if necessary
        if project:
            try:
                arcpy.AddMessage(
                    "\tProjecting input raster with projection "
                    f"{input_sr.name}: {input_sr.factoryCode} to \n"
                    f"\t\t{out_sr.name}: {out_sr.factoryCode}"
                )
                arcpy.management.ProjectRaster(
                    input_r, mem_r, out_sr, resamp, cell_r, tm
                )
            # try again if not enough memory but write temp copy out to gdb
            except MemoryError:
                arcpy.AddMessage('\t\tMemory error, trying something else')
                mem_r = f"{out_p}/tempMURASTER"
                arcpy.management.Delete("memory")
                arcpy.management.ProjectRaster(
                    input_r, mem_r, out_sr, resamp, cell_r, tm
                )
            arcpy.management.CopyRaster(
                mem_r, out_r, None, None, None, None, None, "32_BIT_UNSIGNED"
            )
            arcpy.management.Delete(mem_r)
        else:
            arcpy.management.CopyRaster(
                input_r, out_r, None, None, None, None, None, "32_BIT_UNSIGNED"
            )
        
        # for unknown reason even though 5070 was specified above, these 
        # rasters not receiving the factor code (EPSG)
        arcpy.management.DefineProjection(
            out_r,
            ('PROJCS["NAD_1983_Contiguous_USA_Albers",'
             'GEOGCS["GCS_North_American_1983",'
             'DATUM["D_North_American_1983",'
             'SPHEROID["GRS_1980",6378137.0,298.257222101]],'
             'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],'
             'PROJECTION["Albers"],PARAMETER["False_Easting",0.0],'
             'PARAMETER["False_Northing",0.0],'
             'PARAMETER["Central_Meridian",-96.0],'
             'PARAMETER["Standard_Parallel_1",29.5],'
             'PARAMETER["Standard_Parallel_2",45.5],'
             'PARAMETER["Latitude_Of_Origin",23.0],UNIT["Meter",1.0]]')
        )

        # Rename Band_1, for some reason this doesn't display for rasters
        # fgdb, yet when you export them the band name shows up. 
        rast = arcpy.Raster(out_r)
        if 'MUKEY' not in rast.bandNames:
            rast.renameBand('Band_1', 'MUKEY')
        del rast

        meta_b = UpdateMetadata(out_p, out_r, "10m", module_p, v, st)
        if meta_b:
            arcpy.AddMessage(f"\t{rast_n} metadata successfully updated")
        else:
            arcpy.AddWarning(f"\t{rast_n} metadata unsuccessfully updated")
        return rast_n

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
