#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One of three scripts called by the Create RSS Datasets tool
from the RSS SSURGO Export Tool arctoolbox
This tool creates file geodatabse RSS database.
Created on: 09/19/2024

@author: Alexander Stum
@maintainer: Alexander Stum
    @title:  GIS Specialist & Soil Scientist
    @organization: National Soil Survey Center, USDA-NRCS
    @email: alexander.stum@usda.gov

@modified 07/08/2026
    @by: Alexnder Stum
@version: 2.0

# --- Updated 07/08/2026, v 2.0
- This version produces RSS version 2.1. Version 2.0 had the inclusion 
of the SARASTER and PARASTER. 2.1 has refined and complete metadata and 
adds a Spatial Version column to MURASTER.
- Adapted to incorporate PARASTER and SARASTER
- Adapted updateMetadata to successively update metadata and populates
many new elements
# ---
The orginal tool this is based off of is from the ArcMap Desktop toolbox
ArcGIS Desktop Build RSS gdb: Create RSS DB by Map. This tool creates a 
RSS databse which uses the gSSURGO template, excluding the vector features.
Each text file is imported and relationships and indices made.

ImportXMLWorkspaceDocument command requires Standard or Advanced license
"""
v = '2.0'
rss_v = '2.1'

# Import system modules

import csv
from datetime import datetime
import itertools as it
import os
from pathlib import Path
import platform
import shutil
import sys
import traceback
import xml.etree.cElementTree as ET
from typing import Any, Callable, TypeVar

from arcpy import env
import arcpy


Tist = TypeVar("Tist", tuple, list)


def pyErr(func: str) -> str:
    """When a python exception is raised, this funciton formats the traceback
    message.

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

def funYield(
        fn: Callable, iterSets: Tist, #[dict[str, Any]]
        constSets: dict[str, Any]
    ) : # -> Generator[list[int, str]]
    """Iterativley calls a function as a generator

    Parameters
    ----------
    fn : Callable
        The function to be called as a generator
    iterSets : Tist[dict[str, Any],]
        These dictionaries are a set of dynmaic variables for each iteration. 
        The keys must align with the ``fn`` parameters. The values the 
        arguments for the function call.
    constSets : dict[str, Any]
        This dictionary is composed of the static variables sent as 
        arguments to function call ``fn``. 
        The keys must align with the ``fn`` parameters.

    Yields
    ------
    Generator[int, str]
        If successful, yields the value 0 and an empty string, otherwise
        yields the value 2 with a string message. This generator can be
        modified to yield the returned items from the function ``fn``.
    """
    try:
        # turns list, tuple, str into an iterator
        fn_inputs = iter(iterSets)
        # initialize first set of processes
        # islice slices iterables without copying it
        outputs = {
            fn(**params, **constSets): params for params in fn_inputs
        }
        output, param = outputs.popitem()
        yield [0, output]

    except:
        func = sys._getframe().f_code.co_name
        msgs = pyErr(func)
        yield [2, msgs]


def createGDB(gdb_p: str, module_p: str) -> str:
    """Creates the SSURGO file geodatabase using an xml workspace file to 
    create tables, features, and relationships.

    Parameters
    ----------
    gdb_p : str
        The path of the SSURGO file geodatabase to be created.
    module_p : str
        Module path to access xml

    Returns
    -------
    str
        An empty string if successful, an error message if unsuccessful.

    """
    try:
        outputFolder = os.path.dirname(gdb_p)
        gdb_n = os.path.basename(gdb_p)
        inputXML = module_p + '/RSS_gSSURGO_template.xml'
        if not os.path.isfile(inputXML):
            arcpy.AddError(" \nMissing xml file in tool folder: " + inputXML)
            return False

        if arcpy.Exists(gdb_p):
            arcpy.AddMessage(f"\tDeleting existing file gdb {gdb_p}")
            arcpy.management.Delete(gdb_p)
        arcpy.AddMessage(f"\tCreating new geodatabase ({gdb_n}) in "
                         f"{outputFolder}\n")

        arcpy.management.CreateFileGDB(outputFolder, gdb_n)
        if not arcpy.Exists(gdb_p):
            arcpy.AddError("Failed to create new geodatabase")
            return False
        # The following command will fail when the user only has a Basic license
        arcpy.management.ImportXMLWorkspaceDocument(
            gdb_p, inputXML, "SCHEMA_ONLY"
        )

        env.workspace = gdb_p
        tblList = arcpy.ListTables()
        if len(tblList) < 50:
            arcpy.AddError(f"Output geodatabase has only {len(tblList)} tables")
            return False

        return True

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def importCoint( 
              input_p: str, 
              gdb_p: str, 
              table_d: dict[list[str, str, list[tuple[int, str]]]],
              ) -> str:
    """Runs through each SSURGO download folder and imports the rows into the 
    specified cointerp table . This table has unique information from each 
    survey area. This funciton is only called for gSSURGO 1.0 builds.

    Parameters
    ----------
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase
    table_d : dict[list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.

    Returns
    -------
    str
        An empty string if successful, otherwise and error message.
    """
    try:
        arcpy.env.workspace = gdb_p
        csv.field_size_limit(2147483647)
        table = 'cointerp'
        tab_p = f"{gdb_p}/{table}"
        cols = table_d[table][2]
        # get fields in sequence order
        cols.sort()
        fields = [f[1] for f in cols]
        iCur = arcpy.da.InsertCursor(tab_p, fields)
        
        # Make file path for text file
        txt_p = f"{input_p}/cinterp.txt"
        if not os.path.exists(txt_p):
            return f"{txt_p} does not exist"
        with open(txt_p, 'r') as txt_f:
            csvReader = csv.reader(
                txt_f, delimiter='|', quotechar='"'
            )

            for row in csvReader:
                if row[1] == row[4] or row[1] == "54955":
                    # Slice out excluded elements
                    row = row[:7] + row[11:13] + row[15:]
                    # replace empty sets with None
                    iCur.insertRow(tuple(v or None for v in row))

        del csvReader, iCur
        arcpy.AddMessage(f"\t\tSuccessfully populated {table}")
        return 0 # None

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return 1 # arcpyErr(func)
    except:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return 1 # pyErr(func)


def importList(
              input_p: str, 
              gdb_p: str, 
              table_d: dict[list[str, str, list[tuple[int, str]]]],
              table: str
              ) -> int:
    """Runs through the tabular folder and imports the rows into the 
    specified ``table`` . These tables have unique information from each 
    survey area.

    Parameters
    ----------
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase
    table_d : dict[list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.
    table : str
        Table that is being imported.

    Returns
    -------
    int
        An empty string if successful, otherwise and error message.
    """
    try:
        arcpy.env.workspace = gdb_p
        csv.field_size_limit(2147483647)
        txt = table_d[table][0]
        cols = table_d[table][2]
        tab_p = f"{gdb_p}/{table}"
        # get fields in sequence order
        cols.sort()
        fields = [f[1] for f in cols]
        iCur = arcpy.da.InsertCursor(tab_p, fields)
            # Make file path for text file
        txt_p = f"'{input_p}/{txt}.txt'"
        # in some instances // can create special charaters with eval
        txt_p = txt_p.replace('\\', '/')
        # convert latent f strings
        txt_p = eval("f" + txt_p)
        
        if not os.path.exists(txt_p):
            return f"{txt_p} does not exist"
        with open(txt_p, 'r') as txt_f:
            csvReader = csv.reader(
                txt_f, delimiter='|', quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                iCur.insertRow(tuple(v or None for v in row))

        del csvReader, iCur
        arcpy.AddMessage(f"\t\tSuccessfully populated {table}")
        return 0 # None

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return 1 # arcpyErr(func)
    except:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return 1 # pyErr(func)


def importSet(
              input_p: str, 
              gdb_p: str, 
              table_d: dict[str, list[str, str, list[tuple[int, str]]]]
    ) -> str:
    """Runs through the tabular folder and compiles a set of unique 
    values to insert into respective tables. These tables are largely common 
    to all surveys but some states have rows unique to their surveys.

    Parameters
    ----------
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase
    table_d : dict[list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.

    Returns
    -------
    str
        An empty string if successful, otherwise and error message.
    """
    try:
        csv.field_size_limit(2147483647)
        # 'distsubinterpmd'
        tabs_l = ['distinterpmd', 'sdvattribute', 'sdvfolderattribute']
        arcpy.env.workspace = gdb_p
        
        for table in tabs_l:
            txt = table_d[table][0]
            cols = table_d[table][2]
            tab_p = f"{gdb_p}/{table}"
            # get fields in sequence order
            cols.sort()
            fields = [f[1] for f in cols]
            iCur = arcpy.da.InsertCursor(tab_p, fields)
            row_s = set()
            txt_p = f"{input_p}/{txt}.txt"
            if not os.path.exists(txt_p):
                return f"{txt_p} does not exist"
            with open(txt_p, 'r') as txt_f:
                csvReader = csv.reader(
                    txt_f, #encoding="utf-8"
                    delimiter = '|', 
                    quotechar = '"'
                )
                for row in csvReader:
                    iCur.insertRow(tuple(v or None for v in row))
        del iCur
        return ''

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        return arcpy.AddError(arcpyErr(func))
    except:
        flds = iCur.fields
        arcpy.AddError(f"In table2: {table}: {flds}")
        
        for i, col in enumerate(row):
            arcpy.AddMessage(f"\n{flds[i]}: {len(col)}\n\t{col}")
        try:
            # arcpy.AddMessage(cols)
            # arcpy.AddMessage(txt)
            # for i, e in enumerate(row):
            #     if e:
            #         size = len(e)
            #     else:
            #         size = 0
                # arcpy.AddMessage(f"{fields[i]}: {size}")
            del iCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        return arcpy.AddError(pyErr(func))
        
        
        
def importSing(input_p: str, gdb_p: str) -> dict:
    """Import the tables that are common for each SSURGO download 
    Also creates a table dictionary that with the table information.

    Parameters
    ----------
    input_p : str
        Path to the SSRUGO downloads
    gdb_p : str
        Path of the SSURGO geodatabase

    Returns
    -------
    dict
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name. If the function 
        returns in error the dictionary will return wiht the key 'Error' 
        and a message.
    """
    try:
        # First read in mdstattabs: mstab table into 
        # There should be 75 tables, 6 of which are spatial, so 69
        # Then read tables from gdb
        # Copy common tables and report unused
        # Then import the common tables
        tn = 69
        csv.field_size_limit(2147483647)
        tabs_common = [
            'mdstattabcols', 'mdstatrshipdet', 'mdstattabs', 'mdstatrshipmas',
            'mdstatdommas', 'mdstatidxmas', 'mdstatidxdet',  'mdstatdomdet',
            'sdvfolder', 'sdvalgorithm'
        ]

        arcpy.env.workspace = gdb_p
        txt_p = f"{input_p}/mstab.txt"
        if not os.path.exists(txt_p):
            table_d = {'Error': (f"{txt_p} does not exist", '', [])}
            return table_d
        csvReader = csv.reader(
            open(txt_p, 'r', encoding='utf8'), delimiter='|', quotechar='"'
        )
        
        # dict{Table Physical Name: 
        # [text file, Table Label, [(seq, column names)]]}
        table_d = {t[0]: [t[4], t[2], []] for t in csvReader}
        # Retrieve column names
        txt_p = f"{input_p}/mstabcol.txt"
        if not os.path.exists(txt_p):
            table_d = {'Error': f"{txt_p} does not exist"}
            return table_d
        csvReader = csv.reader(
            open(txt_p, 'r', encoding='utf8'), delimiter='|', quotechar='"'
        )
        for row in csvReader:
            table = row[0]
            if table in table_d:
                # add tuple with sequence (as int to sort) and column name
                table_d[table][2].append((int(row[1]), row[2]))
        
        # Populate static tables
        for table in tabs_common:
            txt = table_d[table][0]
            cols = table_d[table][2]
            tab_p = f"{gdb_p}/{table}"
            # get fields in sequence order
            cols.sort()
            fields = [f[1] for f in cols]

            iCur = arcpy.da.InsertCursor(tab_p, fields)
            txt_p = f"{input_p}/{txt}.txt"
            if not os.path.exists(txt_p):
                table_d = {'Error': f"{txt_p} does not exist"}
                return table_d
            csvReader = csv.reader(
                open(txt_p, 'r', encoding='utf8'), 
                delimiter = '|', 
                quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                iCur.insertRow(tuple(v or None for v in row))
            del iCur
            # Populate the month table
            months = [
                (1, 'January'), (2, 'February'), (3, 'March'), (4, 'April'),
                (5, 'May'), (6, 'June'), (7, 'July'), (8, 'August'),
                (9, 'September'), (10, 'October'), (11, 'November'),
                (12, 'December')
            ]
            month_p = f"{gdb_p}/month"
            iCur = arcpy.da.InsertCursor(month_p, ['monthseq', 'monthname'])
            for month in months:
                iCur.insertRow(month)
            del iCur

        return table_d

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
        except:
            pass
        func = sys._getframe().f_code.co_name
        table_d['Error'] = (arcpy.AddError(arcpyErr(func)), '', [])
        return table_d
    except:
        try:
            del iCur
        except:
            pass
        try:
            arcpy.AddError(f'While working with {txt_p} and {table}')
            arcpy.AddError(f"{row= }")
        except:
            pass
        func = sys._getframe().f_code.co_name
        table_d['Error'] = (arcpy.AddError(arcpyErr(func)), '', [])
        return table_d


def updateMetadata(prev_gdb_p: str,
                   new_gdb_p: str,
                   input_p: str,
                   st: str,
                   fy: str
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
    input_p : str
        Directory locatoin of the tabular export package.
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
        gdb_n = os.path.basename(new_gdb_p)[:-4]
        msgAppend = msg.append

        # timing
        fyi = int(fy)
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
        # initial metadata exported from current target featureclass
        meta_export = arcpy.env.scratchFolder + f"/xxExport_{gdb_n}.xml"
        # the metadata xml that will provide the updated info
        meta_import = arcpy.env.scratchFolder + f"/xxImport_{gdb_n}.xml"
        # Cleanup XML files from previous runs
        if os.path.isfile(meta_import):
            os.remove(meta_import)
        if os.path.isfile(meta_export):
            os.remove(meta_export)

        if prev_gdb_p:
            meta_src = arcpy.metadata.Metadata(prev_gdb_p)
        else:
            meta_src = arcpy.metadata.Metadata(new_gdb_p)
        meta_src.exportMetadata(meta_export, "ISO19115_3")

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
                        word.text = state
                    elif word.text == 'xxSTATE':
                        word.text = st
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
        vers_d = versionTab(input_p, new_gdb_p, st, fy, pub_dt)
        env = (f"{vers_d['ESRI'][1]} {vers_d['ESRI'][2]}; "
            f"{vers_d['Python'][1]} {vers_d['Python'][2]}")
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
        title.text = f"{title.text[:38]}{state} {fy}"
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
        series_id.text = rss_v

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

        # create new xml file which will be imported, 
        # thereby updating the table's metadata
        tree.write(
            meta_import, 
            encoding = "utf-8", 
            xml_declaration = None, 
            default_namespace = None, 
            method = "xml"
        )
        if prev_gdb_p:
            meta_dest = arcpy.metadata.Metadata(new_gdb_p)
            meta_dest.importMetadata(meta_import, "ISO19115_3")
            meta_dest.deleteContent('GPHISTORY')
            meta_dest.save()
            del meta_dest
        else:
            # Source was the new FGDB and therefore also the destination
            meta_src.importMetadata(meta_import, "ISO19115_3")
            meta_src.deleteContent('GPHISTORY')
            meta_src.save()
            # meta_src.synchronize('SELECTIVE')

        # delete the temporary xml metadata files
        if os.path.isfile(meta_import):
            os.remove(meta_import)
        if os.path.isfile(meta_export):
            os.remove(meta_export)
        del tree, meta_src

        return msg
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(arcpyErr(func)))
        return msg
    except:
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(pyErr(func)))
        return msg


def gSSURGO(input_p: str,
            prev_gdb_p: str,
            gdb_p: str,
            module_p: str,
            st: str,
            fy: int
    ) -> str:
    """This function is the backbone of the module. 
    It calls these functions to create and populate a gSSURGO geodatabase: 
    1) ``CreateGDB`` to create a geodatabase using an xml template
    2) ``importSing`` imports tabels that are idential in each SSURGO folder.
    3) ``importSet`` imports tabels that are largely indentical, with some
    novelty.
    4) ``importList`` imports tabels with unique information to each SSURGO
    dataset.
    5) ``createTableRelationships`` Establishes relationships between tables
    to other tables or spatial features.
    6) ``updateMetadata`` Update the geodatabase and spatial features 
    metadata.

    Parameters
    ----------
    input_p : str
        Directory locatoin of the tabular export package.
    prev_gdb_p : str
        The path of last fy's RSS file geodatabase, if first year empty string.
    gdb_p : str
        The path of the RSS file geodatabase to be created.
    module_p : str
        The module tool directory with the xml files.
    st : str
        State abreviation
    fy : int
        fiscal year of publication

    Returns
    -------
    str
        Returns an empty string if a SSURGO geogdatabase is successfully 
        created, otherwise returns an error message.
    """
    try:
        env.overwriteOutput= True
        gdb_n = os.path.basename(gdb_p)
        gdb_n = gdb_n.replace("-", "_")
        # Get the XML Workspace Document appropriate for the specified aoi
              # %% check 1
        # ---- call createGDB
        gdb_b = createGDB(gdb_p, module_p)
        if not gdb_b:
            arcpy.AddMessage(f"Didn't successfully create {gdb_n}\n")
            return False

        # ---- call importSing
        arcpy.SetProgressorLabel("Importing constant tables")
        table_d = importSing(input_p, gdb_p)
        if 'Error' in table_d:
            arcpy.AddError(table_d['Error'])
            return
        arcpy.SetProgressorLabel("Importing table sets")
        msg = importSet(input_p, gdb_p, table_d)
        if msg:
            arcpy.AddError(msg)
            return
        # Tables which are unique to each SSURGO soil survey area
        arcpy.SetProgressorLabel("Importing unique tables")
        tabs_uniq = [
            'component', 'cosurfmorphhpp', 'legend', 'chunified','cocropyld',
            'chtexturegrp', 'cosurfmorphss', 'coforprod', 'sacatalog',
            'cosurfmorphgc', 'cotaxmoistcl', 'chtext', 'chconsistence',
            'chtexture', 'copmgrp', 'cosoilmoist', 'mucropyld', 'chtexturemod',
            'cotext', 'coecoclass', 'cosurfmorphmr', 'cosurffrags',
            'cotreestomng', 'cosoiltemp', 'sainterp', 'chstructgrp',
            'distlegendmd', 'copwindbreak', 'chdesgnsuffix', 'corestrictions',
            'cotaxfmmin', 'chstruct', 'chfrags', 'coforprodo', 'distmd',
            'mutext', 'legendtext', 'muaggatt', 'chorizon', 'cohydriccriteria',
            'chpores', 'chaashto', 'coerosionacc', 'copm', 'comonth',
            'muaoverlap', 'cotxfmother', 'mapunit', 'coeplants', 'laoverlap',
            'cogeomordesc', 'codiagfeatures', 'cocanopycover'
        ]
        # Exclude these cointerp columns
        # interpll, interpllc, interplr, interplrc, interphh, interphhc
        exclude_i = {8, 9, 10, 11, 14, 15}
        table_d['cointerp'][2] = [
            cols for cols in table_d['cointerp'][2] if cols[0] not in exclude_i
        ]

        co_out = importCoint(input_p, gdb_p, table_d)
        if co_out:
            arcpy.AddError(co_out)
            return False

        # Create parameter dictionary with gdb table name and text file folder
        paramSet = [
            {'table': tab} for tab in tabs_uniq
        ]
        constSet = {
            'input_p': input_p, 
            'gdb_p': gdb_p, 
            'table_d': table_d
        }
        # for tab in tabs_uniq:
        #     y = importList(table=tab, **constSet)

        import_jobs = funYield(importList, paramSet, constSet)
        for yld, output in import_jobs:
            try:
                if output:
                    arcpy.AddError(output)
                    return
            except GeneratorExit:
                arcpy.AddWarning("passed")
                # arcpy.AddWarning(f"{paramBack}")
                arcpy.AddWarning(f"{output}")
                pass
        # import_jobs.close()
        del import_jobs
        # gc.collect()

        # Create Indices
        if not createIndices(gdb_p, module_p):
            arcpy.AddWarning(
                "Failed to create indices which may imparct efficient use of "
                "database."
            )

        # Create table relationships and indexes
        # ---- call createTableRelationships
        rel_b = createTableRelationships(gdb_p)
        if not rel_b:
            return False
        
        # Update metadata for the geodatabase and all featureclasses
        arcpy.SetProgressorLabel("Updating metadata...")

        msgs = updateMetadata(prev_gdb_p, gdb_p, input_p, st, str(fy))
        if msgs:
            for msg in msgs:
                arcpy.AddError(msg)

        arcpy.SetProgressorLabel("\tCompacting new database...")
        arcpy.Compact_management(gdb_p)

        env.workspace = os.path.dirname(env.scratchFolder)
        arcpy.AddMessage(f"\t{gdb_p} was successfully created")
        return True

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def createIndices(gdb_p: str, module_p: str) -> bool:
    """Creates attribute indices for the specified table attribute fields.
    As any field involved with a Relationship Class is already indexed,
    therefore the  mdstatidxdet and mdstatidxmas tables are not referenced. 
    Instead, a consolidated csv file, relative to the gSSURGO version, 
    is referenced.

    Parameters
    ----------
    gdb_p : str
        The geodatabase path.
    module_p : str
        The path to the sddt module.

    Returns
    -------
    bool
        Returns True if all indices were successfully created, otherwis 
        False.
    """
    try:
        arcpy.AddMessage('\n\tAdding attribute indices...')
        # Any field involved with a Relationship Class is already indexed
        csv_p = module_p + "/md_index_insert1.csv"
        with open(csv_p, newline='', encoding='utf8') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            # Sequence, Unique, ascending are irrelavent in FGDB's
            arcpy.SetProgressorLabel("Creating indexes")
            for tab_n, idx_n, seq, col_n, uk in csv_r:
                if uk == 'Yes':
                    un_b = "UNIQUE"
                else:
                    un_b = "NON_UNIQUE"
                tab_p = f"{gdb_p}/{tab_n}"
                arcpy.management.AddIndex(tab_p, col_n, idx_n, un_b)
        return True
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(f"{tab_p= } {col_n= } {un_b= }")
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def createTableRelationships(gdb_p: str) -> str:
    """Creates the tabular relationships between the SSRUGO tables using arcpy
    CreateRelationshipClass function. These relationship classes are defined in 
    the mdstatrshipdet and mdstatrshipmas metadata tables. Note that the 
    seven spatial relationships classes were inherited from the xml workspace.

    Parameters
    ----------
    gdb_p : str
        The path of the new geodatabase with the recently imported SSURGO 
        tables.

    Returns
    -------
    str
        An empty string if successful, an error message if unsuccessful.

    """
    try:
        arcpy.AddMessage(
            "\tCreating table relationships on key fields..."
        )
        env.workspace = gdb_p

        if (arcpy.Exists(f"{gdb_p}/mdstatrshipdet")
            and arcpy.Exists(f"{gdb_p}/mdstatrshipmas")):
            tbl1 = f"{gdb_p}/mdstatrshipmas"
            tbl2 = f"{gdb_p}/mdstatrshipdet"
            flds1 = ['ltabphyname', 'rtabphyname']
            flds2 = [
                'ltabphyname', 'rtabphyname', 'ltabcolphyname', 'rtabcolphyname'
            ]
            # Create a set of all table to table relations in mdstatrshipmas
            sCur = arcpy.da.SearchCursor(tbl1, flds1)
            relSet = {(ltab, rtab) for ltab, rtab in sCur}
            del sCur
            # if table to table relationship defined in mdstatrshipmas, then 
            # create relationship with column names from mdstatrshipdet
            sCur = arcpy.da.SearchCursor(tbl2, flds2)
            for ltab, rtab, lcol, rcol in sCur:
                if (ltab, rtab) in relSet:
                    # left table: Destination table
                    # left column: Destination Foreign Key
                    # right table: Origin Table
                    # right column: Origin Primary Key
                    rel_n = f"z_{ltab.lower()}_{rtab.lower()}"
                    # create Forward Label i.e. "> Horizon AASHTO Table"
                    fwdLabel = f"on {lcol}"
                    # create Backward Label i.e. "< Horizon Table"
                    backLabel = f"on {rcol}"
                    arcpy.SetProgressorLabel(
                        "Creating table relationship "
                        f"between {ltab} and {rtab}"
                    )
                    arcpy.management.CreateRelationshipClass(
                        f"{gdb_p}/{ltab}", f"{gdb_p}/{rtab}", rel_n, "SIMPLE",
                        fwdLabel, backLabel, "NONE", "ONE_TO_MANY", "NONE",
                        lcol, rcol
                    )
            del sCur
            
            return True
        else:
            return("Missing mdstatrshipmas and/or mdstatrshipdet tables,"
                   "relationship classes not created")
    except arcpy.ExecuteError:
        try:
            del sCur
        except:
            pass
        arcpy.AddMessage(
            f"{gdb_p}/{rtab}, {gdb_p}/{ltab}, {rel_n}, SIMPLE, "
            f"{fwdLabel}, {backLabel}, NONE, ONE_TO_MANY, NONE, {rcol}, {lcol}"
        )
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        try:
            del sCur
        except:
            pass
        arcpy.AddMessage(
            f"{gdb_p}/{rtab}, {gdb_p}/{ltab}, {rel_n}, SIMPLE, "
            f"{fwdLabel}, {backLabel}, NONE, ONE_TO_MANY, NONE, {rcol}, {lcol}"
        )
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def versionTab(input_p: str, gdb_p: str, st: str, fy: str, pub_dt: str) -> bool:
    """"This tool populates the version table with the ESRI software version,
    operating system version, python version, SSURGO version, gSSURGO version,
    script tool version, RSS version and File Geodatabase version.

    Parameters
    ----------
    input_p : str
        Directory of the SSURGO Download
    gdb_p : str
        Path of the newly created gSSURGO file geodatabase

    Returns
    -------
    bool
        returns true if version table successfully created, otherwise false.
    """
    try:
        gssurgo_v = '1.0'
            # populate version table
        txt_p = f"{input_p}/version.txt"
        if not os.path.exists(txt_p):
            ssurgo_v = 'NA'
        else:
            csvReader = csv.reader(
                open(txt_p, 'r', encoding='utf8'), delimiter='|', quotechar='"'
            )
            ssurgo_v = next(csvReader)[0]
            del csvReader
        esri_i = arcpy.GetInstallInfo()
        # File Geodatabase version
        # https://pro.arcgis.com/en/pro-app/latest/arcpy/functions/
        # workspace-properties.htm
        gdb_v = arcpy.Describe(gdb_p).release
        if gdb_v == '3,0,0':
            gdb_v = '10.0'
        version_d = {
            'ssurgo': ('Data Source', 'SSURGO', ssurgo_v),
            'gSSURGO': ('Data Model', 'gSSURGO', gssurgo_v),
            'OS': (
                'Operating System', "Microsoft " + platform.system(),
                platform.version()
            ),
            'ESRI': (
                'GIS application', 'ESRI: ' + esri_i['ProductName'],
                esri_i['Version']
            ),
            'Python': (
                'Prgramming language', 'Python', platform.python_version()
            ),
            'FGDB': ('Database format', 'File Geodatabase', gdb_v),
            'script': (
                'Script', 'Create RSS Datasets', v
            ),
            'RSS': ('Series', 'Raster Soil Survey', rss_v),
            'edit': ('Edition', f"{st} {fy}", pub_dt[:10])
        }

        version_d['abbrev1'] = ('Abbreviation Level', 'cointerp', '0.5')

        version_p = f"{gdb_p}/version"
        iCur = arcpy.da.InsertCursor(version_p, ['type', 'name', 'version'])
        for vals in version_d.values():
            iCur.insertRow([*vals])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated version table")
        return version_d
    
    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def main(args: list[str, str, str, int, str, str])-> str:
    """This function directs the creation of a RSS dataset

    Parameters
    ----------
    args : list[str, str, str, int, str, str]
        This tool needs
            - Input tabular folder with all the exported text files from NASIS
            - The output folder where the RSS dataset will be created.
            - State abbreviation used in metadata and naming files
            - Fiscal year of publication
            - Previous fy's gdb path or None if first year
            - Module path to access xml and other helper files.

    Returns
    -------
    str
        Then name of the RSS database (file geodatabase) if successful, 
        empty string otherwise
    """
    # %% m
    try:
        arcpy.AddMessage("Creating RSS SSURGO File GDB, version: " + v)
        # location of SSURGO datasets containing SSURGO downloads
        input_p = args[0] # 0: input folder of the tabular data
        output_p = args[1] # output path
        st = args[2] # State
        fy = args[3] # fiscal year
        prev_gdb_p = args[4]
        module_p = args[5] # module path
       
        licenseLevel = arcpy.ProductInfo().upper()
        if licenseLevel == "BASIC":
            arcpy.AddError(
                "ArcGIS License level must be Standard or Advanced "
                "to run this tool"
            )
            return False

        # Create new state directory inside of `output_p`
        new_p = f"{output_p}/{st}"
        if os.path.exists(new_p):
            shutil.rmtree(new_p)
        # os.mkdir(new_p)
        Path(f'{new_p}/RSS_{st}/spatial').mkdir(parents=True, exist_ok=True)
        
        gdb_p = f"{new_p}/RSS_{st}.gdb"

        gdb_b = gSSURGO(input_p, prev_gdb_p, gdb_p, module_p, st, fy)

        if gdb_b:
            return gdb_p
        else:
            return ''

    except arcpy.ExecuteError:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return ''
    except:
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return ''

if __name__ == '__main__':
    main(sys.argv[1:])