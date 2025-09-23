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

@modified 09/19/2025
    @by: Alexnder Stum
@version: 1.2

# ---
The orginal tool this is based off of is from the ArcMap Desktop toolbox
ArcGIS Desktop Build RSS gdb: Create RSS DB by Map. This tool creates a 
RSS databse which uses the gSSURGO template, but not vector features
are created. Each text file is imported and relationships and indices made.

ImportXMLWorkspaceDocument command requires Standard or Advanced license
"""

# Import system modules

import csv
import datetime
import gc
import itertools as it
import os
import platform
import shutil
import sys
import time
import traceback
import xml.etree.cElementTree as ET
from typing import Any, Callable, TypeVar

import arcpy
from arcpy import env

Tist = TypeVar("Tist", tuple, list)


class xml:
    def __init__(self, aoi: str, path: str, gssurgo_v: str):
        self.path = path
        self.aoi = aoi
        self.version = gssurgo_v
        if self.version == '2.0':
            path_i = self.path + '/gSSURGO2_'
        else:
            path_i = self.path + '/gSSURGO1_'
        # Input XML workspace document used to create new gSSURGO schema in 
        # an empty geodatabase
        if aoi == "Lower 48 States":
            self.xml = path_i + "RSS_CONUS_AlbersNAD1983.xml"
        elif aoi == "Hawaii":
            self.xml = path_i + "Hawaii_AlbersWGS1984.xml"
        elif aoi == "Alaska":
            self.xml = path_i + "Alaska_AlbersNAD1983.xml"
        elif aoi == "Puerto Rico and U.S. Virgin Islands":
            self.xml = path_i + "PRUSVI_StateNAD83.xml"
        else:
            self.xml = path_i + "_Geographic_WGS1984.xml"
        self.exist = os.path.isfile(self.xml)


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
        fn_inputs = iter(iterSets)
        # initialize first set of processes
        outputs = {
            fn(**params, **constSets): params
            for params in it.islice(fn_inputs, len(iterSets))
        }
        # output, params = outputs.popitem()
        yield [0, '']

    except:
        arcpy.AddWarning('Better luck next time')
        func = sys._getframe().f_code.co_name
        msgs = pyErr(func)
        yield [2, msgs]


def createGDB(gdb_p: str, inputXML: xml) -> str:
    """Creates the SSURGO file geodatabase using an xml workspace file to 
    create tables, features, and relationships.

    Parameters
    ----------
    gdb_p : str
        The path of the SSURGO file geodatabase to be created.
    imputXML: xml
        An xml class object that has information about the xml workspace to 
        template new file geodatabase.


    Returns
    -------
    str
        An empty string if successful, an error message if unsuccessful.

    """
    try:
        outputFolder = os.path.dirname(gdb_p)
        gdb_n = os.path.basename(gdb_p)

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
            gdb_p, inputXML.xml, "SCHEMA_ONLY"
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
        csvReader = csv.reader(
            open(txt_p, 'r'), delimiter='|', quotechar='"'
        )

        for row in csvReader:
            if row[1] == row[4] or row[1] == "54955":
                # Slice out excluded elements
                row = row[:7] + row[11:13] + row[15:]
                # replace empty sets with None
                iCur.insertRow(tuple(v or None for v in row))

        del csvReader, iCur
        arcpy.AddMessage(f"\tSuccessfully populated {table}")
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
        csvReader = csv.reader(
            open(txt_p, 'r'), delimiter='|', quotechar='"'
        )
        for row in csvReader:
            # replace empty sets with None
            iCur.insertRow(tuple(v or None for v in row))
        del csvReader, iCur
        arcpy.AddMessage(f"\tSuccessfully populated {table}")
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
            csvReader = csv.reader(
                open(txt_p, 'r'), #encoding="utf-8"
                delimiter = '|', 
                quotechar = '"'
            )
            for row in csvReader:
                iCur.insertRow(tuple(v or None for v in row))
                # replace empty sets with None
            #     row_s.add(tuple(v or None for v in row))
            # for row in row_s:
                # iCur.insertRow(row)
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


def updateMetadata(gdb_p: str,
                   survey_i: str,
                   st: str,
                   fy: str
    ) -> list[str]:
    """ Used for featureclass and geodatabase metadata. Does not do individual 
    tables. Reads and edits the original metadata object and then exports the 
    edited version back to the featureclass or geodatabase.

    Parameters
    ----------
    gdb_p : str
        Path of the SSURGO geodatabase.
    survey_i : str
        Summary string of the Survey Area Version date by soil survey.
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
        gdb_n = os.path.basename(gdb_p)
        msgAppend = msg.append

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
        meta_export = env.scratchFolder + f"/xxExport_{gdb_n}.xml"
        # the metadata xml that will provide the updated info
        meta_import = env.scratchFolder + f"/xxImport_{gdb_n}.xml"
        # Cleanup XML files from previous runs
        if os.path.isfile(meta_import):
            os.remove(meta_import)
        if os.path.isfile(meta_export):
            os.remove(meta_export)
        meta_src = arcpy.metadata.Metadata(gdb_p)
        meta_src.exportMetadata(meta_export, 'FGDC_CSDGM')

        # Set date strings for metadata, based upon today's date
        d = datetime.date.today()
        month = d.strftime("%B")
        # ---- call getLastDate
        tbl = gdb_p + "/SACATALOG"
        sqlClause = [None, "ORDER BY SAVEREST DESC"]

        sCur = arcpy.da.SearchCursor(
            tbl, ['SAVEREST'], sql_clause = sqlClause
        )
        row = next(sCur)[0]
        lastDate = row.strftime('%Y%m%d')
        del sCur
        # Parse exported XML metadata file
        # Convert XML to tree format
        tree = ET.parse(meta_export)
        root = tree.getroot()

        # new citeInfo has title.text, edition.text, serinfo/issue.text
        for child in root.findall('idinfo/citation/citeinfo/'):
            if child.tag == "title":
                if child.text.find('xxSTATExx') >= 0:
                    child.text = child.text.replace('xxSTATExx', state)
                if child.text.find('xxFYxx') >= 0:
                    child.text = child.text.replace('xxFYxx', fy)
                # elif place_str != "":
                #     child.text = child.text + " - " + description
            elif child.tag == "edition":
                if child.text == 'xxFYxx':
                    child.text = fy
            elif child.tag == "serinfo":
                for subchild in child.iter('issue'):
                    if subchild.text == "xxFYxx":
                        subchild.text = fy

        # Update place keywords
        ePlace = root.find('idinfo/keywords/place')
        for child in ePlace.iter('placekey'):
            if child.text == "xxSTATExx":
                child.text = state
            elif child.text == "xxSURVEYSxx":
                child.text = survey_i

        # Update credits
        eIdInfo = root.find('idinfo')
        for child in eIdInfo.iter('datacred'):
            # sCreds = child.text
            if child.text.find("xxSTATExx") >= 0:
                child.text = child.text.replace("xxSTATExx", state)
            if child.text.find("xxFYxx") >= 0:
                child.text = child.text.replace("xxFYxx", fy)
            if child.text.find("xxTODAYxx") >= 0:
                child.text = child.text.replace("xxTODAYxx", lastDate)

        # Update Summary
        idDescrip = root.find('idinfo/descript')
        for child in idDescrip.iter('purpose'):
            if child.text.find("xxFYxx") >= 0:
                child.text = child.text.replace("xxFYxx", fy)
            if child.text.find("xxMONTHxx") >= 0:
                child.text = child.text.replace("xxMONTHxx", month)
            if child.text.find("xxSTATExx") >= 0:
                child.text = child.text.replace("xxSTATExx", state)

        procDates = root.find('dataqual/lineage')
        if not procDates is None:
            for child in procDates.iter('procdate'):
                sDate = child.text
                if sDate.find('xxTODAYxx'):
                    child.text = lastDate
        else:
            msgAppend("Process date not found")

        #  create new xml file which will be imported, 
        # thereby updating the table's metadata
        tree.write(
            meta_import, 
            encoding = "utf-8", 
            xml_declaration = None, 
            default_namespace = None, 
            method = "xml"
        )

        # import updated metadata to the geodatabase feature
        meta_src.importMetadata(meta_import, "FGDC_CSDGM")
        meta_src.deleteContent('GPHISTORY')
        meta_src.save()

        # delete the temporary xml metadata files
        if os.path.isfile(meta_import):
            os.remove(meta_import)
        # if os.path.isfile(meta_export):
        #     os.remove(meta_export)
        del meta_src

        return msg
    except arcpy.ExecuteError:
        try:
            tree.write(
                meta_import, 
                encoding = "utf-8", 
                xml_declaration = None, 
                default_namespace = None, 
                method = "xml"
            )
            meta_src.save()
            del meta_src
        except:
            pass
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(arcpyErr(func)))
        return msg
    except:
        try:
            tree.write(
                meta_import, 
                encoding = "utf-8", 
                xml_declaration = None, 
                default_namespace = None, 
                method = "xml"
            )
            meta_src.save()
            del meta_src
        except:
            pass
        func = sys._getframe().f_code.co_name
        msgAppend(arcpy.AddError(pyErr(func)))
        return msg


def gSSURGO(input_p: str,
            gdb_p: str,
            module_p: str,
            gssurgo_v: str,
            v: str,
            st: str,
            fy: int
    ) -> str:
    """This function is the backbone of the module. 
    It calls these functions to create and populate a SSURGO geodatabase: 
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
        Directory locatoin of the SSURGO downloads.
    gdb_p : str
        The path of the SSURGO file geodatabase to be created.
    module_p : str
        The module tool directory with the xml files.
    gssurgo_v : str
        The gssurgo version
    v : str
        The tool version
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
        date_format = "(%Y-%m-%d)"
        # Get the XML Workspace Document appropriate for the specified aoi
        # %% check 1
        # ---- make xml
        inputXML = xml("Lower 48 States", module_p, gssurgo_v)
        if not inputXML.exist:
            arcpy.AddError(" \nMissing xml file: " + inputXML.xml)
            return False
        # %% check 1
        # ---- call createGDB
        gdb_b = createGDB(gdb_p, inputXML)
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
        if gssurgo_v != '1.0':
            tabs_uniq.remove('sainterp')
        # If light, exclude interp rules, except NCCPI
        else:
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
        # threadCount = 1 #psutil.cpu_count() // psutil.cpu_count(logical=False)
        # arcpy.AddMessage(f"{threadCount= }")
        ti = time.time()
        import_jobs = funYield(importList, paramSet, constSet)
        for paramBack, output in import_jobs:
        # for paramBack in paramSet:
            # output = importList(**paramBack, **constSet)
            try:
                # if not output:
                    # arcpy.AddMessage(
                        # f"\tSuccessfully populated {paramBack['table']}"
                    # )
                # else:
                if output:
                    # arcpy.AddError(f"Failed to populate {paramBack['table']}")
                    arcpy.AddError(output)
                    return
            except GeneratorExit:
                arcpy.AddWarning("passed")
                arcpy.AddWarning(f"{paramBack}")
                arcpy.AddWarning(f"{output}")
                pass
        import_jobs.close()
        del import_jobs
        gc.collect()
        # arcpy.AddMessage(f"time: {time.time() - ti}")

        if not versionTab(input_p, gdb_p, gssurgo_v, v):
            arcpy.AddWarning('Version table failed to populate successfully.')

        if gssurgo_v != '1.0':
            table_d['mdruleclass'] = ['NA', 'Rule Class Text Metadata', ()]
            table_d['mdrule'] = ['NA', 'Interpretation Rules Metadata', ()]
            table_d['mdinterp'] = ['NA', 'Interpretations Metadata', ()]
            msg = schemaChange(
                gdb_p, input_p, module_p, table_d)
            # if msg:
            #     arcpy.AddWarning(msg)

        # Create Indices
        if not createIndices(gdb_p, module_p, gssurgo_v):
            arcpy.AddWarning(
                "Failed to create indices which may imparct efficient use of "
                "database."
            )

        # Create table relationships and indexes
        # ---- call createTableRelationships
        rel_b = createTableRelationships(gdb_p)
        if not rel_b:
            return False
        
        # Query the output SACATALOG table to get list of surveys that were 
        # exported to the gSSURGO
        arcpy.AddMessage("\tUpdating metadata...")
        tab_sac = f"{gdb_p}/sacatalog"

        # Areasymbol and Survey Area Version Established
        sCur = arcpy.da.SearchCursor(tab_sac, ["AREASYMBOL", "SAVEREST"])
        export_query = [
            (f"{ssa} {date_obj.strftime(date_format)}", f"'{ssa}'")
            for ssa, date_obj in sCur
        ]
        del sCur
        # survey_i format: NM007 (2022-09-08)
        # query_i format: 'NM007'
        survey_i, query_i = map(','.join, zip(*export_query))

        # Update metadata for the geodatabase and all featureclasses
        arcpy.SetProgressorLabel("Updating metadata...")

        msgs = updateMetadata(gdb_p, survey_i, st, str(fy))
        if msgs:
            for msg in msgs:
                arcpy.AddError(msg)

        arcpy.SetProgressorLabel("\tCompacting new database...")
        arcpy.Compact_management(gdb_p)

        env.workspace = os.path.dirname(env.scratchFolder)

        # for line in re.findall('.{1,80}\W', query_i):
        #     arcpy.AddMessage(line.replace("'", " "))
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


def createIndices(gdb_p: str, module_p: str, gssurgo_v: str) -> bool:
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
    gssurgo_v : str
        The gSSURGO version

    Returns
    -------
    bool
        Returns True if all indices were successfully created, otherwis 
        False.
    """
    try:
        arcpy.AddMessage('\n\tAdding attribute indices...')
        # Any field involved with a Relationship Class is already indexed
        if (gssurgo_v.split('.')[0]) == '2':
            csv_p = module_p + "/md_index_insert2.csv"
        else:
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
            "\n\tCreating table relationships on key fields..."
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


def schemaChange(
        gdb_p: str, input_p: str, module_p: str, 
        table_d: dict[str, list[str, str, list[tuple[int, str]]]], 
        ssa_l: list[str], light: bool
    ) -> bool:
    """This function reconciles differences in importing and schemas between 
    gSSURGO versions.
    One of the most signifcant differences in schema between version 1.0 and 
    2.0 is the structure of the cointerp and sainterp tables. Also, three 
    additional tables related to this restructuring have been added, the 
    mdrule, mdinterp and mdruleclass tables.
    Various csv files are read from sddt/construct module folder to assist 
    in this. Each csv file as a version number <v>.
    md_column_update<v>.csv: List of changes/deletion of columns
    md_column_insert<v>.csv: List of new columns
    md_index_insert<v>.csv: List of new indices to added
    md_index_delete<v>.csv: List of indices to be removed
    md_rule_classes.csv: List of unique interpretation value classes
    md_tables_insert<v>.csv: List of new tables

    Parameters
    ----------
    gdb_p : str
        Path of the SSURGO file geodatabase.
    input_p : str
        Directory with the SSURGO datasets.
    module_p : str
        path to the sddt module
    table_d : dict[str, list[str, str, list[tuple[int, str]]]]
        Key is the Table Physical Name (gdb table name). Value is a list with 
        three elements, the text file base name, table label, and a list of 
        tuples with the column sequence and column name.
    ssa_l : list[str]
        List of SSURGO datasets to be imported.
    light : bool
        Indicates if a concise dataset is being selected. This is pertinent
        to the populating of the cointerp table.

    Returns
    -------
    bool
        Returns True if successful, otherwise False.
    """

    try:
        arcpy.env.workspace = gdb_p
        # Update mdstattabs table
        # Add mdinterp, mdrule, mdruleclass tables
        mdtab_p = gdb_p + "/mdstattabs"
        mdtab_cols = table_d['mdstattabs'][2]
        mdtab_cols.sort()
        mdtab_cols = [col[1] for col in mdtab_cols]
        iCur = arcpy.da.InsertCursor(mdtab_p, [mdtab_cols])
        csv_p = module_p + "/md_tables_insert2.csv"
        with open(csv_p, newline='', encoding='utf8') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            for row in csv_r:
                iCur.insertRow(row)
        del iCur

        # update mdstattabcols
        # update field lengths and/or datatype, i.e. make keys numeric
        mdcols_p = gdb_p + "/mdstattabcols"
        mdcols_cols = table_d['mdstattabcols'][2]
        mdcols_cols.sort()
        mdcols_cols = [col[1] for col in mdcols_cols]
        csv_p = module_p + "/md_column_update2.csv"
        # collect list of column updates
        with open(csv_p, newline='', encoding='utf8') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            # Table: Column: [type, length, sequence]
            col_updates = {}
            for row in csv_r:
                if (table := row[0]) in col_updates:
                    col_updates[table].update({row[1]: row[4:]})
                else:
                    col_updates[table] = {row[1]: row[4:]}
        # Update mdstattabcols table
        uCur = arcpy.da.UpdateCursor(mdcols_p, mdcols_cols)
        d = 0
        u = 0
        for col_row in uCur:
            if (table := col_row[0]) in col_updates:
                tab_updates = col_updates[table]
                if (col := col_row[2]) in tab_updates:
                    d_type, col_l, seq = tab_updates[col]
                    if d_type.lower() != 'delete':
                        # update sequence if updated
                        col_row[1] = seq or col_row[1]
                        # update data type
                        col_row[5] = d_type
                        # update length
                        col_row[7] = col_l or None
                        uCur.updateRow(col_row)
                        tab_updates.pop(col)
                        u += 1
                    else:
                        uCur.deleteRow()
                        tab_updates.pop(col)
                        d += 1
        # arcpy.AddWarning(col_updates)
        del uCur

        # Add new columns
        csv_p = module_p + "/md_column_insert2.csv"
        with open(csv_p, newline='', encoding='utf8') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            iCur = arcpy.da.InsertCursor(mdcols_p, mdcols_cols)
            for row in csv_r:
                iCur.insertRow(tuple(v or None for v in row))
        del iCur

        # Update mdstatidxmas and mdstatidxdet tables
        mdid_stat_p = gdb_p + '/mdstatidxmas'
        mdid_stat_cols = table_d['mdstatidxmas'][2]
        mdid_stat_cols.sort()
        mdid_stat_cols  = [col[1] for col in mdid_stat_cols]
        mdid_det_p = gdb_p + '/mdstatidxdet'
        mdid_det_cols = table_d['mdstatidxdet'][2]
        mdid_det_cols.sort()
        mdid_det_cols  = [col[1] for col in mdid_det_cols]
        # delete obsolete indices
        csv_p = module_p + "/md_index_delete2.csv"
        with open(csv_p, newline='', encoding='utf8') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            idx_delete = {row[0]: row[1] for row in csv_r}
        uCur = arcpy.da.UpdateCursor(mdid_stat_p, mdid_stat_cols[:2])
        for table, col in uCur:
            if table in idx_delete:
                if idx_delete[table] == col:
                    uCur.delteRow()
        del uCur
        uCur = arcpy.da.UpdateCursor(mdid_det_p, mdid_det_cols[:2])
        for table, col in uCur:
            if table in idx_delete:
                if idx_delete[table] == col:
                    uCur.delteRow()
        del uCur

        # Insert new indices
        iCur = arcpy.da.InsertCursor(mdid_stat_p, mdid_stat_cols)
        csv_p = module_p + "/md_index_insert2.csv"
        with open(csv_p, newline='', encoding='utf8') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            idx_det_l = []
            for row in csv_r:
                iCur.insertRow(row[:2] + [row[-1]])
                idx_det_l.append(row[0:4])
        del iCur
        iCur = arcpy.da.InsertCursor(mdid_det_p, mdid_det_cols)
        for row in idx_det_l:
            iCur.insertRow(row)
        del iCur

        # Populate mdruleclass table
        # leave iCur open in case new interp classes found
        csv_p = module_p + "/md_rule_classes2.csv"
        crt_p = gdb_p + "/mdruleclass"
        iCur = arcpy.da.InsertCursor(crt_p, ['classtxt', 'classkey'])
        # rule class text: class key
        class_d = {}
        with open(csv_p, newline='', encoding='utf8') as csv_f:
            csv_r = csv.reader(csv_f, delimiter=',')
            hdr = next(csv_r)
            for class_txt, class_i in csv_r:
                iCur.insertRow([class_txt, class_i])
                class_d[class_txt] = class_i
        class_sz = len(class_d)
        del iCur
        arcpy.AddMessage("\tSuccessfully populated mdruleclass")

        # Read cinterp.txt
            # exclude non-main rule cotinterps if light
            # except for NCCPI rules (main rule 54955)
        arcpy.SetProgressorLabel("importing cointerp")
        co_tbl = 'cointerp'
        q = "tabphyname = 'cointerp'"
        sCur = arcpy.da.SearchCursor(mdcols_p, ['colsequence', 'colphyname'], q)
        coi_cols = [row for row in sCur]
        del sCur
        # get fields in sequence order
        coi_cols.sort()
        fields = [f[1] for f in coi_cols]
        txt = table_d[co_tbl][0]
        co_p = f"{gdb_p}/{co_tbl}"
        iCur = arcpy.da.InsertCursor(co_p, fields)
        # collate interpration names with key as sainterp.txt lacks key
        # interpname: interpkey
        interp_d = {}
        # don't simultaneously populate mdrule as there is are many to one
        # (interpkey, rulekey): [rulename, ruledepth, seq]
        rule_d = {}
        for ssa in ssa_l:
            # Make file path for text file
            txt_p = f"{input_p}/{ssa.upper()}/tabular/{txt}.txt"
            if not os.path.exists(txt_p):
                return f"{txt_p} does not exist"
            csvReader = csv.reader(
                open(txt_p, 'r', encoding='utf8'), delimiter='|', quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                row = [v or None for v in row]
                interp_k = row[1]
                rule_k = row[4]
                # Add to new (rules, interps) to dict to populate mdrule table
                if (interp_k, rule_k) not in rule_d:
                    rule_d[(interp_k, rule_k)] = [
                        *row[5:7], row[3]
                    ]

                # If its a rule not an interp, 
                # the rule and interp keys (mrulekey) are not equal
                if interp_k != rule_k:
                    # An NCCPI rule (some SDV Attributes based on them)
                    # OR not light (all rules included in cointerp)
                    if (interp_k == '54955') or not light:
                        class_txt = row[12]
                        class_k = class_d.get(class_txt)
                        # Possible that new classes come with new interps
                        if not class_k:
                            # increment class key
                            class_i += 1
                            class_k = class_i
                            class_d[class_txt] = class_k
                            arcpy.AddMessage(
                                f"New rule class found: {class_txt}"
                            )
                        nulls = [v.strip() for v in row[15:18]]
                        iCur.insertRow([
                            row[11], class_k, *nulls, row[4], row[0],
                            row[18]
                        ])
                # an interp
                else:
                    # Collect interps 
                    if (rule_n := row[2]) not in interp_d:
                        # Collect interp keys (main rule keys) by name to add to
                        # sainterp and mdinterp tables
                        interp_d[rule_n] = interp_k
                    class_txt = row[12]
                    class_k = class_d.get(class_txt)
                    # Possible that new classes come with new interps
                    if not class_k:
                        # increment class key
                        class_i += 1
                        class_k = class_i
                        class_d[class_txt] = class_k
                        arcpy.AddMessage(f"New rule class found: {class_txt}")
                    # some zeros have a space after them
                    nulls = [v.strip() for v in row[15:18]]
                    iCur.insertRow([
                        row[11], class_k, *nulls, rule_k, row[0], row[18]
                    ])
        arcpy.AddMessage("\tSuccessfully populated cointerp")
        del iCur
        # insert any new found interp classes
        if len(class_d) != class_sz:
            arcpy.AddMessage(f"Adding {len(class_d)} new interp classes")
            iCur = arcpy.da.InsertCursor(crt_p, ['classtxt', 'classkey'])
            for class_txt, class_k in class_d:
                iCur.insertRow([class_txt, class_k])
            del iCur
        # Delete rulekey if light

        # Populate mdrule table
        mdr_p = gdb_p + "/mdrule"
        fields = [
            'rulename', 'ruledepth', 'seqnum', 'interpkey', 'rulekey'
        ]
        iCur = arcpy.da.InsertCursor(mdr_p, fields)
        for k, v in rule_d.items():
            # [rulename, ruledepth, seq], [interpkey, rulekey]
            iCur.insertRow([*v, *k])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated mdrule")

        # Sainterp table
        sa_tbl = 'sainterp'
        q = "tabphyname = 'sainterp'"
        sCur = arcpy.da.SearchCursor(mdcols_p, ['colsequence', 'colphyname'], q)
        sa_cols = [row for row in sCur]
        del sCur
        # get fields in sequence order
        sa_cols.sort()
        fields = [f[1] for f in sa_cols]
        txt = table_d[sa_tbl][0]
        sa_p = f"{gdb_p}/{sa_tbl}"
        iCur = arcpy.da.InsertCursor(sa_p, fields)
        # interp key: first 7 elements from sintperp
        mdinterp_d = {}
        for ssa in ssa_l:
            # Make file path for text file
            txt_p = f"{input_p}/{ssa.upper()}/tabular/{txt}.txt"
            if not os.path.exists(txt_p):
                return f"{txt_p} does not exist"
            csvReader = csv.reader(
                open(txt_p, 'r', encoding='utf8'), delimiter='|', quotechar='"'
            )
            for row in csvReader:
                # replace empty sets with None
                row = tuple(v or None for v in row)
                interp_n = row[1]
                interp_k = interp_d.get(interp_n)
                if interp_k not in mdinterp_d:
                    # Get interp info to populate mdinterp
                    mdinterp_d[interp_k] = row[1:7]
                iCur.insertRow([interp_k, *row[-2:]])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated sainterp")

        # populate mdinterp table
        mdi_p = gdb_p + "/mdinterp"
        fields = [
            'interpname', 'interptype', 'interpdesc', 'interpdesigndate',
            'interpgendate', 'interpmaxreasons', 'interpkey'
        ]
        iCur = arcpy.da.InsertCursor(mdi_p, fields)
        for k, vals in mdinterp_d.items():
            iCur.insertRow([*vals, k])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated mdinterp")
        return True

    except arcpy.ExecuteError:
        try:
            del iCur
        except:
            pass
        try:
            del uCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(arcpyErr(func))
        return False
    except:
        try:
            del iCur
        except:
            pass
        try:
            del uCur
        except:
            pass
        func = sys._getframe().f_code.co_name
        arcpy.AddError(pyErr(func))
        return False


def versionTab(input_p: str, gdb_p: str, gssurgo_v: str, script_v: str) -> bool:
    """"This tool populates the version table with the ESRI software version,
    operating system version, python version, SSURGO version, gSSURGO version,
    script tool version, RSS version and File Geodatabase version.

    Parameters
    ----------
    input_p : str
        Directory of the SSURGO Download
    gdb_p : str
        Path of the newly created gSSURGO file geodatabase
    gssurgo_v : str
        gSSURGO version
    script_v : str
        Script tool version

    Returns
    -------
    bool
        returns true if version table successfully created, otherwise false.
    """
    try:
            # populate version table
        txt_p = f"{input_p}/ersion.txt"
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
            'FGDB': ('Database', 'File Geodatabase', gdb_v),
            'script': (
                'Script', 'SDDT: Create SSURGO File Geodatabase', script_v
            ),
            'RSS': ('Purpose', 'Raster Soil Survey', '1.1')
        }

        version_d['abbrev1'] = ('Abbreviation Level', 'cointerp', '0.5')

        version_p = f"{gdb_p}/version"
        iCur = arcpy.da.InsertCursor(version_p, ['type', 'name', 'version'])
        for vals in version_d.values():
            iCur.insertRow([*vals])
        del iCur
        arcpy.AddMessage("\tSuccessfully populated version")
        return True
    
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
            - Module path to access xml and other helper files.

    Returns
    -------
    str
        Then name of the RSS database (file geodatabase) if successful, 
        empty string otherwise
    """
    # %% m
    try:
        v = '1.2'
        arcpy.AddMessage("Creating RSS SSURGO File GDB, version: " + v)
        # location of SSURGO datasets containing SSURGO downloads
        input_p = args[0] # 0: input folder
        output_p = args[1] # output path
        st = args[2] # State
        fy = args[3] # fiscal year
        gssurgo_v = args[4] # gSSURGO template version
        module_p = args[5] # module path

        if gssurgo_v == 'gSSURGO 2.0':
            gssurgo_v = '2.0'
        else:
            gssurgo_v = '1.0'
        
        # arcpy.AddMessage(f"SDDT version {v} for SSURGO version {dbVersion}")
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
        os.mkdir(new_p)
        gdb_p = f"{new_p}/RSS_{st}.gdb"

        gdb_b = gSSURGO(input_p, gdb_p, module_p, gssurgo_v, v, st, fy)

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
# %%
