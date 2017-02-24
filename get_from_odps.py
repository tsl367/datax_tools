#!/usr/bin/python
#****************************************************************#
# ScriptName: get_from_odps.py
# Author: tsl367@hotmail.com
# Create Date: 2015-12-22 15:19
# Modify Date: 2015-12-23 15:35
# Function:
#***************************************************************#
#cat test.py

import subprocess
import shlex

def get_cols(table_name, ini_path, odpscmd_path, logger):
    cmd = "%s -e --config=%s 'desc %s'" % (odpscmd_path, ini_path, table_name)
    logger.info(cmd)
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    presult = p.communicate()
    res_list = presult[0].split('\n')
    cols_list = []
    partitions_list = []
    is_field = False
    is_field_value = False
    is_partition = False
    is_partition_value = False
    for the_res in res_list:
        try:
            if the_res:
                if not is_field and the_res[0] == "|" and the_res[1:7] == " Field":
                    is_field = True
                    continue
                if is_field and not is_field_value:
                    if the_res[0] == "+":
                        is_field_value = True
                        continue
                if is_field and is_field_value:
                    if the_res[0] == "+":
                        is_field = False
                        is_field_value = False
                    else:
                        the_res_list = the_res.split('|')
                        cols_list.append(the_res_list[1].strip())

                if not is_partition and the_res[0] == "|" and the_res[1:7] == " Parti":
                    is_partition = True
                    continue
                if is_partition and not is_partition_value:
                    if the_res[0] == "+":
                        is_partition_value = True
                        continue
                if is_partition and is_partition_value:
                    if the_res[0] == "+":
                        is_partition = False
                        is_partition_value = False
                    else:
                        the_res_list = the_res.split('|')
                        partitions_list.append(the_res_list[1].strip())
        except Exception,e:
            logger.error("get_cols error, %s" % e)

    return (cols_list, partitions_list)

def get_partitions(table_name, ini_path, odpscmd_path, logger):
    cmd = "%s -e --config=%s 'show partitions %s'" % (odpscmd_path, ini_path, table_name)
    logger.info(cmd)
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
    presult = p.communicate()
    res_list = presult[0].split('\n')
    partitions_list = []
    for the_partition in res_list:
        if the_partition:
            partitions_list.append(the_partition)
    return partitions_list
