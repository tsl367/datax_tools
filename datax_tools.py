#!/usr/bin/python
#****************************************************************#
# ScriptName: datax_tools.py
# Author: tsl367@hotmail.com
# Create Date: 2015-12-22 14:41
# Modify Date: 2015-12-23 15:34
# Function:
#***************************************************************#
import json
import subprocess
import shlex
import ConfigParser
import logging
import logging.handlers

from get_from_odps import get_partitions,get_cols

#"table":"tmp_roy_target_merge","column":["sfzhm","attach"],"partition":["src=by_27_ju"]
cf = ConfigParser.ConfigParser()
cf.read("config.ini")

reader_accessId = cf.get("reader_common", "reader_accessId")
reader_accessKey = cf.get("reader_common", "reader_accessKey")
reader_project = cf.get("reader_common", "reader_project")

writer_accessId = cf.get("writer_common", "writer_accessId")
writer_accessKey = cf.get("writer_common", "writer_accessKey")
writer_project = cf.get("writer_common", "writer_project")

datax_speed = cf.getint("datax_settings", "datax_speed")
channel = cf.getint("datax_settings", "channel")
reader_project_auth = cf.get("datax_settings", "reader_project_auth")
reader_odps_server = cf.get("datax_settings", "reader_odps_server")
writer_odps_server = cf.get("datax_settings", "writer_odps_server")
writer_tunnel = cf.get("datax_settings", "writer_tunnel")
writer_truncate = cf.getboolean("datax_settings", "writer_truncate")
writer_accountType = cf.get("datax_settings", "writer_accountType")
transport_byte = cf.get("datax_settings", "transport_byte")
transport_record = cf.get("datax_settings", "transport_record")

datax_path = cf.get("tool_settings", "datax_path")
odpscmd_path = cf.get("tool_settings", "odpscmd_path")
tables_file = cf.get("tool_settings", "tables_file")
reader_ini = cf.get("tool_settings", "reader_ini")
writer_ini = cf.get("tool_settings", "writer_ini")
log_file = cf.get("tool_settings", "log_file")
run_file = cf.get("tool_settings", "run_file")
check_file = cf.get("tool_settings", "check_file")
check_src_log = cf.get("tool_settings", "check_src_log")
check_dst_log = cf.get("tool_settings", "check_dst_log")

handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=5)
fmt = '%(asctime)s-%(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
ch = logging.StreamHandler()

logger = logging.getLogger(log_file)
logger.addHandler(handler)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

def write_odps_ini():
    file_name = reader_ini
    fp = open(file_name, "w")
    content = "project_name=%s\naccess_id=%s\naccess_key=%s\nend_point=%s\n" % (reader_project, reader_accessId, reader_accessKey, reader_odps_server)
    fp.write(content)
    fp.close()

    file_name = writer_ini
    fp = open(file_name, "w")
    content = "project_name=%s\naccess_id=%s\naccess_key=%s\nend_point=%s\n" % (writer_project, writer_accessId, writer_accessKey, writer_odps_server)
    fp.write(content)
    fp.close()

def gen_dict(reader_table, writer_table):
    logger.info("\n\n=====start from %s to %s=====" % (reader_table, writer_table))
    (reader_col, reader_par_col) = get_cols(reader_table, reader_ini, odpscmd_path, logger)
    (writer_col, writer_par_col) = get_cols(writer_table, writer_ini, odpscmd_path, logger)
    if not reader_col or not writer_col:
        logger.error("connected error:reader %s,writer %s" % (reader_table, writer_table))
        return None
    reader_partition_cols = get_partitions(reader_table, reader_ini, odpscmd_path, logger)
    aim_list = []
    if reader_par_col:
        for the_partition in reader_partition_cols:
            reader_parm = {
                    "accessId":reader_accessId,
                    "accessKey":reader_accessKey,
                    "project":reader_project,
                    "table":reader_table,
                    "column":reader_col,
                    "partition":[the_partition],
                    "packageAuthorizedProject":reader_project_auth,
                    "splitMode":"record",
                    "odpsServer":reader_odps_server
            }
            writer_parm = {
                    "accessId":writer_accessId,
                    "accessKey":writer_accessKey,
                    "project":writer_project,
                    "table":writer_table,
                    "column":writer_col,
                    "partition":the_partition,
                    "odpsServer":writer_odps_server,
                    "truncate":writer_truncate,
                    "tunnelServer":writer_tunnel,
                    "accountType":writer_accountType
            }
            reader_detail = {"name":"odpsreader", "parameter":reader_parm}
            writer_detail = {"name":"odpswriter", "parameter":writer_parm}
            #setting = {"speed":{"byte":datax_speed}}
            setting = {"speed":{"channel":channel}}
            content = [{"reader":reader_detail,"writer":writer_detail}]
            init_dict = {
                "core":{"transport":{"channel":{"speed":{"byte":transport_byte,"record":transport_record}}}},
                "job":{"setting":setting, "content":content}
            }
            aim_list.append(init_dict)
    else:
        reader_parm = {
                "accessId":reader_accessId,
                "accessKey":reader_accessKey,
                "project":reader_project,
                "table":reader_table,
                "column":reader_col,
                "packageAuthorizedProject":reader_project_auth,
                "splitMode":"record",
                "odpsServer":reader_odps_server
        }
        writer_parm = {
                "accessId":writer_accessId,
                "accessKey":writer_accessKey,
                "project":writer_project,
                "table":writer_table,
                "column":writer_col,
                "odpsServer":writer_odps_server,
                "truncate":writer_truncate,
                "tunnelServer":writer_tunnel,
                "accountType":writer_accountType
        }
        reader_detail = {"name":"odpsreader", "parameter":reader_parm}
        writer_detail = {"name":"odpswriter", "parameter":writer_parm}
        #setting = {"speed":{"byte":datax_speed}}
        setting = {"speed":{"channel":channel}}
        content = [{"reader":reader_detail,"writer":writer_detail}]
        init_dict = {
            "core":{"transport":{"channel":{"speed":{"byte":transport_byte,"record":transport_record}}}},
            "job":{"setting":setting, "content":content}
        }
        aim_list.append(init_dict)

    index = 1
    cmd_list = []
    for the_aim in aim_list:
        file_ini = "./json/%s_%s.json" % (reader_table, index)
        fp = open(file_ini, "w")
        fp.write(json.dumps(the_aim))
        fp.close()
        cmd = "%s %s > ./log/%s_%s.log" % (datax_path, file_ini, reader_table, index)
        cmd_list.append(cmd)
        index+=1
        #p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
        #presult = p.communicate()
    logger.info("=====generate json from %s to %s=====\n\n" % (reader_table, writer_table))
    return cmd_list


def read_tables():
    fp = open(tables_file, 'r')
    all_tables = fp.read()
    fp.close()
    tables_list =  all_tables.split('\n')
    table_dict = dict()
    for the_tables in tables_list:
        temp_tables = the_tables.split(' ')
        the_table_list = []
        for the_temp in temp_tables:
            if the_temp:
                the_table_list.append(the_temp)
        if the_table_list:
            if len(the_table_list) == 1:
                table_dict[the_table_list[0]]=the_table_list[0]
            else:
                table_dict[the_table_list[0]]=the_table_list[1]
    return table_dict

def gen_run_script(all_cmd):
    fp = open(run_file, "w")
    content = '#!/bin/bash\n%s' % '\n'.join(all_cmd)
    fp.write(content)
    fp.close()
    logger.info("=====generate all datax cmd in %s=====" % run_file)

def gen_check_script(table_dict):
    fp = open(check_file, "w")

    bash_header = '#!/bin/bash\n/bin/cp /dev/null %s;/bin/cp /dev/null %s;\n' % (check_src_log, check_dst_log)

    fp.write(bash_header)

    cmd_src_header = "%s -e --config=%s" % (odpscmd_path, reader_ini)
    cmd_dst_header = "%s -e --config=%s" % (odpscmd_path, writer_ini)

    cmd_src_content = ""
    cmd_dst_content = ""

    for the_src, the_dst in table_dict.items():
        cmd_src_content += "select '%s', count(*) from %s; " % (the_src, the_src)
        cmd_dst_content += "select '%s', count(*) from %s; " % (the_dst, the_dst)

    fp.write("%s \"%s\" >>%s\n" % (cmd_src_header, cmd_src_content, check_src_log))
    fp.write("%s \"%s\" >>%s\n" % (cmd_dst_header, cmd_dst_content, check_dst_log))
    fp.close()
    logger.info("=====generate all check cmd in %s=====" % check_file)

if __name__ == '__main__':

    write_odps_ini()
    table_dict = read_tables()

    all_cmd = []
    err_list = []
    for the_src,the_aim in table_dict.items():
        the_cmd_list = gen_dict(the_src, the_aim)

        if the_cmd_list:
            all_cmd += the_cmd_list
        else:
            err_list.append("%s to %s" % (the_src, the_aim))

    gen_run_script(all_cmd)
    gen_check_script(table_dict)

    if err_list:
        logger.info("=====error:%s=====" % '\n'.join(err_list))
    else:
        logger.info("=====error:None=====")
