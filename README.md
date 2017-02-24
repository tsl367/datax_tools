#Datax批量工具
##1 功能
该工具可以自动获取源表和目标表的属性及分区信息，自动生成源表所有分区的json文件，并生成批量串行脚本和批量检测脚本。方便大家的数据迁移。

##2 操作过程
1. `mkdir ./json ./log ./temp`
2. 在config.ini中配置好源ODPS和目标ODPS相关的accessID、accessKEY、project、ODPS server等相关信息；
3. 在config.ini中配置好datax.py与odmscmd的路径；
4. 在tables.ini中配置好需要迁移的表名；
5. 运行python datax_tools.py，生成运行脚本;
6. 运行run_datax.sh，批量顺序执行迁移任务；
7. 运行check_datax.sh，进行源表和目标表的条数校验；

##3 详细说明
###3.1 工具配置
####3.1.1 源ODPS配置与目标ODPS配置
在配置文件config.ini的reader_common和writer_common区域，reader表示源ODPS，writer表示目标ODPS。主要配置accessID、accessKey和project名。这些均可从base的用户信息获取。

####3.1.2 datax的相关配置
在配置文件config.ini的datax_settings区域。

* datax_speed: 控制datax实际运行时的速度上限；
* reader_project_auth: datax的验证机制，可为空。如果验证失败，可以输入一个源ODPS ID具备owner权限的项目名；
* reader_odps_server: 源ODPS的api url，可从cmdb查询，或找云管理员获取；
* writer_odps_server: 目标ODPS的api url，可从cmdb查询，或找云管理员获取；
* writer_odps_tunnel: 目标ODPS的tunnel，可从cmdb查询，或找云管理员获取；
* writer_truncate: 覆盖式导入开关；
* writer_accoutType: 一般默认为aliyun；

####3.1.3 工具的相关配置
本批量工具的相关配置，需要注意datax.py和odpscmd的路径，其余一般不用修改。

###3.2 源表和目标表添加
在配置文件tables.ini中。

* 若源表和目标表的表名相同，则直接输入源表的表名即可，每行一个表名；
* 若源表和目标表的表名不同，则每行输入一个源表名和一个目标表名，两者之间用空格分隔；

###3.3 生成脚本
直接运行`python datax_tools.py`即可。

* 运行过程中会打印运行的odpscmd语句及相关运行信息，该语句可能会有延迟或失败；
* 运行的所有过程会记录在./log/info.log中；
* 脚本运行完毕会打印所有获取信息失败的表名；
* 所有获取信息成功的表，会生成相关的json文件，存储在./json中；
* 生成run_datax.sh脚本，用于实际进行datax任务；
* 生成check_datax.sh脚本，用于运行完任务后进行校验；

###3.4 批量运行datax任务
运行run_datax.sh的脚本，批量顺序执行datax任务。

* 相关日志存储在./log/表名.log中；
* 若想并行运行程序，可修改run_datax.sh中的语句，改为后台运行；

###3.5 批量检测
当data_x的所有任务完成后，运行check_datax.sh的脚本，会生成所有源表和目标表所有的条数。

* 为减少对ODPS的请求次数，脚本会一次请求该ODPS（源表，目标表）所有迁移表的条数；
* 结果会存储在./log/check_src.log, ./log/check_dst.log两个日志中；
* 可用vimdiff来比较两个日志，查看具体哪个表条数不同；
