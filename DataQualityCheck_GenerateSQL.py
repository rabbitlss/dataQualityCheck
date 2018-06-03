# coding:utf-8
import xlrd
import os
import datetime
import time
import re
import batchIdConfig as bic
import dqConfig 
import codecs

#*********************************************************************
#Function: 根据测试案例文档完成测试
#Date    : 2016/10/21
#Owner   : lishanshan
#History ：Date  Author  Description
#          2017/01/13 自定义SQL中可以有中文注释，仅限于‘/*---*/’这种类型的中文注释
# 2017/01/16  日志文件名不再是运行时间，而是加上了测试案例文档的名称
# 2017/01/20  PDM代码值新增对是否为''的判断。如果这里填写的是Y，就说明允许为空，那就在检查的where条件里面撇掉空记录。如果是N，那就是说不允许为空，那就不做限制
# 2017/02/04  增加了对别名的处理，如主键检查和外键检查中，如果是像COALESCE或者trim或者拼接字段的情况，都需要给这些公式加上别名，程序中增加了对这些别名的处理（主要是修改了正则表达式）
# 2017/02/06  增加代码值域（PDM）的功能:能过对组合的码值进行判断，如当事人分类细项代码表的码值是级联的，也可以放到这里来验证
# 2017/02/28  新增数据库变量的替换功能。如自定义SQL里面，数据库名写${SDATA_AFT}，然后替换成SDATA_AFT；或者数据库名写${PDATA_AFT}，然后替换成DW_PDATA_AFT
#*********************************************************************


#***********
#去掉空格的小函数
#***********
def sstrip(s):
  return s.strip()


#******
#去掉全部空格
#******
def sreplace(s):
  return str(s).replace("　","").replace(" ","").replace(" ","").replace(" ","").replace("\n","").replace(" ","").replace(" ","")



class DataQualityCheck:
  #定义各个checkType的标志
  #主键重复检查
  pkCheck = 'PK'
  #外键完整性检查
  fkCheck = 'FK'
  #父子关系的外键完整性检查
  fcCheck = 'FC'
  #PDM代码值域检查
  pcCheck = 'PC'
  #SDATA代码值域检查
  scCheck = 'SC'
  #值域检查
  vcCheck = 'VC'
  #交叉链检查
  zcCheck = 'ZC'
  #自定义SQL检查
  ucCheck = 'UC'
  #自定义规则检查
  uuCheck = 'UU'
  
  #
  batchId = ''
  
  #生成的perl文件的前半部分
  preScript = """use strict;   # Declare using Perl strict syntax
use DBI;    # If you are using other Perls package, declare here 
######################################################################
# ------------ Variable Section ------------
my $AUTO_HOME = $ENV{'AUTO_HOME'};                                                             
unshift(@INC, "${AUTO_HOME}/bin");
require spdbedw;
my $AUTO_DATA = $ENV{'${AUTO_HOME}/DATA'};                                                     
my $AUTO_LOG = $ENV{'${AUTO_HOME}/LOG'};     
my $DW_PDATADB = $SPDBEDW::PDATADB;                                                   
my $PDATADB = $SPDBEDW::PVIEWDB;  
my $PVIEWDB = $SPDBEDW::PVIEWDB;                                                             
my $WORKDB = $SPDBEDW::WORKDB;                                                                
my $SDATADB = $SPDBEDW::SDATADB;
my $UPTBAKDB = $SPDBEDW::UPTBAKDB;                                                               
my $MATNDB = $SPDBEDW::MATNDB;
                                                        
my $SDATA = $SPDBEDW::SDATADB;                                                        
my $PDATA = $SPDBEDW::PDATADB;                                                        
my $SDATA_BEF = $SPDBEDW::UPTBAKDB;                                                        
my $PDATA_BEF = $SPDBEDW::UPTBAKDB;                                                        
my $SDATA_AFT = $SPDBEDW::SDATADB;                                                        
my $PDATA_AFT = $SPDBEDW::PDATADB;                                      
my $CHECKDB = $SPDBEDW::MATNDB;                                                     
my $TXNDATE;    
my $SUB_BATCH_ID;                
my $LOGON_STR;
my $LOGON_FILE = "${AUTO_HOME}/etc/LOGON_CHKUSER";
my $CONTROL_FILE = "";
my $TX_DATE = "";
# -------new varibles for PDM Optimization---------
unshift(@INC,"${AUTO_HOME}/bin");       #Declare the Path of CTLFW Package 
require CTLFW;         #Using the CTLFW Package
# ------------ BTEQ function ------------
sub run_bteq_command
{
 my $rc = open(BTEQ, "| bteq");
 unless ($rc) {
  print "Could not invoke BTEQ command\n";
  return -1;
 }
 # ------ Below are BTEQ scripts ------
#print BTEQ ".set session charset 'UTF8';\\n";
print BTEQ ".WIDTH 1024;\\n";
print BTEQ ${LOGON_STR};
print BTEQ "\\n";
 """
  
  #生成的perl文件的后半部分
  postScript = """print BTEQ <<ENDOFINPUT;
.IF ERRORCODE <> 0 THEN .GOTO QUITWITHERROR;
.GOTO QUITWITHNOERROR;
.LABEL QUITWITHERROR
.LOGOFF;
.QUIT 12;
.LABEL QUITWITHNOERROR
.LOGOFF;
.QUIT 0;
ENDOFINPUT
 close(BTEQ);
 my $RET_CODE = $? >> 8;
 if ( $RET_CODE == 12 ) {
  return 1;
 }
 else {
  return 0;
 }
}
# ------------ main function ------------
sub main
{
   my $ret;
   open(LOGONFILE_H, "${LOGON_FILE}");
   $LOGON_STR = <LOGONFILE_H>;
   close(LOGONFILE_H);
   # Get the decoded logon string
   $LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;
   # Call bteq command to load data
   $ret = run_bteq_command();
   print "run_bteq_command() = $ret";
   return $ret;
}
# ------------ program section ------------
# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 ) {
   exit(1);
}
# Get the first argument
$CONTROL_FILE = $ARGV[0];
if (length( $ARGV[1] ) != 0) {
  $SUB_BATCH_ID = $ARGV[1];
}
else {
  $SUB_BATCH_ID = ''
}
$TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 8);
if ( substr(${CONTROL_FILE}, length(${CONTROL_FILE})-3, 3) eq 'dir' ) {
    $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8);
};
$TXNDATE = join('-',substr($TX_DATE,0,4),substr($TX_DATE,4,2),substr($TX_DATE,6,2));
open(STDERR, ">&STDOUT");
my $ret = main();
exit($ret);
 """
  
  
  #配置数据验证模板各sheet的名字
  configSheet = '配置项'
  pkSheet = '字段唯一性'
  fkSheet = '外键完整性'
  fcSheet = '父子关系'
  pcSheet = '代码值域(PDM)'
  scSheet = '代码值域(SA)'
  vcSheet = '值域(手动设置)'
  zcSheet = '历史拉链表'
  ucSheet = '自定义SQL规则'
  uuSheet = '自定义规则'

  #检查SAMPLE的数量，如主键重复的sample记录我们只要20条，则sampleNbr = 50
  sampleNbr = 50

  #是否要读取SDATA代码表Excel的标志。当数据验证Excel的“代码值域验证（SA）”sheet中有值时，置为True
  saParserFlag = False
  #数据库IP地址
  dbAddress = ''
  #数据库用户名
  dbUser = ''
  #数据库密码
  dbPassWord = ''
  #pdata库名
  pdata = ''
  #sdata库名
  sdata = ''
  #验证通过标志，默认为Y。
  checkFlag = 'Y'
  #批次号
  caseNo = ''
  #运行日期
  txnDate = ''
  #SA代码表位置
  saFilePath = ''
  #验数结果所在数据库
  checkResultDb = ''

  #数据日期
  TXNDATE = ''

  #主键重复检查的规则数量
  pkRuleCnt = 0
  #外键检查的规则数量
  fkRuleCnt = 0
  #父子关系检查的规则数量
  fcRuleCnt = 0
  #pdm代码检查规则数量
  pcRuleCnt = 0
  #sa代码检查规则数量
  scRuleCnt = 0
  #值域检查规则数量
  vcRuleCnt = 0
  #交叉链检查规则数量
  zcRuleCnt = 0 
  #自定义SQL检查规则数量
  ucRuleCnt = 0
  #自定义规则数量
  uuRuleCnt = 0

  #对别名的处理的正则表达式。就是将形如COALESCE(agmt_id,'')as xxx找出来，并替换COALESCE(agmt_id,'')as,只保留别名
  ##pattern = r"coalesce\s?\(\s?[\w_]+\s?\,\s?\'\'\s?\)\s?as\s+"
  pattern = r'[\w_\(\)\|\,\'\d\s]+[\s\)]+as\s+'



  #数据库变量的设置文件的地址
  DBParams = "DBParams.txt"

  #日志文件的路径
  #确定运行日期以后，结合运行日期得到日志文件的文件名
  logFileDir = '.\\Log\\'

  logff = ''

  #分割线
  logDis = '-----------------------------------------------分割线----------------------------------------------\n'

  ####通用SQL语句 
  #
  createResVol= ""
  #
  createDetVol= ""
  #
  createCodeVol= ""
  
  #
  insertResTab=""

  insertDetTab=""

  insertCodeTab=""


  #查询全部记录数：其中DATABASENAME和TABLENAME都需要做替换
  selCnt = """   select cast(count(*) AS FLOAT ) as cnt 
     from DATABASENAME.TABLENAME"""

  #主键重复检查是否通过，及存在的错误记录数：其中COLLIST、DATABASENAME、TABLENAME、WHERECLAUSE都需要做替换
  #注意WHERECLAUSE为空的情况如何处理（空的话，可以看成是where 1=1，然后将其他的where条件接在where 1=1后面）
  checkSQL = """   select cast(coalesce(sum(cnt),0)AS FLOAT ) as failedCnt
     from (
           select COLLIST,
          count(*) AS cnt 
             from DATABASENAME.TABLENAME 
        WHERECLAUSE 
        group by  COLLIST2 
           having cnt >1
    )a"""

  #规则&检查结果入库：RESULTDATABASENAME替换成目标表所在库名，values对应的是包含15个元素的list或元组
  checkResultSQL = """ insert into  vtable_checkresult_${BATCHID}(
    checkId,
    runDate,
    batchId,
    checkStartTime,
    checkEndTime,
    DBNm,
    TabNm,
    tabRuleId,
    colNm,
    checkType,
    SQLResult,
    checkResultInd,
    AllRowCnt,
    FailedRowCnt,
    QuerySQL,
    description)
   SELECT INSERTVALUES, 
      CASE 
        WHEN failedRowCnt = 0 THEN 'Y' 
        WHEN failedRowCnt > 0 THEN 'N' 
        ELSE '' 
      END AS checkResultInd,
      t1.CNT AS allRowCnt,
      t2.failedCnt AS failedRowCnt,
      'SAMPLECHECKSQL',
      '' 
     FROM (
    CHECKSQL
    )T2 
LEFT JOIN (
    SELCNTSQL
    )T1 
  ON  1 = 1
"""

  #检查记录入库：RESULTDATABASENAME替换成目标表所在库名，INSERTCOLS根据需要插入的字段数不同而不同，选择从checkColumns1到checkColumns10的collist
  #INSERTCLAUSE就是将对应的内容拼接成一个string，然后替换；COLLIST就是源表的字段序列；
  #DATABASENAME、TABLENAME、WHERECLAUSE等等同于pkCheckSQL的情况
  checkDetailSQL = """INSERT INTO vtable_checkdetail_${BATCHID}(
      checkId,
      runDate,
      batchId,
      DBNm,
      TabNm,
      ColNm,
      checkType,
      FCInd,
      INSERTCOLS,
      sampleCnt) 
   SELECT INSERTCLAUSE, 
      a.*  
     FROM   (
         SELECT COLLIST,
            CAST(COUNT(*)AS FLOAT ) AS CNT 
             FROM DATABASENAME.TABLENAME 
      WHERECLAUSE 
      GROUP BY  COLLIST2 
          HAVING  CNT >1
    )a 
  SAMPLE """ + str(sampleNbr) 

  #可以直接在TDA上面跑的SAMPLE用SQL
  sampleCheckSQL = """   select COLLIST,
      cast(count(*) AS FLOAT )as cnt 
     from DATABASENAME.TABLENAME 
WHERECLAUSE 
group by  COLLIST2 
    having  cnt >1  
   sample """ + str(sampleNbr)


  #外键检查的SQL：注意FatherCols和ChildCols的字段数要对应（字段顺序的对应需要在填文档的时候保证）
  # SELECT COUNT(*) AS failedCnt
  # FROM CD_PDATA_CUP_NEW.T00_PARTY_CARD_RELA_H 
  # WHERE (Party_Id) NOT IN (SELECT Party_Id FROM CD_PDATA_CUP_NEW.T01_Party)  AND Party_Id<>'' AND END_DT = DATE '3000-12-31' 
  fkCheckSQL = """   select cast(count(*)AS FLOAT ) as failedCnt 
     from (
           select CHILDCOLLIST 
             from CHILDDATABASENAME.CHILDTABLENAME
       where  (CHILDCOLS)  
          not in (
               select FATHERCOLS 
                 from FATHERDATABASENAME.FATHERTABLENAME 
               FATHERWHERECLAUSE
            ) 
       CHILDWHERECLAUSE
    )a
  """
  #外键检查中有异常的样例入库：外键只需要插入子表的sample；父子关系验证则需要插入子表和父表的sample
  fkCheckDetailSQL = """insert into vtable_checkdetail_${BATCHID}(
    checkId,
    runDate,
    batchId,
    DBNm,
    TabNm,
    ColNm,
    checkType,
    FCInd,
    INSERTCOLS,
    sampleCnt) 
   select INSERTCLAUSE, 
      a.* 
     from (
         select CHILDCOLLIST,
            cast(count(*)AS FLOAT ) as cnt   
           from CHILDDATABASENAME.CHILDTABLENAME
       where  (CHILDCOLS)  
          not in (
               select FATHERCOLS 
                 from FATHERDATABASENAME.FATHERTABLENAME 
               FATHERWHERECLAUSE
            ) 
       CHILDWHERECLAUSE  
    group by  CHILDCOLS
    )a  
   sample """ + str(sampleNbr)
  #可以直接在TDA上面跑的SAMPLE用SQL
  fkSampleCheckSQL = """   select CHILDCOLLIST,
      cast(count(*) AS FLOAT )as cnt   
     from CHILDDATABASENAME.CHILDTABLENAME
   where  (CHILDCOLS)  
      not in (
           select FATHERCOLS 
             from FATHERDATABASENAME.FATHERTABLENAME 
           FATHERWHERECLAUSE
        ) 
      CHILDWHERECLAUSE 
group by  CHILDCOLS 
   sample """ + str(sampleNbr)


  #父子关系检查SQL：双向的验证，在fkCheckSQL的基础上，加上一个反向的验证，failedCnt记得相加。
  #sample样例存两份的，一份放的是父表的，一份放的是子表的sample样例（库名、表名、字段名都放对应的）
  #正向的验证，用fkCheckSQL就行  不需要 distinct ，而是用group by
  #父子关系的反向的验证
  fcCheckSQL = """   select cast(count(*)AS FLOAT ) as failedCnt 
     from FATHERDATABASENAME.FATHERTABLENAME
   where  (FATHERCOLS)  
      not in (
           select CHILDCOLS 
             from CHILDDATABASENAME.CHILDTABLENAME 
           CHILDWHERECLAUSE
        ) 
      FATHERWHERECLAUSE
  """
  #sample样例的插入
  #正向的样例用fkCHeckDetailSQL即可
  #反向的如下： 
  fcCheckDetailSQL = """insert into vtable_checkdetail_${BATCHID}(
    checkId,
    runDate,
    batchId,
    DBNm,
    TabNm,
    ColNm,
    checkType,
    FCInd,
    FATHERINSERTCOLS,
    sampleCnt) 
   select   FATHERINSERTCLAUSE, 
      a.* 
     from (
         select FATHERCOLLIST,
            cast(count(*)AS FLOAT ) as cnt  
           from FATHERDATABASENAME.FATHERTABLENAME
       where  (FATHERCOLS)  
          not in (
               select CHILDCOLS 
                 from CHILDDATABASENAME.CHILDTABLENAME  
               CHILDWHERECLAUSE
            ) 
          FATHERWHERECLAUSE 
    group by  FATHERCOLS
    )a  
   sample """ + str(sampleNbr)
  #可以直接在TDA上面跑的SAMPLE用SQL
  fcSampleCheckSQL = """   select 'F',
      FATHERCOLS,
      cast(count(*)AS FLOAT ) as cnt   
     from FATHERDATABASENAME.FATHERTABLENAME
   where  (FATHERCOLS)  
      not in (
           select CHILDCOLS 
             from CHILDDATABASENAME.CHILDTABLENAME  
           CHILDWHERECLAUSE
        ) 
      FATHERWHERECLAUSE 
group by  FATHERCOLS   """    + """
     union""" +"""
   select 'C',
      CHILDCOLS,
      count(*) as cnt  
     from CHILDDATABASENAME.CHILDTABLENAME
   where  (CHILDCOLS)  
      not in (
           select FATHERCOLS 
             from FATHERDATABASENAME.FATHERTABLENAME 
           where  1 = 1 
           FATHERWHERECLAUSE
        ) 
   CHILDWHERECLAUSE2 
group by  CHILDCOLS
          """



  #交叉链的检查
  zipCheckSQL = """   select  cast(count(*)AS FLOAT ) as failedCnt 
     from (
       select row_number() over (partition by COLLIST order by STARTDT asc) as id ,
          STARTDT,
          COLLIST 
         from DATABASENAME.TABLENAME 
       WHERECLAUSE
    )A  
inner join    (
       select row_number() over (partition by COLLIST order by STARTDT asc) as id ,
          ENDDT,
          COLLIST 
         from DATABASENAME.TABLENAME 
       WHERECLAUSE
    )B 
       on JOINCLAUSE 
     and  A.id - 1 = B.id 
     and  A.STARTDT < B.ENDDT     
    """
  #sample样例的插入:SELECTCOLS 为A.COLLIST,A.STARTDT
  zipCheckDetailSQL =  """insert into vtable_checkdetail_${BATCHID}(
    checkId,
    runDate,
    batchId,
    DBNm,
    TabNm,
    ColNm,
    checkType,
    FCInd,
    INSERTCOLS) 
   select INSERTCLAUSE,
      SELECTCOLS 
     from (
       select row_number() over (partition by COLLIST order by STARTDT asc) as id ,
          STARTDT,
          COLLIST 
         from DATABASENAME.TABLENAME 
       WHERECLAUSE
    )A  
inner join  (
       select row_number() over (partition by COLLIST order by STARTDT asc) as id ,
          ENDDT,
          COLLIST 
         from DATABASENAME.TABLENAME 
       WHERECLAUSE
    )B 
        on  JOINCLAUSE 
       and  A.id - 1 = B.id 
       and  A.STARTDT < B.ENDDT     
   sample  """ + str(sampleNbr)
  #可以直接在TDA上面跑的SAMPLE用SQL
  zipSampleCheckSQL = """   select  * 
     from (
           select row_number() over (partition by COLLIST order by STARTDT asc) as id ,
              STARTDT,
              COLLIST 
             from DATABASENAME.TABLENAME 
           WHERECLAUSE
        )A  
inner join  (
       select row_number() over (partition by COLLIST order by STARTDT asc) as id ,
          ENDDT,
          COLLIST 
         from DATABASENAME.TABLENAME 
       WHERECLAUSE
    )B 
        on  JOINCLAUSE 
       and  A.id - 1 = B.id 
       and  A.STARTDT < B.ENDDT     
   sample """ + str(sampleNbr)


  #值域验证SQL
  #代码值可空的情况
  codeCheckSQL = """   select cast(count(*)AS FLOAT ) as failedCnt  
     from DATABASENAME.TABLENAME 
   where  CODECOL not in (VALUELIST)  
      and coalesce(CODECOL,'') <> ''   
   WHERECLAUSE"""
  #代码值不可空的情况
  codeCheckSQLNotEmpty = """   select cast(count(*)AS FLOAT ) as failedCnt  
     from DATABASENAME.TABLENAME 
   where  CODECOL not in (VALUELIST)   
    WHERECLAUSE"""
  #代码值异常入库,为什么同样的SQL要跑两遍!无力吐槽啦！......
  codeDetailSQL = """insert into  vtable_codedetail_${BATCHID}(
    checkId,
    runDate,
    batchId,
    DBNm,
    TabNm,
    colNm,
    cdeVal,
    sampleCnt) 
   select INSERTVALS, 
      CODECOL,
      cast(count(*)AS FLOAT ) as cnt  
     from DATABASENAME.TABLENAME 
   where  CODECOL not in (VALUELIST)   
      and coalesce(CODECOL,'') <> ''    
      WHERECLAUSE 
group by  CODECOL 
   sample   500"""
  codeDetailSQLNotEmpty = """insert into  vtable_codedetail_${BATCHID}(
    checkId,
    runDate,
    batchId,
    DBNm,
    TabNm,
    colNm,
    cdeVal,
    sampleCnt) 
   select INSERTVALS, 
      CODECOL,
      cast(count(*) AS FLOAT )as cnt 
     from DATABASENAME.TABLENAME 
   where  CODECOL not in (VALUELIST)   
   WHERECLAUSE 
group by  CODECOL 
   sample 500 """
  #可以直接在TDA上面跑的SAMPLE用SQL
  codeSampleCheckSQL =  """   select  CODECOL,
      cast(count(*)AS FLOAT ) as cnt  
     from DATABASENAME.TABLENAME 
   where  CODECOL not in (VALUELIST)   
      and coalesce(CODECOL,'') <> ''    
      WHERECLAUSE 
group by  CODECOL 
   sample 500"""
  codeSampleCheckSQLNotEmpty = """   select CODECOL,
      cast(count(*)AS FLOAT ) as cnt  
     from DATABASENAME.TABLENAME 
   where  CODECOL not in (VALUELIST)   
     WHERECLAUSE 
group by  CODECOL 
   sample   500"""

  #自定义SQL：无需检查结果，只需要插入detail数据即可
  ucDetailSQL = """insert  into vtable_checkdetail_${BATCHID}(
    checkId,
    runDate,
    batchId,
    DBNm,
    TabNm,
    ColNm,
    checkType,
    FCInd,
    INSERTCOLS) 
   select INSERTCLAUSE,
      COLLIST 
     from   ( SQL )a 
  sample  """ + str(sampleNbr)



  #存储全部规则记录的Dict
  #基本样式如下checkRuleDict = {'PK00001':{'runInd':'Y','dbNm':'pdata','tabNm':'T03_Agmt','colList':'Agmt_id,agmt_type','whereClause':'','operator':'郑彬彬','operatInfo':'20161202A','description':''},
  #               'FK00001':{......}}
  checkRuleDict = {}
  dbDict = {}

  ###
  #主程序，全部流程都在这里面体现
  #包括读取Excel文件，跑SQL，等等其他部分
  #每一步完成后都在日志中记录下来
  #注意对异常情况的处理：
  # ①SQL跑了很久的问题
  # ②SQL跑了很久都没成功的问题
  # ③SQL插入不成功的问题
  #还需要在总的里面跑一下统计总记录数的SQL：select count（*） from xxxx
  #①对字段序列的数目进行判断（小于10），否则报错！
  #②where 1=1
  ###
  def dataCheck(self,perent,DQFileName,batchId):    
    self.createResVol= """ create volatile table vtable_checkresult_"""+ str(batchId) + """ as (select * from ${CHECKDB}.check_Result) with no data on commit preserve rows;"""
  #
    self.createDetVol= """ create volatile table vtable_checkdetail_""" + str(batchId) +""" as (select * from ${CHECKDB}.check_Detail) with no data on commit preserve rows;"""
  #
    self.createCodeVol= """ create volatile table vtable_codedetail_""" + str(batchId) +""" as (select * from ${CHECKDB}.code_Detail) with no data on commit preserve rows;"""
  
  #
    self.insertResTab=""" insert into ${CHECKDB}.check_Result select * from vtable_checkresult_""" + str(batchId) 

    self.insertDetTab=""" insert into ${CHECKDB}.check_Detail select * from vtable_checkdetail_""" + str(batchId) 

    self.insertCodeTab=""" insert into ${CHECKDB}.code_Detail select * from vtable_codedetail_""" + str(batchId) 
  
    self.dataCheckExcelParser(parent,DQFileName,batchId)
    # self.logff.write(self.checkRuleDict)
    # self.logff.write("读取验证规则文档！\n")
    self.readDBDict() 
    #udaExec = teradata.UdaExec(appName = 'DataCheck',version = '0.1' ,logConsole = False)
    self.logff.write(self.preScript)
    self.logff.write("\n")
    # self.logff.write("数据库连接成功！\n")
    print("数据库连接成功！\n")
    # self.logff.write(self.dbAddress,self.dbUser,self.dbPassWord)
    Session = None
    #创建临时表
    
    #createRestab=self.createResVol.replace("RESULTDATABASENAME",self.checkResultDb)
    #createDettab=self.createDetVol.replace("RESULTDATABASENAME",self.checkResultDb)
    #createCodetab=self.createCodeVol.replace("RESULTDATABASENAME",self.checkResultDb)
    try:
      ##Session.execute(createRestab)
      ##Session.execute(createDettab)
      self.logff.write("""print BTEQ "------  创建checkResult的临时表语句：-----;\\n";""")
      self.logff.write("\n")
      self.logff.write("""print BTEQ "%s\\n";"""%self.createResVol)
      self.logff.write("\n")
      self.logff.write("""print BTEQ "------  创建checkDetail的临时表语句：-----;\\n";""")
      self.logff.write("\n")
      self.logff.write("""print BTEQ "%s\\n";"""%self.createDetVol)
      self.logff.write("\n")
      self.logff.write("""print BTEQ "------  创建codeDetail的临时表语句：-----;\\n";""")
      self.logff.write("\n")
      self.logff.write("""print BTEQ "%s\\n";"""%self.createCodeVol)
      self.logff.write("\n")
    except Exception as e:
      print("Exception如下：%s"%e)
      print(" Failed:创建临时表失败！\n")
    
    self.logff.write("""print BTEQ "-------字段唯一性验证：-------;\\n";""")
    self.logff.write("\n")
    print("开始字段唯一性验证！\n")
    # self.logff.write(self.logDis)
    self.primaryKeyCheck(Session,self.checkRuleDict[self.pkCheck])
    self.logff.write("\n")
    self.logff.write("""print BTEQ "-------外键完整性验证：-------;\\n";""")
    self.logff.write("\n")
    print("开始外键完整性验证！\n")
    # self.logff.write(self.logDis)
    self.foreignKeyCheck2(Session,self.checkRuleDict[self.fkCheck])
    self.logff.write("\n")
    self.logff.write("""print BTEQ "-------父子关系验证：-----------;\\n";""")
    self.logff.write("\n")
    print("开始父子关系验证！\n")
    # self.logff.write(self.logDis)
    self.fatherChildCheck(Session,self.checkRuleDict[self.fcCheck])
    self.logff.write("\n")
    # self.logff.write("-------交叉链验证：-------\n")
    self.logff.write("""print BTEQ "-------交叉链验证：-----------;\\n";""")
    self.logff.write("\n")
    print("开始交叉链验证！\n")
    # self.logff.write(self.logDis)
    #self.zipCheck(Session,self.checkRuleDict[self.zcCheck])
    #self.logff.write("\n")

    self.logff.write("""print BTEQ "-------值域（手工设置）验证：-------;\\n";""")
    self.logff.write("\n")
    print("开始值域-手工设置验证！\n")
    # self.logff.write(self.logDis)
    self.valCheck(Session,self.checkRuleDict[self.vcCheck])
    self.logff.write("\n")
    self.logff.write("""print BTEQ "-------值域（SA）验证：-------;\\n";""")
    self.logff.write("\n")
    print("开始值域-SA验证！\n")
    # self.logff.write(self.logDis)
    self.sdataCdeCheck(Session,self.checkRuleDict[self.scCheck])
    self.logff.write("\n")
    self.logff.write("""print BTEQ "-------值域（PDM）验证：-------;\\n";""")
    self.logff.write("\n")
    print("开始值域-PDM验证！\n")
    # self.logff.write(self.logDis)
    self.pdmCdeCheck(Session,self.checkRuleDict[self.pcCheck])
    self.logff.write("\n")
    self.logff.write("""print BTEQ "-------自定义SQL验证：-------;\\n";""")
    self.logff.write("\n")
    print("开始自定义SQL验证！\n")
    # self.logff.write(self.logDis)
    self.ugcSQLCheck(Session,self.checkRuleDict[self.ucCheck])
    self.logff.write("\n")
    self.logff.write("""print BTEQ "-------自定义规则验证：-------;\\n";""")
    self.logff.write("\n")
    print("开始自定义规则验证！\n")
    # self.logff.write(self.logDis)
    self.uuSQLCheck(Session,self.checkRuleDict[self.uuCheck])
    self.logff.write("\n")

    insertResVol=self.insertResTab.replace("RESULTDATABASENAME",self.checkResultDb)
    insertDetVol=self.insertDetTab.replace("RESULTDATABASENAME",self.checkResultDb)
    insertCodeVol=self.insertCodeTab.replace("RESULTDATABASENAME",self.checkResultDb)
    try:
      ##Session.execute(insertResVol)
      ##Session.execute(insertDetVol)
      self.logff.write("""print BTEQ "----- 插入checkResult结果表语句：-----;\\n";""")
      self.logff.write("\n")
      self.logff.write("""print BTEQ "%s\\n" ;"""%insertResVol)
      self.logff.write("\n")
      self.logff.write("""print BTEQ "----- 插入checkDetail结果表语句：-----;\\n";""")
      self.logff.write("\n")
      self.logff.write("""print BTEQ ";%s\\n" ;"""%insertDetVol)
      self.logff.write("\n")
      self.logff.write("""print BTEQ "----- 插入codeDetail结果表语句：-----;\\n";""")
      self.logff.write("\n")
      self.logff.write("""print BTEQ ";%s;\\n";"""%insertCodeVol)
      self.logff.write("\n")
    except Exception as e:
      print("Exception如下：%s"%e)
      print(" Failed:临时表插入结果表失败！\n")

    
    # Session.close()
    # self.logff.write("全部规则验证完毕\n")

    allRuleCnt  = self.pkRuleCnt + self.fkRuleCnt + self.fcRuleCnt + self.pcRuleCnt + self.scRuleCnt + self.vcRuleCnt + self.zcRuleCnt + self.ucRuleCnt + self.uuRuleCnt

    print("总的规则数：%s 条！\n"%allRuleCnt)
    # self.logff.write("总的规则数：%s 条！\n"%allRuleCnt)

    self.logff.write(self.postScript)
    self.logff.write("\n")
    
    self.logff.close()


  #######
  #读取数据库变量的信息。文件位置是本程序同目录下的DBParams.txt
  #读取文件内容
  #解析后放入到dbDict这个字典变量中
  #######
  def readDBDict(self):
    for line in open(self.DBParams):
      self.dbDict[line.split(":")[0]] = line.split(":")[1].replace('\n','')

  #####
  #读取代码验证模板Excel(和程序文件同目录下的.\\数据验证文件\\目录下的Excel文件！)
  #拼接库名、表名等。注意替换库名！以及对于额外的where条件的拼接！
  #注意对数据的验证：
  # ①规则编号不能重复
  # ②数据的正确性需要保证(一些中文逗号的替换，一些空值的处理等)
  # ③注意对一些基本的参数给赋值，如库名、表名、数据库连接等等
  #   PDATA、SDATA、验证通过标志、批次号、运行日期、PDM表的位置、SA代码表位置
  # ④将各checkType对应sheet的内容存放到内存中！
  #   规则编号作为key[或者程序自动生成，如PKCheck的第一条就是PK00001]，具体的记录List作为value
  #     这里还是将具体的规则当做Dict来做，而不是简单地弄成List比较好，便于后期修改以后也能够畅快地用起来
  #   这样也就不用校验规则编号是否重复啦~
  # 同时给出一个是否需要读取SA文件的flag！
  ####
  def dataCheckExcelParser(self,parent,DQFileName,batchId):
    #读取“配置项”Sheet，将Excel内容填充到各个参数内

    #Excel对象下的sheet对象
    dataCheckExcel = None
    configCheckSheet = None
    pkCheckSheet = None
    fkCheckSheet = None
    fcCheckSheet = None
    pcCheckSheet = None
    scCheckSheet = None
    vcCheckSheet = None
    zcCheckSheet = None
    ucCheckSheet = None
    uuCheckSheet = None
    nameLis=[]
    
    self.batchId = batchId
    logFile = self.logFileDir + DQFileName + batchId + ".pl"
    nameLis = DQFileName.split('-')
    logFile = self.logFileDir + "-".join([batchId,nameLis[2],nameLis[3]])[:-5] + ".pl"
    logFile1 = ".\\Log\\123.pl"
    print("logfile1")
    logff1 = codecs.open(logFile,'w','GB18030')
    print("logff1")
    #self.logff = codecs.open(logFile,'w','utf-8')  
    self.logff = codecs.open(logFile,'w','GB18030')

    self.logff.close
    #self.logff = codecs.open(logFile,'w','utf-8')
    self.logff = codecs.open(logFile,'w','GB18030')

        # self.logff.write("当前时间:" + time.strftime("%Y-%m-%d %H:%M") +"\n")


    #读取文件为Excel对象
    try:
      dataCheckExcel = xlrd.open_workbook(filename = os.path.join(parent,DQFileName))
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件不能读取！请检查文件格式并重试！" + "\n")
      print("验数Excel文件不能读取！请检查文件格式并重试！" + "\n")
            # os._exit(0)
          #读取“配置项”sheet
    try:
      configCheckSheet = dataCheckExcel.sheet_by_name(self.configSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件中未包含‘%s’Sheet，请检查并重试！\n" %self.configSheet)
      print("验数Excel文件中未包含‘%s’Sheet，请检查并重试！\n" %self.configSheet)
            # os._exit(0)
          #读取主键重复规则sheet
    try:
      pkCheckSheet = dataCheckExcel.sheet_by_name(self.pkSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
      print("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.pkSheet)
            # os._exit(0)
          #其他sheet同理
          #外键
    try:
      fkCheckSheet = dataCheckExcel.sheet_by_name(self.fkSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.fkSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.fkSheet)
            # os._exit(0)
          #父子关系fc
    try:
      fcCheckSheet = dataCheckExcel.sheet_by_name(self.fcSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.fcSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.fcSheet)
            # os._exit(0)
          #PDM代码pc
    try:
      pcCheckSheet = dataCheckExcel.sheet_by_name(self.pcSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.pcSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.pcSheet)
            # os._exit(0)
          #SA代码sc
    try:
      scCheckSheet = dataCheckExcel.sheet_by_name(self.scSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.scSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.scSheet)
            # os._exit(0)
          #值域vc
    try:
      vcCheckSheet = dataCheckExcel.sheet_by_name(self.vcSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.vcSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.vcSheet)
            # os._exit(0)
          #交叉链zc
    try:
      zcCheckSheet = dataCheckExcel.sheet_by_name(self.zcSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.zcSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.zcSheet)
            # os._exit(0)
          #自定义SQL-uc
    try:
      ucCheckSheet = dataCheckExcel.sheet_by_name(self.ucSheet)
    except Exception as e:
            # self.logff.write("Exception如下：%s"%e)
            # self.logff.write("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.ucSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.ucSheet)
            # os._exit(0)
          #自定义规则-uu
    try:
      uuCheckSheet = dataCheckExcel.sheet_by_name(self.uuSheet)
    except Exception as e:
      #self.logff.write("Exception如下：%s"%e)
      #self.logff.write("Failed:验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.uuSheet)
      print("验数Excel文件中未包含'%s'Sheet，请检查重试！\n" %self.uuSheet)
            # os._exit(0)

    #配置项sheet解析
    self.dbAddress = dqConfig.dqConfig['数据库IP地址']
    self.dbUser = dqConfig.dqConfig['数据库用户名']
    self.dbPassWord = dqConfig.dqConfig['数据库密码']
    #self.pdata = dqConfig.dqConfig['PDATA']
    self.pdata = "${PDATA}"
    #self.sdata = dqConfig.dqConfig['SDATA']
    self.sdata = "${SDATA}"
    self.checkFlag = dqConfig.dqConfig['验证通过标志']
    self.caseNo = self.batchId+"${SUB_BATCH_ID}"
    
    #self.txnDate = dqConfig.dqConfig['运行日期']
    self.txnDate = "${TXNDATE}"
    
    self.saFilePath = dqConfig.dqConfig['SA代码表位置']

    self.checkResultDb = dqConfig.dqConfig['验数结果数据库']
    
    self.TXNDATE = dqConfig.dqConfig["""数据日期（${TXNDATE}）"""]
    if '' == self.TXNDATE:
      self.TXNDATE = '${TXNDATE}'
        # print(self.TXNDATE)

    #if '' == self.dbAddress or '' == self.dbUser or '' == self.dbPassWord or '' == self.pdata or '' == self.sdata or '' == self.checkFlag or '' == self.caseNo or '' ==  self.txnDate  or '' == self.checkResultDb:
      # self.logff.write("Failed:配置项不完整！请检查重试！" + "\n")
    # print("配置项不完整！请检查重试！" + "\n")
    # os._exit(0)

    # self.logff.write("本次运行日期为：%s，批次号为：%s ！"%(self.txnDate,self.caseNo))
    print("本次运行日期为：%s，批次号为：%s ！\n"%(self.txnDate,self.caseNo))
    # self.logff.write("dbAddress:%s"%self.dbAddress)


    #读取“主键重复”sheet，将内容填充到嵌套的Dict中
    #存放主键重复检查规则的Dict
    pkDict = {}
    for i in range(1,pkCheckSheet.nrows):
      pkInfo = pkCheckSheet.row_values(i)[0:9]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = pkInfo[0]
      pDict['dbNm'] = pkInfo[1]
      pDict['tabNm'] = pkInfo[2]
      pDict['tabRuleId'] = pkInfo[3]
      pDict['colList'] = pkInfo[4]
      pDict['whereClause'] = pkInfo[5]
      pDict['operator'] = pkInfo[6]
      pDict['operatInfo'] = pkInfo[7]
      pDict['description'] = pkInfo[8]
      pkDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.pkCheck
    self.checkRuleDict[checkType] = pkDict

    #读取“外键检查”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc]
    fkDict = {}
    for i in range(1,fkCheckSheet.nrows):
      fkInfo = fkCheckSheet.row_values(i)[0:12]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = fkInfo[0]
      pDict['dbNm'] = fkInfo[1]
      pDict['tabNm'] = fkInfo[2]
      pDict['colList'] = fkInfo[3]
      pDict['whereClause'] = fkInfo[4]
      pDict['fatherDbNm'] = fkInfo[5]
      pDict['fatherTabNm'] = fkInfo[6]
      pDict['fatherCollist'] = fkInfo[7]
      pDict['fatherWhereClause'] = fkInfo[8]
      pDict['operator'] = fkInfo[9]
      pDict['operatInfo'] = fkInfo[10]
      pDict['description'] = fkInfo[11]
      fkDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.fkCheck
    self.checkRuleDict[checkType] = fkDict

    #读取“父子关系”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc]
    fcDict = {}
    for i in range(1,fcCheckSheet.nrows):
      fcInfo = fcCheckSheet.row_values(i)[0:12]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = fcInfo[0]
      pDict['dbNm'] = fcInfo[1]
      pDict['tabNm'] = fcInfo[2]
      pDict['colList'] = fcInfo[3]
      pDict['whereClause'] = fcInfo[4]
      pDict['fatherDbNm'] = fcInfo[5]
      pDict['fatherTabNm'] = fcInfo[6]
      pDict['fatherCollist'] = fcInfo[7]
      pDict['fatherWhereClause'] = fcInfo[8]
      pDict['operator'] = fcInfo[9]
      pDict['operatInfo'] = fcInfo[10]
      pDict['description'] = fcInfo[11]
      fcDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.fcCheck
    self.checkRuleDict[checkType] = fcDict

    #读取“历史拉链表”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc]
    zcDict = {}
    for i in range(1,zcCheckSheet.nrows):
      zcInfo = zcCheckSheet.row_values(i)[0:10]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = zcInfo[0]
      pDict['dbNm'] = zcInfo[1]
      pDict['tabNm'] = zcInfo[2]
      pDict['stDt'] = zcInfo[3]
      pDict['endDt'] = zcInfo[4]
      pDict['colList'] = zcInfo[5]
      pDict['whereClause'] = zcInfo[6]
      pDict['operator'] = zcInfo[7]
      pDict['operatInfo'] = zcInfo[8]
      pDict['description'] = zcInfo[9]
      zcDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.zcCheck
    self.checkRuleDict[checkType] = zcDict

    #读取“值域（PDM）”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc]
    pcDict = {}
    for i in range(1,pcCheckSheet.nrows):
      sheetInfo = pcCheckSheet.row_values(i)[0:11]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = sheetInfo[0]
      pDict['dbNm'] = sheetInfo[1]
      pDict['tabNm'] = sheetInfo[2]
      pDict['colList'] = sheetInfo[3]
      pDict['isEmpty'] = sheetInfo[6]
      pDict['pdmCde'] = sheetInfo[4]
      pDict['pdmCdeCol'] = sheetInfo[5]
      pDict['whereClause'] = sheetInfo[7]
      pDict['operator'] = sheetInfo[8]
      pDict['operatInfo'] = sheetInfo[9]
      pDict['description'] = sheetInfo[10]
      pcDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.pcCheck
    self.checkRuleDict[checkType] = pcDict
    #其他......

    #读取“值域（SA）”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc]
    scDict = {}
    for i in range(1,scCheckSheet.nrows):
      sheetInfo = scCheckSheet.row_values(i)[0:9]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = sheetInfo[0]
      pDict['dbNm'] = sheetInfo[1]
      pDict['tabNm'] = sheetInfo[2]
      pDict['colList'] = sheetInfo[3]
      pDict['isEmpty'] = sheetInfo[4]
      pDict['whereClause'] = sheetInfo[5]
      pDict['operator'] = sheetInfo[6]
      pDict['operatInfo'] = sheetInfo[7]
      pDict['description'] = sheetInfo[8]
      scDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.scCheck
    self.checkRuleDict[checkType] = scDict

    #读取“值域（手动设置）”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc]
    vcDict = {}
    for i in range(1,vcCheckSheet.nrows):
      sheetInfo = vcCheckSheet.row_values(i)[0:11]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = sheetInfo[0]
      pDict['dbNm'] = sheetInfo[1]
      pDict['tabNm'] = sheetInfo[2]
      pDict['colList'] = sheetInfo[3]
      pDict['valType'] = sheetInfo[4]
      pDict['valScale'] = sheetInfo[5]
      pDict['isEmpty'] = sheetInfo[6]
      pDict['whereClause'] = sheetInfo[7]
      pDict['operator'] = sheetInfo[8]
      pDict['operatInfo'] = sheetInfo[9]
      pDict['description'] = sheetInfo[10]
      vcDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.vcCheck
    self.checkRuleDict[checkType] = vcDict

    #读取“自定义SQL”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc]
    ucDict = {}
    for i in range(1,ucCheckSheet.nrows):
      sheetInfo = ucCheckSheet.row_values(i)[0:9]
      #内部再维护一个Dict
      pDict = {}
      pDict['runInd'] = sheetInfo[0]
      pDict['ugcType'] = sheetInfo[1]
      pDict['dbNm'] = sheetInfo[2]
      pDict['tabNm'] = sheetInfo[3]
      pDict['colList'] = sheetInfo[4]
      pDict['sql'] = sheetInfo[5]
      pDict['operator'] = sheetInfo[6]
      pDict['operatInfo'] = sheetInfo[7]
      pDict['description'] = sheetInfo[8]
      ucDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.ucCheck
    self.checkRuleDict[checkType] = ucDict

    #读取“自定义规则”sheet，将内容填充到嵌套的Dict中[fk,fc,pc,sc,vc,zc,uc,uu]
    uuDict = {}
    if None != uuCheckSheet:
      for i in range(1,uuCheckSheet.nrows):
        sheetInfo = uuCheckSheet.row_values(i)[0:10]
        #内部再维护一个Dict
        pDict = {}
        pDict['runInd'] = sheetInfo[0]
        pDict['ugcType'] = sheetInfo[1]
        pDict['dbNm'] = sheetInfo[2]
        pDict['tabNm'] = sheetInfo[3]
        pDict['colList'] = sheetInfo[4]
        pDict['sql'] = sheetInfo[5]
        pDict['correctNum'] = sheetInfo[6]
        pDict['operator'] = sheetInfo[7]
        pDict['operatInfo'] = sheetInfo[8]
        pDict['description'] = sheetInfo[9]
        uuDict[i] = pDict
    #将规则放入到总的规则Dict中
    checkType = self.uuCheck
    self.checkRuleDict[checkType] = uuDict


  ####
  #执行主键检查
  #注意对于检查结果记录sample20，以及这些记录如何入库的问题
  #检查key的前两位，如果为PK那就是pkCheck
  #然后读取传递过来的Dict，跑SQL，记日志，记录结果
  ####
  def primaryKeyCheck(self,session,pkDict):

    print(" 总共有%s条规则！"%len(pkDict))
    for k,pDict in pkDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = pDict['runInd']
      #库名
      dbNm = pDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = pDict['tabNm'] 
      #规则内子编号，如果为空，则默认为0-主键
      tabRuleId = pDict['tabRuleId']
      #主键字段序列
      colList = pDict['colList']
      #去掉字段别名后的字段序列
      colList2 = colList.lower()
      rf = re.findall(self.pattern,colList2)
      if [] != rf:
        for f in rf:
          colList2 = colList2.replace(f,'')

      # self.logff.write("记录%s：%s 库 %s 表 %s 字段 [%s]\n"%(k,dbNm,tabNm,colList,tabRuleId))
      print("记录%s：%s 库 %s 表 %s 字段 [%s]\n"%(k,dbNm,tabNm,colList,tabRuleId))

      if '' == tabRuleId:
        tabRuleId = 0
      else:
        tabRuleId = str(tabRuleId).replace("-主键",'').replace("-候选键","")
      #where条件
      whereClause = pDict['whereClause']
      #维护人
      operator = pDict['operator']
      #维护信息
      operatInfo = pDict['operatInfo']
      #备注
      description = pDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "pk" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.pkRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)

          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)

          whereClause = ' where 1=1 and ' + whereClause

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          # self.logff.write("  Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+1):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          # self.logff.write("  Failed:字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'",""),'PK'))
        insertClause = "'" + insertClause + "',''"

        # self.logff.write(self.pdata,dbNm +"\n")

        pkSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE",whereClause).replace('${BATCHID}',self.batchId)
        pkCheckSQL = self.checkSQL.replace("COLLIST2",colList2).replace("WHERECLAUSE",whereClause).replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace('${BATCHID}',self.batchId)
        pkCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        pkCheckDetailSQL = self.checkDetailSQL.replace("COLLIST2",colList2).replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace('${BATCHID}',self.batchId)
        pkSampleCheckSQL = self.sampleCheckSQL.replace("COLLIST2",colList2).replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace('${BATCHID}',self.batchId)
        
        # self.logff.write("查询全部记录数：%s"%pkSelCnt + "\n")
        # self.logff.write("%s"%pkSampleCheckSQL + ";\n")
        # self.logff.write("  结果插入到库表中：%s"%pkCheckResultSQL + "\n")
        # self.logff.write("  结果样例插入到库表中:%s"%pkCheckDetailSQL + "\n")

        #运行检查表全部记录数的SQL
        # try:
        #   for row in session.execute(pkSelCnt):
        #     # self.logff.write("  全部记录数：%s\n"%row["cnt"])
        #     allRowCnt = row["cnt"]
        # except Exception as e:
        #   # self.logff.write("Exception如下：%s"%e)
        #   # self.logff.write("  Failed:查询全部记录数的SQL运行失败，请检查验证规则文件！\n")

        #运行验数主键重复记录数的sql
        # try:
        #   for row in session.execute(pkCheckSQL):
        #     # self.logff.write("  未通过验证的记录数：%s\n"%row["failedCnt"])
        #     failedRowCnt = row["failedCnt"]
        # except Exception as e:
        #   # self.logff.write("Exception如下：%s"%e)
        #   # self.logff.write("  Failed:查询未通过主键重复验证的记录数的SQL运行失败，请检查验证规则文件！\n")
        #   isOk = 'Failed'
        #   sqlCheckResult = 'N'

        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL,description)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append(tabRuleId)
          insertData.append(colList)
          insertData.append('PK')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt)
          insertData.append(pkSampleCheckSQL)
          insertData.append(description)

          # self.logff.write(insertData)

          # session.execute(pkCheckResultSQL,insertData)
          # self.logff.write("  插入结果数据成功！\n")

          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList,'PK',''])
          IS = "'" + IS + "'"
          pkCheckResultSQL = pkCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",pkSampleCheckSQL.replace("'","''")).replace('CHECKSQL',pkCheckSQL).replace('SELCNTSQL',pkSelCnt)

          
          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%pkCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL:---------\n")
          self.logff.write(";%s"%pkCheckDetailSQL + ";\n")


        except Exception as e:
          print("Exception如下：%s"%e)
          # self.logff.write("Exception如下：%s"%e)
          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")



        #运行插入结果样例的SQL
        # if 0 != failedRowCnt :
        #   try:
        #     session.execute(pkCheckDetailSQL)
        #     # self.logff.write("  插入样例数据成功！\n")
        #   except Exception as e:
        #     # self.logff.write("Exception如下：%s"%e)
        #     # self.logff.write("  Failed:将样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")

        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
      # else:
      #   print("该条规则不用运行！\n" )
      #   # self.logff.write("  该条规则不用运行！" + "\n")
        self.logff.write(self.logDis)
      self.logff.write("ENDOFINPUT\n")
    # self.logff.write("共有 %s 条主键重复检查规则！\n\n"%self.pkRuleCnt)

  ####
  #执行外键检查
  #检查key的前两位，如果为FK那就是fkCheck
  ####
  def foreignKeyCheck(self,session,ruleDict):
    print(" 总共有%s条规则！"%len(ruleDict))
    for k,rDict in ruleDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = rDict['runInd']
      #库名
      dbNm = rDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = rDict['tabNm'] 
      
      fatherDbNm = rDict['fatherDbNm']
      if 'pdata' == fatherDbNm.lower():
        fatherDbNm = self.pdata
      elif 'sdata' == fatherDbNm.lower():
        fatherDbNm = self.sdata
      fatherTabNm = rDict['fatherTabNm']
      fatherColList = rDict['fatherCollist']
      #去掉字段别名后的字段序列
      fatherColList2 = fatherColList.lower()
      rf = re.findall(self.pattern,fatherColList2)
      if [] != rf:
        for f in rf:
          fatherColList2 = fatherColList2.replace(f,'')
      fatherWhereClause = rDict['fatherWhereClause']
      #主键字段序列
      colList = rDict['colList']
      #去掉字段别名后的字段序列
      colList2 = colList.lower()
      rf = re.findall(self.pattern,colList2)
      if [] != rf:
        for f in rf:
          colList2 = colList2.replace(f,'')

      self.logff.write("记录%s：\n   子表：%s 库 %s 表 %s 字段[外键完整性]\n   父表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))
      print("记录%s：\n  子表：%s 库 %s 表 %s 字段[外键完整性]\n   父表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))

      #where条件
      whereClause = rDict['whereClause']
      #维护人
      operator = rDict['operator']
      #维护信息
      operatInfo = rDict['operatInfo']
      #备注
      description = rDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "fk" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.fkRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)          
          whereClause = '  and ' + whereClause

        if('' != fatherWhereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          fatherWhereClause = fatherWhereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            fatherWhereClause = fatherWhereClause.replace(dbKey,dbValue)          
          fatherWhereClause = 'where 1 = 1  and ' + fatherWhereClause

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          self.logff.write("  Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+1):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          self.logff.write("  Failed:字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'",""),'FK'))
        insertClause = "'" + insertClause + "','C'"

        # self.logff.write(self.pdata,dbNm +"\n")

        #将模板SQL里面的库名、表名、字段名等内容替换掉
        fkSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE","where 1=1  "+whereClause).replace('${BATCHID}',self.batchId)
        fkCheckSQL = self.fkCheckSQL.replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList).replace("FATHERWHERECLAUSE",fatherWhereClause).replace('${BATCHID}',self.batchId)
        fkCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        fkCheckDetailSQL = self.H.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("CHILDCOLLIST",colList).replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList).replace("FATHERWHERECLAUSE",fatherWhereClause).replace('${BATCHID}',self.batchId)
        fkSampleCheckSQL = self.fkSampleCheckSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("CHILDCOLLIST",colList).replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList).replace("FATHERWHERECLAUSE",fatherWhereClause).replace('${BATCHID}',self.batchId)
        
        # self.logff.write("查询全部记录数：%s"%fkSelCnt + "\n")
        self.logff.write("  查询SQL：%s"%fkCheckSQL + "\n")
        # self.logff.write("结果插入到库表中：%s"%fkCheckResultSQL + "\n")
        self.logff.write("  结果样例插入到库表中:%s"%fkCheckDetailSQL + "\n")

        #运行检查表全部记录数的SQL
        try:
          for row in session.execute(fkSelCnt):
            self.logff.write("  全部记录数：%s\n"%row["cnt"])
            allRowCnt = row["cnt"]
        except Exception as e:
          self.logff.write("Exception如下：%s"%e)
          self.logff.write("  Failed:查询全部记录数的SQL运行失败，请检查验证规则文件后重试！\n")

        #运行验数主键重复记录数的sql
        try:
          for row in session.execute(fkCheckSQL):
            self.logff.write("  未通过验证的记录数：%s\n"%row["failedCnt"])
            failedRowCnt = row["failedCnt"]
        except Exception as e:
          self.logff.write("Exception如下：%s"%e)
          self.logff.write("  Failed:查询未通过外键完整性验证的记录数的SQL运行失败，请检查验证规则文件后重试！\n")
          isOk = 'Failed'
          sqlCheckResult = 'N'

        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append('')
          insertData.append(colList)
          insertData.append('FK')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt)
          insertData.append(fkSampleCheckSQL)
          insertData.append(description)
          # self.logff.write(insertData)

          session.execute(fkCheckResultSQL,insertData)
          self.logff.write("  插入结果数据成功！\n")

        except Exception as e:
          self.logff.write("Exception如下：%s"%e)
          self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        if 0 != failedRowCnt :
          try:
            session.execute(fkCheckDetailSQL)
            self.logff.write("  插入样例数据成功！\n")
          except Exception as e:
            self.logff.write("Exception如下：%s"%e)
            self.logff.write("  Failed:将样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")

        self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
        
      else:
        self.logff.write("  该条规则不用运行！" + "\n")

      self.logff.write(self.logDis)
      self.logff.write("ENDOFINPUT\n")

    #self.logff.write("共有 %s 条外键完整性检查规则！\n\n"%self.fkRuleCnt)

  ####
  #执行外键检查
  #检查key的前两位，如果为FK那就是fkCheck
  #允许子表的字段数大于等于父表的字段数。也就是说允许子表查询如Src_tab_id这样的内容
  ####
  def foreignKeyCheck2(self,session,ruleDict):
    print(" 总共有%s条规则！"%len(ruleDict))
    for k,rDict in ruleDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = rDict['runInd']
      #库名
      dbNm = rDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = rDict['tabNm'] 
      
      fatherDbNm = rDict['fatherDbNm']
      if 'pdata' == fatherDbNm.lower():
        fatherDbNm = self.pdata
      elif 'sdata' == fatherDbNm.lower():
        fatherDbNm = self.sdata
      fatherTabNm = rDict['fatherTabNm']
      fatherColList = rDict['fatherCollist']
      #去掉字段别名后的字段序列
      fatherColList2 = fatherColList.lower()
      rf = re.findall(self.pattern,fatherColList2)
      if [] != rf:
        for f in rf:
          fatherColList2 = fatherColList2.replace(f,'')
      fatherWhereClause = rDict['fatherWhereClause']
      #主键字段序列
      colList = rDict['colList']
      #针对子表的字段数多于父表的字段数的情况
      colList2 = rDict['colList'].lower()
      rf = re.findall(self.pattern,colList2)
      if [] != rf:
        # print(rf)
        for f in rf:
          colList2 = colList2.replace(f,'')

      # self.logff.write("记录%s：\n   子表：%s 库 %s 表 %s 字段[外键完整性]\n   父表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))

      print("记录%s：\n  子表：%s 库 %s 表 %s 字段[外键完整性]\n   父表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))

      #对colList的处理，如果其split之后的长度大于fatherColList的长度，就截取下
      colList2 = colList2.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
      fatherColList2 = fatherColList2.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')

      if len(colList.split(",")) > len(fatherColList.split(",")):
        # self.logff.write("Warning:该条规则的子表字段数多于父表的字段数！\n")
        colList2 = ",".join(colList2.split(",")[0:len(fatherColList2.split(","))])
      elif len(colList.split(",")) < len(fatherColList.split(",")):
        # self.logff.write("Warning！外键检查中，该条规则的子表字段数少于父表字段数！请检查后重试！\n")
        print("记录%s：\n  子表：%s库 %s表 %s字段[外键完整性]\n  父表：%s库 %s表 %s字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))
        print("Warning！外键检查中，该条规则的子表字段数少于父表字段数！请检查是否为COALESCE表达式！\n")
        # continue

      #where条件
      whereClause = rDict['whereClause']
      #维护人
      operator = rDict['operator']
      #维护信息
      operatInfo = rDict['operatInfo']
      #备注
      description = rDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "fk" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.fkRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)          
          whereClause = '  and ' + whereClause

        if('' != fatherWhereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          fatherWhereClause = fatherWhereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            fatherWhereClause = fatherWhereClause.replace(dbKey,dbValue)          
          fatherWhereClause = 'where 1 = 1  and ' + fatherWhereClause

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList2 = colList2.replace("，",",").replace("，",",").replace("，",",")
        cols = colList2.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          # self.logff.write("  Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+1):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          # self.logff.write("  Failed:字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'",""),'FK'))
        insertClause = "'" + insertClause + "','C'"

        # self.logff.write(self.pdata,dbNm +"\n")

        #将模板SQL里面的库名、表名、字段名等内容替换掉
        fkSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE","where 1=1  "+whereClause).replace('${BATCHID}',self.batchId)
        fkCheckSQL = self.fkCheckSQL.replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList).replace("FATHERWHERECLAUSE",fatherWhereClause).replace("CHILDCOLLIST",colList).replace('${BATCHID}',self.batchId)
        fkCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        fkCheckDetailSQL = self.fkCheckDetailSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("CHILDCOLLIST",colList).replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList).replace("FATHERWHERECLAUSE",fatherWhereClause).replace('${BATCHID}',self.batchId)
        fkSampleCheckSQL = self.fkSampleCheckSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("CHILDCOLLIST",colList).replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList).replace("FATHERWHERECLAUSE",fatherWhereClause).replace('${BATCHID}',self.batchId)
        
        # self.logff.write("查询全部记录数：%s"%fkSelCnt + "\n")
        # self.logff.write("  查询SQL：%s"%fkCheckSQL + "\n")
        # self.logff.write("结果插入到库表中：%s"%fkCheckResultSQL + "\n")
        # self.logff.write("%s"%fkSampleCheckSQL + ";\n")

        #运行检查表全部记录数的SQL
        # try:
        #   for row in session.execute(fkSelCnt):
        #     # self.logff.write("  全部记录数：%s\n"%row["cnt"])
        #     allRowCnt = row["cnt"]
        # except Exception as e:
        #   # self.logff.write("Exception如下：%s"%e)
        #   # self.logff.write("  Failed:查询全部记录数的SQL运行失败，请检查验证规则文件后重试！\n")

        #运行验数主键重复记录数的sql
        # try:
        #   for row in session.execute(fkCheckSQL):
        #     # self.logff.write("  未通过验证的记录数：%s\n"%row["failedCnt"])
        #     failedRowCnt = row["failedCnt"]
        # except Exception as e:
        #   # self.logff.write("Exception如下：%s"%e)
        #   # self.logff.write("  Failed:查询未通过外键完整性验证的记录数的SQL运行失败，请检查验证规则文件后重试！\n")
        #   isOk = 'Failed'
        #   sqlCheckResult = 'N'

        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append('')
          insertData.append(colList2)
          insertData.append('FK')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt)
          insertData.append(fkSampleCheckSQL)
          insertData.append(description)
          # self.logff.write(insertData)

          # session.execute(fkCheckResultSQL,insertData)
          # self.logff.write("  插入结果数据成功！\n")

          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList2,'FK',''])
          IS = "'" + IS + "'"
          fkCheckResultSQL = fkCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",fkSampleCheckSQL.replace("'","''")).replace('CHECKSQL',fkCheckSQL).replace('SELCNTSQL',fkSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%fkCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL:---------\n")
          self.logff.write(";%s"%fkCheckDetailSQL + ";\n")


        except Exception as e:
          print("E!")
          # self.logff.write("Exception如下：%s"%e)
          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        # if 0 != failedRowCnt :
        #   try:
        #     session.execute(fkCheckDetailSQL)
        #     # self.logff.write("  插入样例数据成功！\n")
        #   except Exception as e:
        #     # self.logff.write("Exception如下：%s"%e)
        #     print(" Failed:将样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")

        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
        
      else:
        print(" 该条规则不用运行！" + "\n")

      self.logff.write("ENDOFINPUT\n")
      # self.logff.write(self.logDis)

    # self.logff.write("共有 %s 条外键完整性检查规则！\n\n"%self.fkRuleCnt)


  ####
  #执行父子关系检查
  #检查key的前两位，如果为FC那就是fcCheck
  ####
  def fatherChildCheck(self,session,ruleDict):
    print(" 总共有%s条规则！"%len(ruleDict))
    for k,rDict in ruleDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = rDict['runInd']
      #库名
      dbNm = rDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = rDict['tabNm'] 
      
      fatherDbNm = rDict['fatherDbNm']
      if 'pdata' == fatherDbNm.lower():
        fatherDbNm = self.pdata
      elif 'sdata' == fatherDbNm.lower():
        fatherDbNm = self.sdata
      fatherTabNm = rDict['fatherTabNm']
      fatherColList = rDict['fatherCollist']
      #去掉字段别名后的字段序列
      fatherColList2 = fatherColList.lower()
      rf = re.findall(self.pattern,fatherColList2)
      if [] != rf:
        for f in rf:
          fatherColList2 = fatherColList2.replace(f,'')

      fatherWhereClause = rDict['fatherWhereClause']
      #主键字段序列
      colList = rDict['colList']
      #去掉字段别名后的字段序列
      colList2 = colList.lower()
      rf = re.findall(self.pattern,colList2)
      if [] != rf:
        for f in rf:
          colList2 = colList2.replace(f,'')     


      # self.logff.write("记录%s：\n   子表：%s 库 %s 表 %s 字段[父子关系验证]\n  父表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))
      print("记录%s：\n  子表：%s 库 %s 表 %s 字段[父子关系验证]\n  父表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))

      #where条件
      whereClause = rDict['whereClause']
      #维护人
      operator = rDict['operator']
      #维护信息
      operatInfo = rDict['operatInfo']
      #备注
      description = rDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "fc" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0
      failedRowCnt2 = 0 

      #主键重复规则数目计数！
      self.fcRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)          
          whereClause = '  and ' + whereClause

        if('' != fatherWhereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          fatherWhereClause = fatherWhereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            fatherWhereClause = fatherWhereClause.replace(dbKey,dbValue)          
          fatherWhereClause = ' and ' + fatherWhereClause

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          print(" Warning:子表检查字段数超过10，无法完整保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+1):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          print(" Failed:子表字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        fatherInsertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        fatherColList = fatherColList.replace("，",",").replace("，",",").replace("，",",")
        fatherCols = fatherColList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        fatherRunDetailInd = True
        if 10 < len(fatherCols):
          fatherRunDetailInd = False
          print(" Warning:父表检查字段数超过10，无法完整保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(fatherCols):
          for i in range(1,len(fatherCols)+1):
            fatherInsertCols = fatherInsertCols + 'checkColumn' + str(i) + ','

          fatherInsertCols = fatherInsertCols[0:len(fatherInsertCols)-1]
          # self.logff.write(fatherInsertCols + "\n")
        else:
          print(" Failed:父表字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'",""),'FC'))
        insertClause = "'" + insertClause + "','C'"


        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        fatherInsertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        fatherInsertClause = "','".join((fatherInsertClause,fatherDbNm,fatherTabNm,fatherColList.replace("'",""),'FC'))
        fatherInsertClause = "'" + fatherInsertClause + "','F'"

        # self.logff.write(self.pdata,dbNm +"\n")

        #将模板SQL里面的库名、表名、字段名等内容替换掉
        fcSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE","where 1=1  "+whereClause).replace('${BATCHID}',self.batchId)
        fcCheckSQL1 = self.fkCheckSQL.replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("FATHERWHERECLAUSE","where 1 = 1 " + fatherWhereClause).replace("FATHERCOLLIST",fatherColList2).replace("CHILDCOLLIST",colList).replace('${BATCHID}',self.batchId)
        fcCheckSQL2 = self.fcCheckSQL.replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE","where 1 = 1 " + whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("FATHERWHERECLAUSE",fatherWhereClause).replace("FATHERCOLLIST",fatherColList2).replace('${BATCHID}',self.batchId)
        fcCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        fcCheckDetailSQL1 = self.fkCheckDetailSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("CHILDCOLS",colList2).replace("CHILDCOLLIST",colList).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("FATHERWHERECLAUSE","where 1 = 1 " +fatherWhereClause).replace('${BATCHID}',self.batchId)
        fcCheckDetailSQL2 = self.fcCheckDetailSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("FATHERINSERTCOLS",fatherInsertCols).replace("FATHERINSERTCLAUSE",fatherInsertClause).replace("CHILDCOLS",colList2).replace("CHILDCOLLIST",colList).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE","where 1 = 1 " +whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("FATHERWHERECLAUSE",fatherWhereClause).replace("FATHERCOLLIST",fatherColList2).replace('${BATCHID}',self.batchId)
        fcCSampleCheckSQL = self.fcSampleCheckSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("FATHERINSERTCOLS",fatherInsertCols).replace("FATHERINSERTCLAUSE",fatherInsertClause).replace("CHILDCOLS",colList2).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE2",whereClause).replace("CHILDWHERECLAUSE","where 1 = 1 " +whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("FATHERWHERECLAUSE",fatherWhereClause).replace("CHILDCOLLIST",colList).replace("FATHERCOLLIST",fatherColList2).replace('${BATCHID}',self.batchId)
        
      
        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt + failedRowCnt2:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append('')
          insertData.append(colList)
          insertData.append('FC')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt+failedRowCnt2)
          insertData.append(fcCSampleCheckSQL)
          insertData.append(description)
          # self.logff.write(insertData)

          # session.execute(fcCheckResultSQL,insertData)
          # self.logff.write("  插入结果数据成功！\n")


          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList2,'FC',''])
          IS = "'" + IS + "'"
          fcCheckResultSQL = fcCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",fcCSampleCheckSQL.replace("'","''")).replace('CHECKSQL',"SELECT CAST(SUM(failedCnt) AS FLOAT) AS failedCnt FROM ( " + fcCheckSQL1 + " union " + fcCheckSQL2 + ")b").replace('SELCNTSQL',fcSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%fcCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL1:---------\n")
          self.logff.write(";%s"%fcCheckDetailSQL1 + ";\n")

          self.logff.write("-----样例数据入库SQL2:---------\n")
          self.logff.write("%s"%fcCheckDetailSQL2 + ";\n")


        except Exception as e:
          print("Exception如下：%s"%e)
          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        # if 0 != failedRowCnt :
        #   try:
        #     # session.execute(fcCheckDetailSQL1)
        #     self.logff.write("  插入样例数据成功！\n")
        #   except Exception as e:
        #     self.logff.write("Exception如下：%s"%e)
        #     self.logff.write("  Failed:将样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")
        #运行插入结果样例的SQL-父表
        # if 0 != failedRowCnt2 :
        #   try:
        #     session.execute(fcCheckDetailSQL2)
        #     self.logff.write("  插入样例数据成功！\n")
        #   except Exception as e:
        #     self.logff.write("Exception如下：%s"%e)
        #     self.logff.write("  Failed:将父表样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")

        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))

      # else:
      #   self.logff.write("  该条规则不用运行！" + "\n")
      # self.logff.write(self.logDis)
      self.logff.write("ENDOFINPUT\n")

    print("共有 %s 条父子关系检查规则！\n\n"%self.fcRuleCnt)


  ####
  #执行PDM代码值域检查
  #检查key的前两位，如果为PC那就是pdmCdeCheck
  #pdm代码验证的依据是哪个呢？T99_STD_CDE_MAP_INFO？还是PDM对应的代码表的代码字段取值范围？
  ####
  def pdmCdeCheck(self,session,ruleDict):
    print(" 总共有%s条规则！"%len(ruleDict))
    for k,rDict in ruleDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = rDict['runInd']
      #库名
      dbNm = rDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      elif '' == dbNm:
        dbNm = self.pdata
      #表名
      tabNm = rDict['tabNm'] 
      
      #是否非空
      isEmpty = rDict['isEmpty']

      fatherDbNm = 'pdata'
      if 'pdata' == fatherDbNm.lower():
        fatherDbNm = self.pdata
      elif 'sdata' == fatherDbNm.lower():
        fatherDbNm = self.sdata
      fatherTabNm = rDict['pdmCde']
      fatherColList = rDict['pdmCdeCol']
      #去掉字段别名后的字段序列
      fatherColList2 = fatherColList.lower()
      rf = re.findall(self.pattern,fatherColList2)
      if [] != rf:
        for f in rf:
          fatherColList2 = fatherColList2.replace(f,'')

      fatherWhereClause = ''
      #主键字段序列
      colList = rDict['colList']
      #去掉字段别名后的字段序列
      colList2 = colList.lower()
      rf = re.findall(self.pattern,colList2)
      if [] != rf:
        for f in rf:
          colList2 = colList2.replace(f,'')

      # self.logff.write("记录%s：\n   数据表：%s 库 %s 表 %s 字段[PDM代码值域]\n  代码表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))
      print("记录%s：\n  数据表：%s 库 %s 表 %s 字段[PDM代码值域]\n  代码表：%s 库 %s 表 %s 字段\n"%(k,dbNm,tabNm,colList,fatherDbNm,fatherTabNm,fatherColList))

      #where条件
      whereClause = rDict['whereClause']
      #维护人
      operator = rDict['operator']
      #维护信息
      operatInfo = rDict['operatInfo']
      #备注
      description = rDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "pc" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.fkRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)          
          whereClause = '  and ' + whereClause

        if('' != fatherWhereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          fatherWhereClause = fatherWhereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            fatherWhereClause = fatherWhereClause.replace(dbKey,dbValue)          
          fatherWhereClause = 'where 1 = 1  and ' + fatherWhereClause

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")


        if 'Y' == isEmpty.strip():
          for col in cols:
            whereClause =  whereClause + " and  " + col + "  <> '' "

        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          print(" Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+1):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          print(" Failed:字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'",""),'PC'))
        insertClause = "'" + insertClause + "',''"

        # self.logff.write(self.pdata,dbNm +"\n")

        #将模板SQL里面的库名、表名、字段名等内容替换掉
        fkSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE","where 1=1  "+whereClause).replace("CHILDCOLS",colList2).replace('${BATCHID}',self.batchId)
        fkCheckSQL = self.fkCheckSQL.replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("FATHERWHERECLAUSE",fatherWhereClause).replace("CHILDCOLLIST",colList).replace("CHILDCOLS",colList2).replace('${BATCHID}',self.batchId)
        fkCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        fkCheckDetailSQL = self.fkCheckDetailSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("CHILDCOLLIST",colList).replace("FATHERWHERECLAUSE",fatherWhereClause).replace("CHILDCOLS",colList2).replace('${BATCHID}',self.batchId)  + "0"
        fkSampleCheckSQL = self.fkSampleCheckSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("CHILDCOLS",colList2).replace("CHILDCOLLIST",colList).replace("CHILDDATABASENAME",dbNm).replace("CHILDTABLENAME",tabNm).replace("CHILDWHERECLAUSE",whereClause).replace("FATHERDATABASENAME",fatherDbNm).replace("CHILDCOLLIST",colList).replace("FATHERTABLENAME",fatherTabNm).replace("FATHERCOLS",fatherColList2).replace("FATHERWHERECLAUSE",fatherWhereClause).replace("CHILDCOLS",colList2).replace('${BATCHID}',self.batchId)  + "0"


        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append('')
          insertData.append(colList)
          insertData.append('PC')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt)
          insertData.append(fkSampleCheckSQL)
          insertData.append(description)
          # self.logff.write(insertData)


          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList,'PC',''])
          IS = "'" + IS + "'"
          fkCheckResultSQL = fkCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",fkSampleCheckSQL.replace("'","''")).replace('CHECKSQL',fkCheckSQL).replace('SELCNTSQL',fkSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%fkCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL:---------\n")
          self.logff.write(";%s"%fkCheckDetailSQL + ";\n")


          # session.execute(fkCheckResultSQL,insertData)
          # self.logff.write("  插入结果数据成功！\n")

        except Exception as e:
          print("Exception如下：%s"%e)
          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        # if 0 != failedRowCnt :
        #   try:
        #     # session.execute(fkCheckDetailSQL)
        #     self.logff.write("  插入样例数据成功！\n")
        #   except Exception as e:
        #     self.logff.write("Exception如下：%s"%e)
        #     self.logff.write("  Failed:将样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")

        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
        
      else:
        print(" 该条规则不用运行！" + "\n")

      self.logff.write("ENDOFINPUT\n")
      # self.logff.write(self.logDis)

    # self.logff.write("共有 %s 条PDM代码值域检查规则！\n\n"%self.fkRuleCnt)

  ####
  #执行SDATA代码值域检查
  #检查key的前两位，如果为SC那就是sdataCdeCheck
  #确定可以通过源系统提供的代码表Excel验证吗？如果源系统的代码表Excel样式发生了改变呢？又如何？
  #还是说也如外键检查一样，或者如值域检查一样？
  ####
  def sdataCdeCheck(self,session,ruleDict):
    print(" 总共有%s条规则！"%len(ruleDict))
    #sdata代码表的内容
    if len(ruleDict) > 0:
      scd = self.readSdataCode3()
    else:
      return
    for k,pDict in ruleDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      # self.logff.write("第%s条记录：\n"%k)
      #运行标志
      runInd = pDict['runInd']
      #库名
      dbNm = pDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = pDict['tabNm'].upper() 

      #代码字段序列
      colList = pDict['colList'].upper()

      # self.logff.write("记录%s：%s 库 %s 表 %s 字段[SA代码]\n"%(k,dbNm,tabNm,colList))
      print("记录%s：%s 库 %s 表 %s 字段[SA代码]\n"%(k,dbNm,tabNm,colList))
      #代码值是否可空
      isEmpty = pDict['isEmpty']
      #where条件
      whereClause = pDict['whereClause']
      #维护人
      operator = pDict['operator']
      #维护信息
      operatInfo = pDict['operatInfo']
      #备注
      description = pDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "sc" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      valScale = set()
      #如果表名和字段名在sdata代码表excel内，执行检查，否则continue
      tabCol = ".".join(map(sstrip,[tabNm,colList]))
      if tabCol in scd:
        # self.logff.write(tabCol)
        valScale = scd[tabCol]
      else:
        print(" Failed:%s表的%s字段不在sdata代码表内，请检查后重试！\n"%(tabNm,colList))
        
      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.scRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)          
          whereClause = '  and ' + whereClause

        
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          print(" Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'


        #INSERTVALS，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertVals = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertVals = "','".join((insertVals,dbNm,tabNm,colList.replace("'","")))
        insertVals = "'" + insertVals + "'"

        #对值域范围的包装:
        valList = ""
        valScale = map(sreplace,valScale)
        valList = "','".join(map(str,valScale))
        valList = "'" + valList + "'"

        # self.logff.write(self.pdata,dbNm +"\n")

        if "Y" == isEmpty:
          vcSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE"," where 1 = 1 "+whereClause).replace('${BATCHID}',self.batchId)
          vcCheckSQL = self.codeCheckSQL.replace("WHERECLAUSE",whereClause).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
          vcCheckDetailSQL = self.codeDetailSQL.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcSampleCheckSQL = self.codeSampleCheckSQL.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
        else:
          vcSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE",whereClause).replace('${BATCHID}',self.batchId)
          vcCheckSQL = self.codeCheckSQLNotEmpty.replace("WHERECLAUSE",whereClause).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
          vcCheckDetailSQL = self.codeDetailSQLNotEmpty.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcSampleCheckSQL = self.codeSampleCheckSQLNotEmpty.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)



        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append("")
          insertData.append(colList)
          insertData.append('SC')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt)
          insertData.append(vcSampleCheckSQL)
          insertData.append(description)
          # self.logff.write(insertData)

          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList,'SC',''])
          IS = "'" + IS + "'"
          vcCheckResultSQL = vcCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",vcSampleCheckSQL.replace("'","''")).replace('CHECKSQL',vcCheckSQL).replace('SELCNTSQL',vcSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%vcCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL:---------\n")
          self.logff.write(";%s"%vcCheckDetailSQL + ";\n")



          # session.execute(vcCheckResultSQL,insertData)
          # self.logff.write("  插入结果数据成功！\n")

        except Exception as e:
          print("Exception如下：%s"%e)
          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        # if 0 != failedRowCnt :
        #   try:
        #     session.execute(vcCheckDetailSQL)
        #     self.logff.write("  插入样例数据成功！\n")
        #   except Exception as e:
        #     self.logff.write("Exception如下：%s"%e)
        #     self.logff.write("  Failed:将样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")
        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
      else:
        print(" 该条规则不用运行！" + "\n")
      self.logff.write("ENDOFINPUT\n")
    #   self.logff.write(self.logDis)

    # self.logff.write("共有 %s 条SA代码值域检查规则！\n\n"%self.scRuleCnt)


  ####
  #执行值域检查
  #检查key的前两位，如果为VC那就是valCheck
  #先不对码值的类型是数字型还是字符型做判断了，直接都在外面加上单引号！
  ####
  def valCheck(self,session,ruleDict):
    print(" 总共有%s条规则！"%len(ruleDict))
    for k,pDict in ruleDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = pDict['runInd']
      #库名
      dbNm = pDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = pDict['tabNm'] 

      #代码字段序列
      colList = pDict['colList']
      #值域类型：数字、字符、日期类型
      valType = pDict['valType']
      #代码值是否可空
      isEmpty = pDict['isEmpty']

      # self.logff.write("记录%s：%s 库 %s 表 %s 字段[值域-手工设置]\n"%(k,dbNm,tabNm,colList))
      print("记录%s：%s 库 %s 表 %s 字段[值域-手工设置]\n"%(k,dbNm,tabNm,colList))

      valScale = pDict['valScale']
      #where条件
      whereClause = pDict['whereClause']
      #维护人
      operator = pDict['operator']
      #维护信息
      operatInfo = pDict['operatInfo']
      #备注
      description = pDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "vc" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.vcRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)          
          whereClause = '  and ' + whereClause

        
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          print(" Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'


        #INSERTVALS，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertVals = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertVals = "','".join((insertVals,dbNm,tabNm,colList.replace("'","")))
        insertVals = "'" + insertVals + "'"

        #对值域范围的包装:
        valList = ""

        if(float == type(valScale) ):
          if(valScale == int(valScale)):
            valScale = str(int(valScale))

        valScale = valScale.replace("'","").replace("“","").replace("”","").replace("，",",")
        vals = valScale.split(",")
        valList = "','".join(map(str,vals))
        valList = "'" + valList + "'"

        # self.logff.write(self.pdata,dbNm +"\n")

        if "Y" == isEmpty:
          vcSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE"," where 1=1 " + whereClause).replace('${BATCHID}',self.batchId)
          vcCheckSQL = self.codeCheckSQL.replace("WHERECLAUSE",whereClause).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
          vcCheckDetailSQL = self.codeDetailSQL.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcSampleCheckSQL = self.codeSampleCheckSQL.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
        else:
          vcSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE"," where 1=1 " + whereClause).replace('${BATCHID}',self.batchId)
          vcCheckSQL = self.codeCheckSQLNotEmpty.replace("WHERECLAUSE",whereClause).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
          vcCheckDetailSQL = self.codeDetailSQLNotEmpty.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)
          vcSampleCheckSQL = self.codeSampleCheckSQLNotEmpty.replace("WHERECLAUSE",whereClause).replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTVALS",insertVals).replace("CODECOL",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("VALUELIST",valList).replace('${BATCHID}',self.batchId)

        
        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append("")
          insertData.append(colList)
          insertData.append('VC')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt)
          insertData.append(vcSampleCheckSQL)
          insertData.append(description)
          # self.logff.write(insertData)


          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList,'VC',''])
          IS = "'" + IS + "'"
          vcCheckResultSQL = vcCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",vcSampleCheckSQL.replace("'","''")).replace('CHECKSQL',vcCheckSQL).replace('SELCNTSQL',vcSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%vcCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL:---------\n")
          self.logff.write(";%s"%vcCheckDetailSQL + ";\n")



          # session.execute(vcCheckResultSQL,insertData)
          # self.logff.write("  插入结果数据成功！\n")

        except Exception as e:
          print("Exception如下：%s"%e)
          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        
        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
      else:
        print(" 该条规则不用运行！" + "\n")
      self.logff.write("ENDOFINPUT\n")
      # self.logff.write(self.logDis)

    # self.logff.write("共有 %s 条值域（手工设置）检查规则！\n\n"%self.vcRuleCnt)

  ###
  #历史拉链表的交叉链检查
  ###
  def zipCheck(self,session,ruleDict):
    print(" 总共有%s条规则！"%len(ruleDict))
    for k,rDict in ruleDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = rDict['runInd']
      #库名
      dbNm = rDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = rDict['tabNm'] 
      
      stDt = rDict['stDt']
      endDt = rDict['endDt']

      #主键字段序列
      colList = rDict['colList']

      #self.logff.write("记录%s：%s 库 %s 表 %s 字段[交叉链]\n"%(k,dbNm,tabNm,colList))
      print("记录%s：%s 库 %s 表 %s 字段[交叉链]\n"%(k,dbNm,tabNm,colList))

      #where条件
      whereClause = rDict['whereClause']
      #维护人
      operator = rDict['operator']
      #维护信息
      operatInfo = rDict['operatInfo']
      #备注
      description = rDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "zc" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.zcRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
          whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
          for dbKey,dbValue in self.dbDict.items():
            whereClause = whereClause.replace(dbKey,dbValue)
          whereClause = 'where 1 = 1  and ' + whereClause


        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 9 < len(cols):
          runDetailInd = False
          print(" Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+2):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          print(" Failed:字段数量为0个，请检查规则配置文件！" + "\n")
          print(" 字段数量为0个！请检查规则配置文件后重试！")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'","")+","+stDt,'ZC'))
        insertClause = "'" + insertClause + "',''"

        #插入sample样例的语句中子select语句的SELECTCLAUSE，其实就是在字段名前面加上A
        selectClause = ""
        for i in range(0,len(cols)):
          selectClause = selectClause + ",A." + str(cols[i]).strip()
        selectClause = selectClause[1:len(selectClause)] + ",A." + stDt

        #A和B表的join语句：如 A.AGMT_ID = B.AGMT_ID AND A.AGMT_MODIFIER_NO = B.AGMT_MODIFIER_NO AND A.PARTY_ID = B.PARTY_ID AND A.AGMT_PARTY_RELA_TYPE_CD = B.AGMT_PARTY_RELA_TYPE_CD
        joinClause = ''
        for i in range(0,len(cols)):
          joinClause = joinClause + " and A." +str(cols[i]).strip() + " = B."  +str(cols[i]).strip() 
        joinClause = joinClause[4:len(joinClause)] 


        # self.logff.write(self.pdata,dbNm +"\n")

        #将模板SQL里面的库名、表名、字段名等内容替换掉
        zcSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE",whereClause).replace('${BATCHID}',self.batchId)
        zcCheckSQL = self.zipCheckSQL.replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE",whereClause).replace("STARTDT",stDt).replace("ENDDT",endDt).replace("JOINCLAUSE",joinClause).replace('${BATCHID}',self.batchId)
        zcCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        zcCheckDetailSQL = self.zipCheckDetailSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("SELECTCOLS",selectClause).replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE",whereClause).replace("STARTDT",stDt).replace("ENDDT",endDt).replace("JOINCLAUSE",joinClause).replace('${BATCHID}',self.batchId)
        zcSampleCheckSQL = self.zipSampleCheckSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("SELECTCOLS",selectClause).replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("WHERECLAUSE",whereClause).replace("STARTDT",stDt).replace("ENDDT",endDt).replace("JOINCLAUSE",joinClause).replace('${BATCHID}',self.batchId)
        
        # self.logff.write("查询全部记录数：%s"%zcSelCnt + "\n")
        #self.logff.write(" 查询SQL：%s"%zcCheckSQL + "\n")
        # self.logff.write("结果插入到库表中：%s"%zcCheckResultSQL + "\n")
        #self.logff.write(" 结果样例插入到库表中:%s"%zcCheckDetailSQL + "\n")

        # #运行检查表全部记录数的SQL
        # try:
        #   for row in session.execute(zcSelCnt):
        #     self.logff.write("  全部记录数：%s\n"%row["cnt"])
        #     allRowCnt = row["cnt"]
        # except Exception as e:
        #   self.logff.write("Exception如下：%s"%e)
        #   self.logff.write("  Failed:查询全部记录数的SQL运行失败，请检查验证规则文件后重试！\n")

        # #运行验数主键重复记录数的sql
        # try:
        #   for row in session.execute(zcCheckSQL):
        #     self.logff.write("  未通过验证的记录数：%s\n"%row["failedCnt"])
        #     failedRowCnt = row["failedCnt"]
        # except Exception as e:
        #   self.logff.write("Exception如下：%s"%e)
        #   self.logff.write("  Failed:查询未通过zipCheck完整性验证的记录数的SQL运行失败，请检查验证规则文件后重试！\n")
        #   isOk = 'Failed'
        #   sqlCheckResult = 'N'

        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if 0 != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append('')
          insertData.append(colList)
          insertData.append('ZC')
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append(failedRowCnt)
          insertData.append(zcSampleCheckSQL)
          insertData.append(description)
          # self.logff.write(insertData)


          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList,'ZC',''])
          IS = "'" + IS + "'"
          zcCheckResultSQL = zcCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",zcSampleCheckSQL.replace("'","''")).replace('CHECKSQL',zcCheckSQL).replace('SELCNTSQL',zcSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%zcCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL12345:---------\n")
          self.logff.write(";%s"%zcCheckDetailSQL + ";\n")


          #session.execute(zcCheckResultSQL,insertData)
          #self.logff.write(" 插入结果数据成功！\n")

        except Exception as e:
          print("Exception如下：%s"%e)
          print(" Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        
        print(" SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
      else:
        print(" 该条规则不用运行！" + "\n")
      #self.logff.write(self.logDis)
      self.logff.write("ENDOFINPUT\n")


    #self.logff.write("共有 %s 条交叉链检查规则！\n\n"%self.zcRuleCnt)


  ####
  #执行自定义检查
  #注意对于检查结果记录sample50
  #然后读取传递过来的Dict，跑SQL，记日志，记录结果
  ####
  def ugcSQLCheck(self,session,ucDict):
    print(" 总共有%s条规则！"%len(ucDict))
    for k,pDict in ucDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = pDict['runInd']
      #库名
      dbNm = pDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = pDict['tabNm'] 
      #规则内子编号，如果为空，则默认为0-主键
      ugcType = pDict['ugcType']
      #主键字段序列
      colList = pDict['colList']
      # self.logff.write("记录%s：%s 库 %s 表 %s 字段 [%s]\n"%(k,dbNm,tabNm,colList,ugcType))
      print("记录%s：%s 库 %s 表 %s 字段 [%s]\n"%(k,dbNm,tabNm,colList,ugcType))

      #where条件
      sql = pDict['sql']

      #将自定义SQL中可能存在的数据库变量（如${SDATA_AFT}等）替换成对应的库名(如SDATA_AFT)
      for dbKey,dbValue in self.dbDict.items():
        sql = sql.replace(dbKey,dbValue)

      #维护人
      operator = pDict['operator']
      #维护信息
      operatInfo = pDict['operatInfo']
      #备注
      description = pDict['description']

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "uc" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      # failedRowCnt = 0

      #主键重复规则数目计数！
      self.ucRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        # if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
        #   whereClause = whereClause.lower().replace("where",'').upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
        #   whereClause = ' where 1=1 and ' + whereClause

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          # self.logff.write("  Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+1):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          # self.logff.write("  Failed:字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'",""),'UC-'+ugcType))
        insertClause = "'" + insertClause + "',''"

        # self.logff.write(self.pdata,dbNm +"\n")

        ucSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace('${BATCHID}',self.batchId)
        ucCheckSQL = sql.replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata).replace('${BATCHID}',self.batchId)
        ucCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        ucCheckDetailSQL = self.ucDetailSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("SQL",ucCheckSQL).replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata).replace('${BATCHID}',self.batchId)
        ucSampleCheckSQL = ucCheckSQL
        
        

        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          # if 0 != failedRowCnt:
          sqlCheckResult =  ''
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append('')
          insertData.append(colList)
          insertData.append('UC-'+ugcType)
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append('')
          insertData.append(ucSampleCheckSQL)
          insertData.append(description)

          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList,'UC-' + ugcType,''])
          IS = "'" + IS + "'"
          ucCheckResultSQL = ucCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",ucSampleCheckSQL.replace("'","''")).replace('CHECKSQL',"  SELECT  -1 AS failedCnt ").replace('SELCNTSQL',ucSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%ucCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL123:---------\n")
          self.logff.write(";%s"%ucCheckDetailSQL + ";\n")


          # self.logff.write(insertData)

          # session.execute(ucCheckResultSQL,insertData)
          # self.logff.write("  插入结果数据成功！\n")

        except Exception as e:
          print("Exception如下：%s"%e)
          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")

        #运行插入结果样例的SQL
        # try:
        #   session.execute(ucCheckDetailSQL)
        #   self.logff.write("  插入样例数据成功！\n")
        # except Exception as e:
        #   self.logff.write("Exception如下：%s"%e)
        #   self.logff.write("  Failed:将样例数据入库的SQL运行失败，请检查验证规则文件后重试！\n")

        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
      else:
        print(" 该条规则不用运行！" + "\n")
      self.logff.write("ENDOFINPUT\n")
    #   self.logff.write(self.logDis)

    # self.logff.write("共有 %s 条自定义SQL检查规则！\n\n"%self.ucRuleCnt)


  ####
  #执行自定义规则检查
  #注意对于检查结果记录sample50
  #然后读取传递过来的Dict，跑SQL，记日志，记录结果
  ####
  def uuSQLCheck(self,session,uuDict):
    print(" 总共有%s条规则！"%len(uuDict))
    for k,pDict in uuDict.items():
      self.logff.write("print BTEQ <<ENDOFINPUT;\n")
      #运行标志
      runInd = pDict['runInd']
      #库名
      dbNm = pDict['dbNm'] 
      if 'pdata' == dbNm.lower():
        dbNm = self.pdata
      elif 'sdata' == dbNm.lower():
        dbNm = self.sdata
      #表名
      tabNm = pDict['tabNm'] 
      #规则内子编号，如果为空，则默认为0-主键
      ugcType = pDict['ugcType']
      #主键字段序列
      colList = pDict['colList']
      # self.logff.write("记录%s：%s 库 %s 表 %s 字段 [%s]\n"%(k,dbNm,tabNm,colList,ugcType))
      print("记录%s：%s 库 %s 表 %s 字段 [%s]\n"%(k,dbNm,tabNm,colList,ugcType))

      #where条件
      sql = pDict['sql']

      #将自定义SQL中可能存在的数据库变量（如${SDATA_AFT}等）替换成对应的库名(如SDATA_AFT)
      for dbKey,dbValue in self.dbDict.items():
        sql = sql.replace(dbKey,dbValue)


      #维护人
      operator = pDict['operator']
      #维护信息
      operatInfo = pDict['operatInfo']
      #备注
      description = pDict['description']

      #目标记录数(默认为0)
      correctNum = pDict['correctNum']
      if None == correctNum or '' == correctNum:
        correctNum = 0

      #SQL是否运行正常
      isOk = 'Success'

      #规则验证是否通过
      sqlCheckResult = self.checkFlag

      checkId = "uu" + str(k)
      # self.logff.write("%s-%s-%s-%s-%s-%s-%s-%s-%s"%(runInd,dbNm,tabNm,tabRuleId,colList,whereClause,operator,operatInfo,description))

      allRowCnt = 0
      failedRowCnt = 0

      #主键重复规则数目计数！
      self.uuRuleCnt += 1

      startTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      #总共需要拼接并跑4条SQL，两个Select语句，两个Insert语句
      #先判断运行标志，如果不运行的话，那就只写入日志，但是不跑SQL
      if 'Y' == runInd:
        #where条件的拼接:填写的时候不能带where关键字，当然，如果带了，我们去掉就是了！
        #如果where条件为空，则whereClause为空
        #replace默认是区分大小写的，这里我们最好改成不区分大小写
        # if('' != whereClause.replace("  ","").replace(" ","").replace("　","").strip()):
        #   whereClause = whereClause.lower().upper().replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata)
        #   whereClause = ' where 1=1 and ' + whereClause

        #插入详细异常记录的语句拼接：INSERTCOLS（根据主键字段数，确定checkColumn1到10的序列）--注意判断主键字段数要少于10个
        insertCols = ''
        #先将Collist中的可能存在的中文逗号转换成英文的逗号,然后按照逗号分隔开
        colList = colList.replace("，",",").replace("，",",").replace("，",",").lower().replace("coalesce(",'').replace(",'')",'')
        cols = colList.split(",")
        #判断字段序列的长度是否大于10，如果大于10，则checkDetail相关的SQL不运行了，并self.logff.write出来
        runDetailInd = True
        if 10 < len(cols):
          runDetailInd = False
          print(" Warning:检查字段数超过10，无法保存Sample样例数据，请注意！" + "\n")
        #否则就设计insertCols。如果colList中有6个字段，那么insertCols就是：'checkColumn1,checkColumn2,...,checkColumn6'
        if 1<= len(cols):
          for i in range(1,len(cols)+1):
            insertCols = insertCols + 'checkColumn' + str(i) + ','

          insertCols = insertCols[0:len(insertCols)-1]
          # self.logff.write(insertCols + "\n")
        else:
          self.logff.write("  Failed:字段数量为0个，请检查规则配置文件！" + "\n")
          os._exit(0)

        #INSERTCLAUSE，根据程序运行结果拼接的内容，如运行日期、任务编号、维护信息等
        insertClause = checkId + "'," + str("cast('" + self.txnDate +"' as date format 'yyyy-mm-dd')" ) + ",'" +  str(self.caseNo)
        insertClause = "','".join((insertClause,dbNm,tabNm,colList.replace("'",""),'UU-'+ugcType))
        insertClause = "'" + insertClause + "',''"

        # self.logff.write(self.pdata,dbNm +"\n")

        uuSelCnt = self.selCnt.replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace('${BATCHID}',self.batchId)
        uuCheckSQL = sql.replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata).replace('${BATCHID}',self.batchId)
        uuCheckResultSQL = self.checkResultSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace('${BATCHID}',self.batchId)
        uuCheckDetailSQL = self.ucDetailSQL.replace("RESULTDATABASENAME",self.checkResultDb).replace("INSERTCOLS",insertCols).replace("INSERTCLAUSE",insertClause).replace("COLLIST",colList).replace("DATABASENAME",dbNm).replace("TABLENAME",tabNm).replace("SQL",uuCheckSQL).replace("${TXNDATE}",self.TXNDATE).replace("${PDATA}",self.pdata).replace("${SDATA}",self.sdata).replace('${BATCHID}',self.batchId)
        uuSampleCheckSQL = uuCheckSQL
        
        # self.logff.write("查询全部记录数：%s"%pkSelCnt + "\n")
        # self.logff.write("  查询SQL：%s"%uuCheckSQL + "\n")
        # # self.logff.write("  结果插入到库表中：%s"%ucCheckResultSQL + "\n")
        # self.logff.write("  结果样例插入到库表中:%s"%uuCheckDetailSQL + "\n")

        #运行检查表全部记录数的SQL
        # try:
        #   for row in session.execute(uuSelCnt):
        #     self.logff.write("  全部记录数：%s\n"%row["cnt"])
        #     allRowCnt = row["cnt"]
        # except Exception as e:
        #   self.logff.write("Exception如下：%s"%e)
        #   self.logff.write("  Failed:查询全部记录数的SQL运行失败，请检查验证规则文件！\n")

        # #运行验数主键重复记录数的sql
        # try:
        uuCheckSQL = 'select count(*)  - ' + str(correctNum) + '  as failedCnt from (' + uuCheckSQL + ')z'
        #   # print(uuCheckSQL)
        #   # for row in session.execute( uuCheckSQL ):
        #   #   self.logff.write("  运行自定义规则成功!")
        #   #   # print(row["failedCnt"])
        #   #   failedRowCnt = row["failedCnt"]
        # except Exception as e:
        #   # self.logff.write("Exception如下：%s"%e)
        #   # self.logff.write("  Failed:运行自定义规则失败，请检查验证规则文件！\n")
        #   isOk = 'Failed'
        #   sqlCheckResult =  'N'

        endTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        #(checkId,runDate,batchId,checkStartTime,checkEndTime,DBNm,TabNm,tabRuleId,colNm,checkType,SQLResult,checkResultInd,AllRowCnt,FailedRowCnt,QuerySQL)
        #运行插入验数结果的SQL
        try:
          if correctNum != failedRowCnt:
            sqlCheckResult =  'N'
          insertData = []
          insertData.append(checkId)
          insertData.append(self.txnDate)
          insertData.append(self.caseNo)
          insertData.append(startTime)
          insertData.append(endTime)
          insertData.append(dbNm)
          insertData.append(tabNm)
          insertData.append('')
          insertData.append(colList)
          insertData.append('UU-'+ugcType)
          insertData.append(isOk)
          insertData.append(sqlCheckResult)
          insertData.append(allRowCnt)
          insertData.append('')
          insertData.append(uuSampleCheckSQL)
          insertData.append(description)

          # self.logff.write(insertData)

          IS = "','".join([checkId,self.txnDate,self.caseNo])

          IS = IS + "'"

          IS = ','.join([IS,"CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME","CAST(DATE AS FORMAT  'YYYY-MM-DD')||' '||TIME"])

          IS = IS + ",'" + dbNm

          IS = "','".join([IS,tabNm,'',colList,'UC-' + ugcType,''])
          IS = "'" + IS + "'"
          uuCheckResultSQL = uuCheckResultSQL.replace("INSERTVALUES",IS).replace("SAMPLECHECKSQL",uuSampleCheckSQL.replace("'","''")).replace('CHECKSQL',uuCheckSQL).replace('SELCNTSQL',uuSelCnt)

          self.logff.write("-----验证结果数据入库SQL:---------\n")
          self.logff.write("%s"%uuCheckResultSQL + "\n")

          self.logff.write("-----样例数据入库SQL:---------\n")
          self.logff.write(";%s"%uuCheckDetailSQL + ";\n")

        except Exception as e:
          print("Exception如下：%s"%e)
      self.logff.write("ENDOFINPUT\n")

          # self.logff.write("  Failed！插入验证结果失败，请检查验证规则文件后重试！\n")
        # self.logff.write("  SQL开始运行时间：%s ，SQL结束运行时间：%s\n"%(startTime,endTime))
      # else:
      #   print(" 该条规则不用运行！" + "\n")
      # self.logff.write(self.logDis)



  #根据配置项的sdata代码表地址，读取里面的SDATA代码信息。
  #"表级代码表"sheet的0、1、5列是我们需要的，拼成一个sdataCodeDict={“表名-字段名”：CodeSet}
  #这里根据高博整理的代码表、新模板来读取
  def readSdataCode(self):
    codeSheet = "2.2源代码值-全量"
    sdataCode = xlrd.open_workbook(filename = self.saFilePath)
    sdataCodeSheet = sdataCode.sheet_by_name(codeSheet)
    #存储代码信息的Dict
    #表名所在列index
    tabIndex = 1
    #列名所在列index
    colIndex = 1
    #代码值所在列index
    codeIndex = 2

    sdataCodeDict = {}
    for i in range(4,sdataCodeSheet.nrows):
      cInfo = sdataCodeSheet.row_values(i)
      val = set()
      if(float == type(cInfo[codeIndex]) ):
        if(cInfo[codeIndex] == int(cInfo[codeIndex])):
          val.add(str(int(cInfo[codeIndex])))
      elif "''" == cInfo[codeIndex] or "'" == cInfo[codeIndex]:
        val.add("")
      else:
        val.add( cInfo[codeIndex])
      #将表名和字段名拼接一下，作为dict的key，val为代码值的set
      tabCol = cInfo[tabIndex]
      # self.logff.write(tabCol)
      codeSet = set()
      if tabCol in sdataCodeDict:
        codeSet = sdataCodeDict[tabCol]
        sdataCodeDict[tabCol] = set.union(codeSet,val)
      else:
        codeSet = val
        sdataCodeDict[tabCol] = codeSet

    # self.logff.write(sdataCodeDict)
    return sdataCodeDict


  def readSdataCode2(self):
    codeSheet = "表记代码表"
    sdataCode = xlrd.open_workbook(filename = self.saFilePath)
    sdataCodeSheet = sdataCode.sheet_by_name(codeSheet)
    #存储代码信息的Dict
    #表名所在列index
    tabIndex = 0
    #列名所在列index
    colIndex = 1
    #代码值所在列index
    codeIndex = 2

    sdataCodeDict = {}
    for i in range(1,sdataCodeSheet.nrows):
      cInfo = sdataCodeSheet.row_values(i)
      val = set()
      if(float == type(cInfo[codeIndex]) ):
        if(cInfo[codeIndex] == int(cInfo[codeIndex])):
          val.add(str(int(cInfo[codeIndex])))
      elif "''" == cInfo[codeIndex] or "'" == cInfo[codeIndex]:
        val.add("")
      else:
        val.add( cInfo[codeIndex])
      #将表名和字段名拼接一下，作为dict的key，val为代码值的set
      tabCol = ".".join(map(sstrip,map(str,[cInfo[tabIndex],cInfo[colIndex]])))
      tabCol = "CBS_" + tabCol
      # self.logff.write(tabCol)
      codeSet = set()
      if tabCol in sdataCodeDict:
        codeSet = sdataCodeDict[tabCol]
        sdataCodeDict[tabCol] = set.union(codeSet,val)
      else:
        codeSet = val
        sdataCodeDict[tabCol] = codeSet

    # self.logff.write(sdataCodeDict)
    return sdataCodeDict

#读取代码表全量Excel中的sdata代码全量
  def readSdataCode3(self):
    codeSheet = "SDATA代码全量"
    sdataCode = xlrd.open_workbook(filename = self.saFilePath)
    sdataCodeSheet = sdataCode.sheet_by_name(codeSheet)
    #存储代码信息的Dict
    #表名所在列index
    tabIndex = 1
    #列名所在列index
    colIndex = 2
    #代码值所在列index
    codeIndex = 3

    sdataCodeDict = {}
    for i in range(1,sdataCodeSheet.nrows):
      cInfo = sdataCodeSheet.row_values(i)
      val = set()
      if(float == type(cInfo[codeIndex]) ):
        if(cInfo[codeIndex] == int(cInfo[codeIndex])):
          val.add(str(int(cInfo[codeIndex])))
      elif "''" == cInfo[codeIndex] or "'" == cInfo[codeIndex]:
        val.add("")
      else:
        val.add( cInfo[codeIndex])
      #将表名和字段名拼接一下，作为dict的key，val为代码值的set
      tabCol = ".".join(map(sstrip,map(str,[cInfo[tabIndex].upper(),cInfo[colIndex].upper()])))
      tabCol = tabCol
      # self.logff.write(tabCol)
      codeSet = set()
      if tabCol in sdataCodeDict:
        codeSet = sdataCodeDict[tabCol]
        sdataCodeDict[tabCol] = set.union(codeSet,val)
      else:
        codeSet = val
        sdataCodeDict[tabCol] = codeSet

    # self.logff.write(sdataCodeDict)
    return sdataCodeDict

#运行主程序
if __name__ == "__main__":
  #数据验证文件的路径
  checkExcelDir = ".\\测试案例文件\\"
  print(checkExcelDir)
  for parent,dirnames,filenames in os.walk(checkExcelDir):
    print(parent)
    for filename in filenames:
      print(filename)
      for batchId in bic.batchId[filename]:
        dqc = DataQualityCheck()
        dqc.dataCheck(parent,filename,batchId)