import pymysql
import pandas as pd

# MySQL 连接配置
config_jobList = {
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'host': '192.168.123.243',
    'database': 'jobList',
}

# 连接到 MySQL 数据库
conn_jobList = pymysql.connect(**config_jobList)
sql_query = """
SELECT CONCAT(jb.id, '\t')                                             AS id,
       jb.enterpriseName,
       jb.positionName,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE jb.willNature = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'work_nature')             AS willNature,
       CONCAT(jb.minimumWage, '-', jb.maximumWage)                     AS Salary,
       CASE
           WHEN jb.payMethod = 0 THEN '年薪'
           WHEN jb.payMethod = 1 THEN '月薪'
           WHEN jb.payMethod = 2 THEN '日薪'
           END                                                         AS payMethod,
       CASE
           WHEN jb.exp = '0' THEN '经验不限'
           WHEN jb.exp = '不限' THEN '经验不限'
           WHEN jb.exp = '1' THEN '1年以上'
           WHEN jb.exp = '3' THEN '3年以上'
           WHEN jb.exp = '5' THEN '5年以上'
           WHEN jb.exp = '10' THEN '10年以上'
           ELSE jb.exp
           END                                                         AS exp,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE jb.educationalRequirements = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'education_requirement')   AS edu_require,
       CASE
           WHEN jb.count = 0 THEN '不限人数'
           ELSE jb.count
           END                                                         AS position_count,
       REGEXP_REPLACE(jb.welfare, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '')        AS welfare,
       jb.workplace,
       REGEXP_REPLACE(jb.function, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '')   AS `function`,
       jb.jobRequiredments,
       (SELECT GROUP_CONCAT(CONCAT_WS('-', REGEXP_REPLACE(kL.labelName, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '')
                                ) SEPARATOR ',')
        FROM keywordlist kL
        WHERE jb.id = kL.jobPositionId)                                AS keyword_info,
       (SELECT GROUP_CONCAT(CONCAT_WS('-', sL.labelName) SEPARATOR ',')
        FROM skillslist sL
        WHERE jb.id = sL.jobPositionId)                                AS skill_info,
       eA.provinceCode,
       eA.cityCode,
       eA.regionCode,
       CONCAT(
               IFNULL((SELECT dicttype.dict_type.label
                       FROM dicttype.dict_type
                                JOIN enterpriseaddress eA ON jb.enterpriseId = eA.enterpriseId
                       WHERE eA.provinceCode = dicttype.dict_type.value
                         AND dicttype.dict_type.dictType = 'region_code'
                       LIMIT 1), ''),
               IFNULL((SELECT dicttype.dict_type.label
                       FROM dicttype.dict_type
                                JOIN enterpriseaddress eA ON jb.enterpriseId = eA.enterpriseId
                       WHERE eA.cityCode = dicttype.dict_type.value
                         AND dicttype.dict_type.dictType = 'region_code'
                       LIMIT 1), ''),
               IFNULL((SELECT dicttype.dict_type.label
                       FROM dicttype.dict_type
                                JOIN enterpriseaddress eA ON jb.enterpriseId = eA.enterpriseId
                       WHERE eA.regionCode = dicttype.dict_type.value
                         AND dicttype.dict_type.dictType = 'region_code'
                       LIMIT 1), ''),
               IFNULL(eA.detailedAddress, '')
           )                                                           AS enter_address,
       REGEXP_REPLACE(eA_infor.industry, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '') AS job_industry,
       eA_infor.shortName                                              AS eA_shortName,
       eA_infor.econKind                                               AS eA_econKind,
       eA_infor.personScope                                            AS job_personScope,
       eA_infor.recruitJobNum                                          AS job_recruitJobNum,
       eA_infor.registCapi                                             AS job_registCapi,
       eA_infor.introduction                                           AS job_introduction,
       eA_infor.totalPublicJobNum                                      AS job_totalPublicJobNum,
       eA_infor.email                                                  AS job_email,
       CONCAT(eA_infor.phone, '')                                      AS job_phone,
       jb.deadline
FROM job_infor jb
         LEFT JOIN enterpriseaddress eA ON jb.enterpriseId = eA.enterpriseId
         LEFT JOIN enterprise_infor eA_infor ON jb.enterpriseId = eA_infor.enterpriseId
ORDER BY jb.id ASC;
"""

# 使用 pandas 从 MySQL 数据库中读取数据
df = pd.read_sql(sql_query, conn_jobList)
l = range(1, len(df) + 1)

df.insert(0, 'index', l)

# 将数据保存到 XLSX 文件
xlsx_path = 'Q1_jobList.xlsx'

df.replace('None','',inplace=True)
# 删除停用词，这里的路径需要按照情况进行修改
special_stopwords_path = '原数据中的停用词.txt'
special_stopwords = open(special_stopwords_path,'r', encoding='utf-8')
stopwords_special = []
for i in special_stopwords.readlines():
    stopwords_special.append(i.strip())
for i in range(len(df.columns)):
    df[df.columns[i]] = df[df.columns[i]].map(lambda x:'' if x in stopwords_special else x)



df.to_excel(xlsx_path, index=False)

# 关闭数据库连接
conn_jobList.close()

col = ['序号', '招聘信息ID', '企业名称', '招聘岗位', '岗位性质', '预期薪资', '支付方式', '工作经验要求', '学历要求',
       '招聘人数', '岗位福利', '工作地点', '岗位职能', '岗位要求', '岗位关键词', '技能要求', '省代码', '市代码',
       '区代码', '企业地址', '企业所属行业', '企业简称',
       '企业性质', '企业规模', '在招职位', '注册资本', '企业简介', '全部职位', '邮箱', '电话', '截止日期']

df.columns = col

csv_path = 'Q1_jobList(提交版).csv'
df.to_csv(csv_path, index=False)

print('文件已经成功输出！')
