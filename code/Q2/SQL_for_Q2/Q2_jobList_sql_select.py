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
connection = pymysql.connect(**config_jobList)
sql_query = """
SELECT CONCAT(jb.id, '\t')                                             AS id,
       jb.enterpriseName,
       jb.positionName,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE jb.willNature = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'work_nature')             AS willNature,
       jb.minimumWage,
       jb.maximumWage,
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
           CASE
           WHEN exp = '0' OR exp = '经验不限' OR exp = '不限' THEN 0
           WHEN exp = '1-3年' THEN 1
           WHEN exp = '3-5年' THEN 3
           WHEN exp = '5-7年' THEN 5
           WHEN exp = '5-10年' THEN 5
           WHEN exp = '7年以上' THEN 7
           WHEN exp = '1' THEN 1
           WHEN exp = '3' THEN 3
           WHEN exp = '5' THEN 5
           WHEN exp = '10' THEN 10
           ELSE exp
           END                                                         AS exp_min,
       CASE
           WHEN exp = '0' OR exp = '经验不限' OR exp = '不限' THEN 100
           WHEN exp = '1-3年' THEN 3
           WHEN exp = '3-5年' THEN 5
           WHEN exp = '5-7年' THEN 7
           WHEN exp = '5-10年' THEN 10
           WHEN exp = '7年以上' THEN 100
           WHEN exp = '1' THEN 100
           WHEN exp = '3' THEN 100
           WHEN exp = '5' THEN 100
           WHEN exp = '10' THEN 100
           else exp
           END                                                         AS exp_max,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE jb.educationalRequirements = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'education_requirement')   AS edu_require,
       CASE
           WHEN jb.count = 0 THEN '不限人数'
           ELSE jb.count
           END                                                         AS position_count,
       REGEXP_REPLACE(jb.welfare, '[\\\\[\\\\]\"\\\\\\\\]', '')        AS welfare,
       jb.workplace,
       REGEXP_REPLACE(jb.function, '[\\\\[\\\\]\"\\\\\\\\]', '')       AS `function`,
       jb.jobRequiredments,
       (SELECT GROUP_CONCAT(CONCAT_WS('-', kL.labelName) SEPARATOR ',')
        FROM keywordlist kL
        WHERE jb.id = kL.jobPositionId)                                AS keyword_info,
       (SELECT GROUP_CONCAT(CONCAT_WS('-', sL.labelName) SEPARATOR ',')
        FROM skillslist sL
        WHERE jb.id = sL.jobPositionId)                                AS skill_info,
       eA.provinceCode,
       eA.cityCode,
       eA.regionCode,
       fp.fixed_province,
       fp.fixed_city,
       fp.fixed_region,
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
       REGEXP_REPLACE(eA_infor.industry, '[\\\\[\\\\]\"\\\\\\\\]', '') AS job_industry,
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
         LEFT JOIN fixed_places fp ON jb.id = fp.id
ORDER BY jb.id ASC;
"""

# 使用 pandas 从 MySQL 数据库中读取数据
df = pd.read_sql(sql_query, connection)
l = range(1, len(df) + 1)
df.insert(0, 'index', l)

description_query = """
SELECT CONCAT(jb.id, '\t')                                                           AS id,
       jb.enterpriseName,
       CONCAT('简称', eA_infor.shortName)                                            AS enter_shortName,
       CONCAT('所属行业',
              REGEXP_REPLACE(eA_infor.industry, '[\\\\[\\\\]\"\\\\\\\\]', ''))       AS enter_industry,
       CONCAT('公司性质', eA_infor.econKind)                                         AS enter_econKind,
       CONCAT('企业简介', eA_infor.introduction)                                     AS enter_introduction,
       CONCAT('岗位', jb.positionName)                                               AS positionName,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE jb.willNature = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'work_nature')                           AS willNature,
       CONCAT('支付方式',
              CASE
                  WHEN jb.payMethod = 0 THEN '年薪'
                  WHEN jb.payMethod = 1 THEN '月薪'
                  WHEN jb.payMethod = 2 THEN '日薪'
                  END)                                                               AS payMethod,
       CONCAT('工作经验',
              CASE
                  WHEN jb.exp = '0' THEN '经验不限'
                  WHEN jb.exp = '不限' THEN '经验不限'
                  WHEN jb.exp = '1' THEN '1年以上'
                  WHEN jb.exp = '3' THEN '3年以上'
                  WHEN jb.exp = '5' THEN '5年以上'
                  WHEN jb.exp = '10' THEN '10年以上'
                  ELSE jb.exp
                  END
           )                                                                         AS exp,
       CONCAT('学历要求',
              (SELECT dicttype.dict_type.label
               FROM dicttype.dict_type
               WHERE jb.educationalRequirements = dicttype.dict_type.value
                 AND dicttype.dict_type.dictType = 'education_requirement'), '以上') AS edu_require,
       CONCAT('福利',
              REGEXP_REPLACE(jb.welfare, '[\\\\[\\\\]\"\\\\\\\\]', ''))              AS welfare,
       CONCAT('职能', REGEXP_REPLACE(jb.function, '[\\\\[\\\\]\"\\\\\\\\]', ''))     AS `funciton`,
       jb.jobRequiredments
FROM job_infor jb
         LEFT JOIN (SELECT kl.jobPositionId,
                           GROUP_CONCAT(kl.labelName SEPARATOR ',') AS keywordLabel
                    FROM keywordlist kl
                    GROUP BY kl.jobPositionId) AS keywordList ON jb.id = keywordList.jobPositionId
         LEFT JOIN (SELECT skl.jobPositionId,
                           GROUP_CONCAT(skl.labelName SEPARATOR ',') AS skillsLabel
                    FROM skillslist skl
                    GROUP BY skl.jobPositionId) AS skillList ON jb.id = skillList.jobPositionId
         LEFT JOIN enterprise_infor eA_infor ON jb.enterpriseId = eA_infor.enterpriseId
         LEFT JOIN fixed_places fp ON jb.id = fp.id
ORDER BY jb.id ASC;
"""

# 使用 pandas 从 MySQL 数据库中读取数据
df1 = pd.read_sql(description_query, connection)
df1.fillna('', inplace=True)


# 自定义函数，用于拼接一行的所有单元格
def concatenate_row(row):
    return "，".join(row)


# 对 DataFrame 的每一行应用自定义函数
df1["concatenated"] = (df1.drop(columns=['id'])).apply(concatenate_row, axis=1)
df = pd.merge(df, df1[['id', 'concatenated']].copy(), on='id')

# 将数据保存到 XLSX 文件
xlsx_path = 'Q2_jobList.xlsx'

df.replace('None', '', inplace=True)
# 删除停用词，这里的路径需要按照情况进行修改
special_stopwords_path = '原数据中的停用词.txt'
special_stopwords = open(special_stopwords_path, 'r', encoding='utf-8')
stopwords_special = []
for i in special_stopwords.readlines():
    stopwords_special.append(i.strip())
for i in range(len(df.columns)):
    df[df.columns[i]] = df[df.columns[i]].map(lambda x: '' if x in stopwords_special else x)

df.to_excel(xlsx_path, index=False)

# 关闭数据库连接
connection.close()
print('文件已经成功输出！')
