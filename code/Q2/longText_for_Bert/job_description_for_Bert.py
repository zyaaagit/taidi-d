import pymysql
import pandas as pd

# MySQL 连接配置
config = {
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'host': '192.168.123.243',
    'database': 'jobList',
}

# 连接到 MySQL 数据库
connection = pymysql.connect(**config)

sql_query = """
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
                  WHEN jb.exp = 0 THEN '经验不限'
                  WHEN jb.exp = '不限' THEN '经验不限'
                  WHEN jb.exp = 1 THEN '1年以上'
                  WHEN jb.exp = 3 THEN '3年以上'
                  WHEN jb.exp = 5 THEN '5年以上'
                  WHEN jb.exp = 10 THEN '10年以上'
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
         LEFT JOIN fixed_places fp ON jb.enterpriseId = fp.id
ORDER BY jb.id ASC;
"""

# 使用 pandas 从 MySQL 数据库中读取数据
df = pd.read_sql(sql_query, connection)
df.fillna('', inplace=True)


# 自定义函数，用于拼接一行的所有单元格
def concatenate_row(row):
    return "，".join(row)


# 对 DataFrame 的每一行应用自定义函数
df["concatenated"] = (df.drop(columns=['id'])).apply(concatenate_row, axis=1)

# 将数据保存到 XLSX 文件
xlsx_path = 'job_description_for_Bert.xlsx'
df.to_excel(xlsx_path, index=False)
# 关闭数据库连接
connection.close()
print('文件导出完毕！')
