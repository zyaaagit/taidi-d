
import pymysql
import pandas as pd

# MySQL 连接配置
config = {
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'host': '192.168.123.243',
    'database': 'resumes',
}

# 连接到 MySQL 数据库
connection = pymysql.connect(**config)

sql_query = """
SELECT CONCAT(res.id, '\t')                                                                     AS id,
       res.username,
       CONCAT('意向岗位：', (REGEXP_REPLACE(res.expectPosition, '[\\\\[\\\\]\"\\\\\\\\]', ''))) AS expectPosition,
       CONCAT('来自', REGEXP_REPLACE(res.address, '[\\\\[\\\\]\"\\\\\\\\]', ''))               AS address,
       CONCAT('性别', (SELECT dicttype.dict_type.label
                       FROM dicttype.dict_type
                       WHERE res.gender = dicttype.dict_type.value
                         AND dicttype.dict_type.dictType = 'sex'))                              AS sex,
       CONCAT('可', res.arrivalTime)                                                           AS arrivalTime,
       CONCAT('', res.exp)                                                                   AS exp,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE res.politicalStatus = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'political_outlook')                                AS political,
       res.selfEvaluation,
       CONCAT('期待行业：', REGEXP_REPLACE(res.expectIndustry, '[\\\\[\\\\]\"\\\\\\\\]', ''))   AS expectIndustry,
       CONCAT('找一份', (SELECT dicttype.dict_type.label
                          FROM dicttype.dict_type
                          WHERE res.willNature = dicttype.dict_type.value
                            AND dicttype.dict_type.dictType = 'work_nature'), '工作')           AS workNature,
       CONCAT('意向工作城市', REGEXP_REPLACE(res.city, '[\\\\[\\\\]\"\\\\\\\\]', ''))          AS expectCity,
       eduEP.edu_description,
       projectEP.proj_descrition,
       trainEP.train_description,
       workEP.workEp_description,
       langList.lanl_description,
       certList.cl_description,
       skillList.skill_discription,
       comEP.comp_description
FROM resume_infor res
         LEFT JOIN (SELECT pr.resumeId,
                           GROUP_CONCAT(CONCAT_WS('，',
                                                  IFNULL(CONCAT('项目所属公司：', pr.company), ''),
                                                  IFNULL(CONCAT('参加的项目名称：',
                                                                pr.projectName), ''),
                                                  IFNULL(CONCAT('该项目中担任职位：',
                                                                pr.roleName), ''),
                                                  IFNULL(CONCAT('项目主要内容描述：',
                                                                pr.description), ''),
                                                  IFNULL(CONCAT('项目的主要成就：',
                                                                pr.achievement), '')) SEPARATOR '；') AS proj_descrition
                    FROM projectexperiencelist pr
                    GROUP BY pr.resumeId) AS projectEP
                   ON res.id = projectEP.resumeId
         LEFT JOIN (SELECT tp.resumeId,
                           GROUP_CONCAT(CONCAT_WS('，',
                                                  IFNULL(CONCAT('组织培训的公司：', tp.orgName), ''),
                                                  IFNULL(CONCAT('培训主题：',
                                                                tp.recordName), ''),
                                                  IFNULL(CONCAT('培训心得描述：',
                                                                tp.description), '')) SEPARATOR
                                        '；') AS train_description
                    FROM trainingexperiencelist tp
                    GROUP BY tp.resumeId) AS trainEP ON res.id = trainEP.resumeId
         LEFT JOIN (SELECT wp.resumeId,
                           GROUP_CONCAT(CONCAT_WS('，',
                                                  IFNULL(CONCAT('工作公司：', wp.company), ''),
                                                  IFNULL(CONCAT('所属行业：', 
                                                                REGEXP_REPLACE(wp.industry, '[\\\\[\\\\]\"\\\\\\\\]', '')), ''),
                                                  IFNULL(CONCAT('职位：', wp.positionName), ''),
                                                  IFNULL(CONCAT('工作内容：',
                                                                IFNULL(wp.willNature, ''), wp.description), ''),
                                                  IFNULL(CONCAT('岗位：',
                                                                wp.positionName), '')) SEPARATOR '；') AS
                               workEp_description
                    FROM workexperiencelist wp
                    GROUP BY wp.resumeId) AS workEP ON res.id = workEP.resumeId
         LEFT JOIN (SELECT lanl.resumeId,
                           GROUP_CONCAT(CONCAT_WS('，',
                                                  IFNULL(CONCAT(lanl.name,
                                                                (SELECT dicttype.dict_type.label
                                                                 FROM dicttype.dict_type
                                                                 WHERE lanl.level = dicttype.dict_type.value
                                                                   AND dicttype.dict_type.dictType = 'mastery'))
                                                      , '')) SEPARATOR '；') AS lanl_description
                    FROM languagelist lanl
                    GROUP BY lanl.resumeId) AS langList ON res.id = langList.resumeId
         LEFT JOIN (SELECT cl.resumeId,
                           GROUP_CONCAT(CONCAT_WS(',',
                                                  CONCAT('获得证书', cl.name)) SEPARATOR
                                        '；') AS cl_description
                    FROM certlist cl
                    GROUP BY cl.resumeId) AS certList ON res.id = certList.resumeId
         LEFT JOIN (SELECT skl.resumeId,
                           GROUP_CONCAT(CONCAT_WS('',
                                                  IFNULL(skl.name, ''),
                                                  (SELECT dicttype.dict_type.label
                                                   FROM dicttype.dict_type
                                                   WHERE skl.level = dicttype.dict_type.value
                                                     AND dicttype.dict_type.dictType = 'mastery')
                                            ) SEPARATOR ',') AS skill_discription
                    FROM skilllist skl
                    GROUP BY skl.resumeId) AS skillList ON res.id = skillList.resumeId
         LEFT JOIN (SELECT comp.resumeId,
                           GROUP_CONCAT(CONCAT(
                                                  IFNULL(CONCAT('获得', comp.name, '', IFNULL(comp.level, '')),
                                                         '')) SEPARATOR
                                        '；') AS comp_description
                    FROM competitionexperiencelist comp
                    GROUP BY comp.resumeId) AS comEP ON res.id = comEP.resumeId
         LEFT JOIN (SELECT edu.resumeId,
                           GROUP_CONCAT(CONCAT_WS(',',
                                                  IFNULL(edu.educationalBackground, ''),
                                                  IFNULL(CONCAT('就读于', edu.school), ''),
                                                  IFNULL(CONCAT('专业为', edu.speciality), '')) SEPARATOR
                                        '；') AS edu_description
                    FROM educationexperiencelist edu
                    GROUP BY edu.resumeId) AS eduEP ON res.id = eduEP.resumeId;
"""

# 使用 pandas 从 MySQL 数据库中读取数据
df = pd.read_sql(sql_query, connection)
df.fillna('', inplace=True)

# 自定义函数，用于拼接一行的所有单元格
def concatenate_row(row):
    return "，".join(row)


# 对 DataFrame 的每一行应用自定义函数
df["concatenated"] =(df.drop(columns=['id'])).apply(concatenate_row, axis=1)







# 将数据保存到 XLSX 文件
xlsx_path = 'resume_description_for_Bert.xlsx'
df.to_excel(xlsx_path, index=False)
# 关闭数据库连接
connection.close()
print('文件导出完毕！')
