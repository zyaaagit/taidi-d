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

# 编写 SQL 查询
sql_query = """
SELECT CONCAT(res.id, '\t')                                             AS id,
       res.username,
       REGEXP_REPLACE(res.expectPosition, '[\\\\[\\\\]\"\\\\\\\\]', '') AS expectPosition,
       REGEXP_REPLACE(res.address, '[\\\\[\\\\]\"\\\\\\\\]', '')        AS address,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE res.gender = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'sex')                      AS sex,
        (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE res.jobStatus = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'job_wanted_status') AS job_wanted_status,
       res.arrivalTime,
       res.birthday,
       res.exp,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE res.politicalStatus = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'political_outlook')        AS political,
       res.selfEvaluation,
       REGEXP_REPLACE(res.expectIndustry, '[\\\\[\\\\]\"\\\\\\\\]', '') AS expectIndustry,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE res.willNature = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'work_nature')              AS workNature,
        res.willSalaryStart,
        res.willSalaryEnd,
       REGEXP_REPLACE(res.city, '[\\\\[\\\\]\"\\\\\\\\]', '')           AS expectCity,
       eduEp_educationBackgrounds,
       eduEP.eduEP_schools,
       eduEP.eduEp_start,
       eduEP.eduEp_end,
       eduEP_specialities,

       projectEP.projectEP_companies,
       projectEP.projectEP_projectNames,
       projectEP.projectEp_descriptions,
       projectEP.projectEP_start,
       projectEP.projectEP_end,
       projectEP.projectEP_roleNames,
       projectEP.projectEP_achievements,

       trainEP.trainEp_orgName,
       trainEP.trainEP_recordName,
       trainEP.trainEP_start,
       trainEP.trainEP_end,
       trainEP_description,

       workEP.workEp_companies,
       workEP.workEp_industries,
       workEP.workEp_start,
       workEP.workEp_end,
       workEP.workEp_positionNames,
       workEP.workEP_descriptions,
    
       keywordList.keyword_info,

       langList.lang_info,

       certList.cert_info,

       skillList.skill_info,

       comEP.com_info
FROM resume_infor res
         LEFT JOIN (SELECT pr.resumeId,
                           GROUP_CONCAT(CONCAT(pr.company) SEPARATOR ',')     AS projectEP_companies,
                           GROUP_CONCAT(CONCAT(pr.projectName) SEPARATOR ',') AS projectEP_projectNames,
                           GROUP_CONCAT(CONCAT(pr.description) SEPARATOR ',') AS projectEp_descriptions,
                           GROUP_CONCAT(CONCAT(pr.startTime) SEPARATOR ',')   AS projectEP_start,
                           GROUP_CONCAT(CONCAT(pr.endTime) SEPARATOR ',')   AS projectEP_end,
                           GROUP_CONCAT(pr.roleName SEPARATOR ',')    AS projectEP_roleNames,
                           GROUP_CONCAT(pr.achievement SEPARATOR ',') AS projectEP_achievements
                    FROM projectexperiencelist pr
                    GROUP BY pr.resumeId) AS projectEP
                   ON res.id = projectEP.resumeId
         LEFT JOIN (SELECT tp.resumeId,
                           GROUP_CONCAT(CONCAT(tp.orgName) SEPARATOR ',')     AS trainEp_orgName,
                           GROUP_CONCAT(CONCAT(tp.recordName) SEPARATOR ',')  AS trainEP_recordName,
                           GROUP_CONCAT(CONCAT(tp.startTime) SEPARATOR ',')   AS trainEP_start,
                           GROUP_CONCAT(CONCAT(tp.endTime) SEPARATOR ',')   AS trainEP_end,
                           GROUP_CONCAT(CONCAT(tp.description) SEPARATOR ',') AS trainEP_description
                    FROM trainingexperiencelist tp
                    GROUP BY tp.resumeId) AS trainEP ON res.id = trainEP.resumeId
         LEFT JOIN (SELECT wp.resumeId,
                           GROUP_CONCAT(CONCAT(wp.company) SEPARATOR ',')      AS workEp_companies,
                           GROUP_CONCAT(CONCAT(
                           REGEXP_REPLACE(wp.industry, '[\\\\[\\\\]\"\\\\\\\\]', '')
                           ) SEPARATOR ',')     AS workEp_industries,
                           GROUP_CONCAT(CONCAT(wp.joinTime) SEPARATOR ',') AS workEp_start,
                           GROUP_CONCAT(CONCAT(wp.outTime) SEPARATOR ',') AS workEp_end,
                           GROUP_CONCAT(CONCAT(wp.positionName) SEPARATOR ',') AS workEp_positionNames,
                           GROUP_CONCAT(CONCAT(wp.description) SEPARATOR ',')  AS workEP_descriptions
                    FROM workexperiencelist wp
                    GROUP BY wp.resumeId) AS workEP ON res.id = workEP.resumeId
         LEFT JOIN (SELECT kl.resumeId,
                           GROUP_CONCAT(CONCAT_WS(':', kL.labelName) SEPARATOR ',') AS keyword_info
                    FROM keywordlist kl
                    GROUP BY kl.resumeId) AS keywordList ON res.id = keywordList.resumeId
         LEFT JOIN (SELECT lanl.resumeId,
                           GROUP_CONCAT(CONCAT_WS(':',
                                                  IFNULL(lanL.name, ''),
                                                  (SELECT dicttype.dict_type.label
                                                   FROM dicttype.dict_type
                                                   WHERE lanl.level = dicttype.dict_type.value
                                                     AND dicttype.dict_type.dictType = 'mastery')
                                            ) SEPARATOR ',') AS lang_info
                    FROM languagelist lanl
                    GROUP BY lanl.resumeId) AS langList ON res.id = langList.resumeId
         LEFT JOIN (SELECT cl.resumeId,
                           GROUP_CONCAT(CONCAT_WS(':', IFNULL(cl.name, ''), IFNULL(cl.getTime, '')) SEPARATOR
                                        '; ') AS cert_info
                    FROM certlist cl
                    GROUP BY cl.resumeId) AS certList ON res.id = certList.resumeId
         LEFT JOIN (SELECT skl.resumeId,
                           GROUP_CONCAT(CONCAT_WS(':',
                                                  IFNULL(skl.name, ''),
                                                  (SELECT dicttype.dict_type.label
                                                   FROM dicttype.dict_type
                                                   WHERE skl.level = dicttype.dict_type.value
                                                     AND dicttype.dict_type.dictType = 'mastery')
                                            ) SEPARATOR ',') AS skill_info
                    FROM skilllist skl
                    GROUP BY skl.resumeId) AS skillList ON res.id = skillList.resumeId
         LEFT JOIN (SELECT comp.resumeId,
                           GROUP_CONCAT(CONCAT_WS(':', comp.name, IFNULL(comp.level, '')) SEPARATOR ',') AS com_info
                    FROM competitionexperiencelist comp
                    GROUP BY comp.resumeId) AS comEP ON res.id = comEP.resumeId
         LEFT JOIN (SELECT edu.resumeId,
                           GROUP_CONCAT(CONCAT(edu.educationalBackground) SEPARATOR ',') AS eduEp_educationBackgrounds,
                           GROUP_CONCAT(CONCAT(edu.studyTime) SEPARATOR ',') AS eduEp_start,
                           GROUP_CONCAT(CONCAT(edu.graduationTime) SEPARATOR ',') AS eduEp_end,
                           GROUP_CONCAT(CONCAT(edu.school) SEPARATOR ',')                AS eduEP_schools,
                           GROUP_CONCAT(CONCAT(edu.speciality) SEPARATOR ',')            AS eduEP_specialities
                    FROM educationexperiencelist edu
                    GROUP BY edu.resumeId) AS eduEP ON res.id = eduEP.resumeId
ORDER BY res.id ASC;
"""
# 使用 pandas 从 MySQL 数据库中读取数据
df = pd.read_sql(sql_query, connection)

df['address'] = df['address'].str.replace(',', '')
l = range(1, len(df) + 1)
df.insert(0, 'index', l)

description_query = """
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
df1 = pd.read_sql(description_query, connection)
df1.fillna('', inplace=True)

special_stopwords_path = '原数据中的停用词.txt'

special_stopwords = open(special_stopwords_path, 'r', encoding='utf-8')
stopwords_special = []
for i in special_stopwords.readlines():
    stopwords_special.append(i.strip())

for i in df1.columns[1:]:
    df1[i] = df1[i].map(lambda x: '' if x in stopwords_special else x)

for i in df.columns[1:]:
    df[i] = df[i].map(lambda x: '' if x in stopwords_special else x)


# 自定义函数，用于拼接一行的所有单元格
def concatenate_row(row):
    return "，".join(row)


# 对 DataFrame 的每一行应用自定义函数
df1["concatenated"] = (df1.drop(columns=['id'])).apply(concatenate_row, axis=1)

df = pd.merge(df, df1[['id', 'concatenated']].copy(), on='id')

# 将数据保存到 XLSX 文件
xlsx_path = 'Q2_resume.xlsx'

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

# col = ['序号', '求职者ID', '姓名', '预期岗位', '地址', '性别', '求职状态', '到岗时间', '生日', '工作经验', '政治面貌',
#        '自我评价',
#        '预期行业', '求职性质', '预期薪资', '预期城市', '学历', '毕业院校', '在校时间', '专业', '项目所属公司',
#        '项目名称',
#        '项目经历简述', '项目时间', '项目中担任角色', '项目成就', '培训组织单位', '培训项目', '培训时间', '培训经历简述',
#        '工作企业', '工作行业', '工作时间', '工作岗位', '工作经历简述', '关键词', '语言', '证书', '技能',
#        '竞赛']
# csv_path = 'Q1_resume(提交版).csv'
# df.columns = col
# df.to_csv(csv_path, index=False)
print('文件导出完毕！')
