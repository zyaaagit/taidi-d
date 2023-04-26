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
       REGEXP_REPLACE(res.expectPosition, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '') AS expectPosition,
       REGEXP_REPLACE(res.address, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '')        AS address,
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
       REGEXP_REPLACE(res.expectIndustry, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '') AS expectIndustry,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE res.willNature = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'work_nature')              AS workNature,
        CONCAT(res.willSalaryStart, '-', res.willSalaryEnd) AS expectSalary,
       REGEXP_REPLACE(res.city, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '')           AS expectCity,
       eduEp_educationBackgrounds,
       eduEP.eduEP_schools,
       eduEP.eduEp_studyTimes,
       eduEP_specialities,

       projectEP.projectEP_companies,
       projectEP.projectEP_projectNames,
       projectEP.projectEp_descriptions,
       projectEP.projectEP_period,
       projectEP.projectEP_roleNames,
       projectEP.projectEP_achievements,

       trainEP.trainEp_orgName,
       trainEP.trainEP_recordName,
       trainEP.trainEP_period,
       trainEP_description,

       workEP.workEp_companies,
       workEP.workEp_industries,
       workEP.workEp_period,
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
                           GROUP_CONCAT(CONCAT(pr.startTime, ':', pr.endTime) SEPARATOR ',')   AS projectEP_period,
                           GROUP_CONCAT(pr.roleName SEPARATOR ',')    AS projectEP_roleNames,
                           GROUP_CONCAT(pr.achievement SEPARATOR ',') AS projectEP_achievements
                    FROM projectexperiencelist pr
                    GROUP BY pr.resumeId) AS projectEP
                   ON res.id = projectEP.resumeId
         LEFT JOIN (SELECT tp.resumeId,
                           GROUP_CONCAT(CONCAT(tp.orgName) SEPARATOR ',')     AS trainEp_orgName,
                           GROUP_CONCAT(CONCAT(tp.recordName) SEPARATOR ',')  AS trainEP_recordName,
                           GROUP_CONCAT(CONCAT(tp.startTime, ':', tp.endTime) SEPARATOR ',')   AS trainEP_period,
                           GROUP_CONCAT(CONCAT(tp.description) SEPARATOR ',') AS trainEP_description
                    FROM trainingexperiencelist tp
                    GROUP BY tp.resumeId) AS trainEP ON res.id = trainEP.resumeId
         LEFT JOIN (SELECT wp.resumeId,
                           GROUP_CONCAT(CONCAT(wp.company) SEPARATOR ',')      AS workEp_companies,
                           GROUP_CONCAT(CONCAT(
                           REGEXP_REPLACE(wp.industry, '[\\\\[\\\\]\"\“\”\\\\\\\\]', '')
                           ) SEPARATOR ',')     AS workEp_industries,
                           GROUP_CONCAT(CONCAT(wp.joinTime, ':', wp.outTime) SEPARATOR ',') AS workEp_period,
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
                           GROUP_CONCAT(CONCAT(edu.studyTime, ':', edu.graduationTime) SEPARATOR ',') AS eduEp_studyTimes,
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

# 将数据保存到 XLSX 文件
xlsx_path = 'Q1_resume.xlsx'
df['keyword_info'] = df['keyword_info'].astype('str')
df.replace('None','',inplace=True)
# 删除停用词，这里的路径需要按照情况进行修改
special_stopwords_path = '../jobs/原数据中的停用词.txt'
special_stopwords = open(special_stopwords_path,'r', encoding='utf-8')
stopwords_special = []
for i in special_stopwords.readlines():
    stopwords_special.append(i.strip())
for i in range(len(df.columns)):
    df[df.columns[i]] = df[df.columns[i]].map(lambda x:'' if x in stopwords_special else x)

df.to_excel(xlsx_path, index=False)

# 关闭数据库连接
connection.close()

col = ['序号', '求职者ID', '姓名', '预期岗位', '地址', '性别', '求职状态', '到岗时间', '生日', '工作经验', '政治面貌',
       '自我评价',
       '预期行业', '求职性质', '预期薪资', '预期城市', '学历', '毕业院校', '在校时间', '专业', '项目所属公司',
       '项目名称',
       '项目经历简述', '项目时间', '项目中担任角色', '项目成就', '培训组织单位', '培训项目', '培训时间', '培训经历简述',
       '工作企业', '工作行业', '工作时间', '工作岗位', '工作经历简述', '关键词', '语言', '证书', '技能',
       '竞赛']
csv_path = 'Q1_resume(提交版).csv'
df.columns = col
df.to_csv(csv_path, index=False)
print('文件导出完毕！')


