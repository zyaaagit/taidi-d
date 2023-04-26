USE joblist;

SELECT CONCAT(jb.id, '\t')                                                           AS id,
       jb.enterpriseName,
       CONCAT('简称', eA_infor.shortName)                                            AS enter_shortName,
       CONCAT('所属行业', eA_infor.industry)                                         AS enter_industry,
       CONCAT('公司性质', eA_infor.econKind)                                         AS enter_econKind,
       CONCAT('规模', eA_infor.personScope)                                          AS enter_personScope,
       CONCAT('注册资本', eA_infor.registCapi)                                       AS enter_resistCapi,
       CONCAT('企业简介', eA_infor.introduction)                                     AS enter_introduction,
       CONCAT('在招岗位', eA_infor.recruitJobNum, '个')                              AS enter_recruitJobNum,
       CONCAT('全部职位', eA_infor.totalPublicJobNum, '个')                          AS enter_totalPublicJobNum,
       CONCAT('岗位', jb.positionName)                                               AS positionName,
       (SELECT dicttype.dict_type.label
        FROM dicttype.dict_type
        WHERE jb.willNature = dicttype.dict_type.value
          AND dicttype.dict_type.dictType = 'work_nature')                           AS willNature,
       CONCAT('薪资', jb.minimumWage, '-', jb.maximumWage)                           AS salary,
       CONCAT('支付方式',
              CASE
                  WHEN jb.payMethod = 0 THEN '年薪'
                  WHEN jb.payMethod = 1 THEN '月薪'
                  WHEN jb.payMethod = 2 THEN '日薪'
                  END)                                                               AS payMethod,
       CONCAT('工作经验', jb.exp, '以上')                                            AS exp,
       CONCAT('学历要求',
              (SELECT dicttype.dict_type.label
               FROM dicttype.dict_type
               WHERE jb.educationalRequirements = dicttype.dict_type.value
                 AND dicttype.dict_type.dictType = 'education_requirement'), '以上') AS edu_require,
       CONCAT('招聘人数',
              CASE
                  WHEN jb.count = 0 THEN '不限人数'
                  ELSE jb.count
                  END
           )                                                                         AS position_count,
       CONCAT('福利',
              REGEXP_REPLACE(jb.welfare, '[\\\\[\\\\]\"\\\\\\\\]', ''))              AS welfare,
       CONCAT('职能', jb.function)                                                   AS `funciton`,
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
ORDER BY jb.id ASC
;