CREATE DATABASE IF NOT EXISTS resumes;

USE resumes;

CREATE TABLE IF NOT EXISTS resume_infor
(
    id              VARCHAR(255) PRIMARY KEY,
    resumeName      VARCHAR(255),
    username        VARCHAR(255),
    gender          INT,
    birthday        DATE,
    address         VARCHAR(255),
    arrivalTime     VARCHAR(255),
    publishTime     DATE,
    politicalStatus VARCHAR(255),
    city            VARCHAR(255),
    exp             VARCHAR(255),
    expectIndustry  VARCHAR(255),
    expectPosition  VARCHAR(255),
    jobStatus       INT,
    selfEvaluation  TEXT,
    willNature      INT,
    willSalaryEnd   INT,
    willSalaryStart INT
);


CREATE TABLE IF NOT EXISTS keywordList
(
    id            VARCHAR(255) PRIMARY KEY,
    enterpriseId  VARCHAR(255),
    resumeId      VARCHAR(255),
    labelName     VARCHAR(255),
    labelId       VARCHAR(255),
    labelTypeId   VARCHAR(255),
    labelTypeName VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS educationExperienceList
(

    id                    VARCHAR(255) PRIMARY KEY,
    educationalBackground VARCHAR(255),
    graduationTime        DATE,
    resumeId              VARCHAR(255),
    school                VARCHAR(255),
    speciality            VARCHAR(255),
    studyTime             DATE
);

CREATE TABLE IF NOT EXISTS projectExperienceList
(
    id          VARCHAR(255) PRIMARY KEY,
    achievement TEXT,
    company     VARCHAR(255),
    description TEXT,
    endTime     DATE,
    projectName VARCHAR(255),
    resumeId    VARCHAR(255),
    roleName    VARCHAR(255),
    startTime   DATE
);

CREATE TABLE IF NOT EXISTS competitionExperienceList
(
    id          VARCHAR(255) PRIMARY KEY,
    level       VARCHAR(255),
    name        VARCHAR(255),
    resumeId    VARCHAR(255),
    type        VARCHAR(255),
    certificate VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS trainingExperienceList
(

    description TEXT,
    endTime     DATE,
    id          VARCHAR(255) PRIMARY KEY,
    orgName     VARCHAR(255),
    recordName  VARCHAR(255),
    resumeId    VARCHAR(255),
    startTime   DATE
);

CREATE TABLE IF NOT EXISTS skillList
(
    id       VARCHAR(255) PRIMARY KEY,
    level    VARCHAR(255),
    name     VARCHAR(255),
    resumeId VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS languageList
(
    id       VARCHAR(255) PRIMARY KEY,
    level    VARCHAR(255),
    name     VARCHAR(255),
    resumeId VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS certList
(
    id         VARCHAR(255) PRIMARY KEY,
    resumeId   VARCHAR(255),
    getTime    DATE,
    name       VARCHAR(255),
    type       VARCHAR(255),
    attachment VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS workExperienceList
(
    id           VARCHAR(255) PRIMARY KEY,
    company      VARCHAR(255),
    description  TEXT,
    industry     VARCHAR(255),
    joinTime     DATE,
    outTime      DATE,
    positionName VARCHAR(255),
    resumeId     VARCHAR(255),
    willNature   INT

);

CREATE TABLE IF NOT EXISTS attachmentList
(
    id VARCHAR(255) PRIMARY KEY,
    resumeId VARCHAR(255),
    name VARCHAR(255),
    description TEXT,
    `file` VARCHAR(255)
);


