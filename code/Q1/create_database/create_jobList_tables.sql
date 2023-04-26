CREATE DATABASE IF NOT EXISTS jobList;

USE jobList;


CREATE TABLE IF NOT EXISTS job_infor
(
    id                      varchar(255) primary key,
    count                   int,
    deadline                date,
    educationalRequirements int,
    enterpriseId            varchar(255),
    enterpriseName          varchar(255),
    exp                     varchar(255),
    `function`              varchar(255),
    jobRequiredments        text,
    maximumWage             int,
    minimumWage             int,
    payMethod               int,
    positionName            varchar(255),
    publishTime             date,
    publisher               varchar(255),
    publisherName           varchar(255),
    resumeCount             int,
    status                  int,
    updateTime              date,
    welfare                 varchar(255),
    willNature              int,
    workplace               varchar(255),
    messageTemplateId       varchar(255)
);

CREATE TABLE IF NOT EXISTS enterpriseAddress
(
    id              VARCHAR(255),
    cityCode        VARCHAR(255),
    detailedAddress VARCHAR(255),
    enterpriseId    VARCHAR(255) PRIMARY KEY,
    provinceCode    VARCHAR(255),
    regionCode      VARCHAR(255),
    remarks         VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS keywordList
(
    id            VARCHAR(255) PRIMARY KEY ,
    createTime    DATE,
    creatorId     VARCHAR(255),
    creatorName   VARCHAR(255),
    updateTime    DATE,
    jobPositionId VARCHAR(255),
    labelId       VARCHAR(255),
    labelName     VARCHAR(255),
    labelTypeId   VARCHAR(255),
    labelTypeName VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS skillsList
(
    id            VARCHAR(255) PRIMARY KEY,
    createTime    DATE,
    creatorId     VARCHAR(255),
    creatorName   VARCHAR(255),
    updateTime    DATE,
    jobPositionId VARCHAR(255),
    labelId       VARCHAR(255),
    labelName     VARCHAR(255),
    labelTypeId   VARCHAR(255),
    labelTypeName VARCHAR(255)
);


CREATE TABLE IF NOT EXISTS enterprise_infor
(
    id                VARCHAR(255),
    enterpriseId      VARCHAR(255) PRIMARY KEY,
    logo              VARCHAR(255),
    shortName         VARCHAR(255),
    industry          VARCHAR(255),
    econKind          VARCHAR(255),
    startDate         DATE,
    registCapi        VARCHAR(255),
    personScope       VARCHAR(255),
    website           VARCHAR(255),
    email             VARCHAR(255),
    phone             VARCHAR(255),
    slogan            VARCHAR(255),
    introduction      TEXT,
    photo             VARCHAR(255),
    label             VARCHAR(255),
    postCode          VARCHAR(255),
    recruitJobNum     INT,
    totalPublicJobNum INT
);
