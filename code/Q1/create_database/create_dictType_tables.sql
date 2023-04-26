CREATE DATABASE IF NOT EXISTS dictType;


USE dictType;
CREATE TABLE IF NOT EXISTS dict_type
(
    children VARCHAR(255),
    dictType VARCHAR(255),
    id VARCHAR(255) PRIMARY KEY,
    label VARCHAR(255),
    parentId VARCHAR(255),
    remark VARCHAR(255),
    sort VARCHAR(255),
    value VARCHAR(255)
);