import pymysql
import requests

# 数据库连接配置
db_config = {
    'host': '192.168.123.243',
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'database': 'resumes'
}

# 建立数据库连接
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# 创建数据库resumes
create_db_resumes = """
    CREATE DATABASE IF NOT EXISTS resumes
"""
cursor.execute(create_db_resumes)

# 切换到resumes数据库
cursor.execute("""
    USE resumes
""")

# 在resumes数据库下创建表单ids
create_tb_ids = """
    CREATE TABLE IF NOT EXISTS ids (
        id VARCHAR(255) PRIMARY KEY
    )
"""
cursor.execute(create_tb_ids)


# 获取页面上的简历 ID
def fetch_resume_ids(page_number):
    url = "https://www.5iai.com/api/resume/baseInfo/public/es?"
    payload = {
        'pageSize': 10,
        'pageNumber': page_number,
        'function': '',
        'skills': '',
        'workplace': '',
        'keyword': ''
    }

    response = requests.get(url, params=payload)
    data = response.json()
    resumes = data['data']['content']

    resume_ids = []
    for resume in resumes:
        resume_ids.append((resume['id'],))

    return resume_ids


# 遍历所有页面并将获取到的 ID 存储在一个列表中
all_ids = []
for page_number in range(1, 1094):
    resume_ids = fetch_resume_ids(page_number)
    print(f"正在爬取 pageNumber: {page_number}")
    all_ids.extend(resume_ids)

# 使用 executemany 一次性插入所有 ID
query = "INSERT IGNORE INTO ids (id) VALUES (%s)"
cursor.executemany(query, set(all_ids))
conn.commit()

# 关闭数据库连接
cursor.close()
conn.close()

print(len(all_ids))

print("所有id数据已成功插入到数据库中，一共{}条。".format(len(set(all_ids))))
