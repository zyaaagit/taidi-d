import pymysql
import requests

# 数据库连接配置
db_config = {
    'host': '192.168.123.243',
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'database': 'mysql'
}

# 建立数据库连接
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# 创建数据库jobList
create_db_jobList = """
    CREATE DATABASE IF NOT EXISTS jobList
"""
cursor.execute(create_db_jobList)

# 切换到jobList数据库
cursor.execute("""
    USE jobList
""")

# 创建 "ids" 表

# 在resumes数据库下创建表单ids
create_tb_ids = """
    CREATE TABLE IF NOT EXISTS ids (
        id VARCHAR(255) PRIMARY KEY
    )
"""
cursor.execute(create_tb_ids)


# 获取页面上job的ID
def fetch_jobs_ids(page_number):
    url = 'https://www.5iai.com/api/enterprise/job/public/es?'
    payload = {
        'pageSize': 16,
        'pageNumber': page_number,
        'willNature': '',
        'function': '',
        'workplace': '',
        'keyword': ''
    }

    response = requests.get(url, params=payload)
    data = response.json()
    companies = data['data']['content']
    company_ids = []
    for company in companies:
        company_ids.append(company['id'])
    return company_ids


# 遍历所有页面并将获取到的 ID 存储在一个列表中
all_ids = []
for page_number in range(1, 159):
    resume_ids = fetch_jobs_ids(page_number)
    print(f"正在爬取 pageNumber: {page_number}")
    all_ids.extend(resume_ids)

# 使用 executemany 一次性插入所有 ID
query = "INSERT IGNORE INTO ids (id) VALUES (%s)"
cursor.executemany(query, all_ids)
conn.commit()

cursor.close()
conn.close()
print("所有id数据已成功插入到数据库中，共收集到{}条数据".format(len(all_ids)))
