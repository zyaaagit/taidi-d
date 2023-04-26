import pymysql
import requests

# 数据库连接配置
db_config = {
    'host': '192.168.123.243',
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'database': 'jobList'
}

# 建立数据库连接
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# 查询所有的 ID
query = "SELECT DISTINCT enterpriseId FROM job_infor"
cursor.execute(query)
all_ids = cursor.fetchall()
print('已请求到所有id，接下来进行数据爬取, 共有{}个id'.format(len(all_ids)))


# 定义一个函数，用于爬取指定 ID 的网页内容
def fetch_enterprise_data(enterpriseId):
    url = f'https://www.5iai.com/api/enterprise/detail/{enterpriseId}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        try:
            return data['data']
        except:
            print(enterpriseId)


# 定义一个函数，用于将爬取到的数据插入到指定的表单中
def insert_infor(all_data_infor, fields, tableName):
    if 'function' in fields:
        idx = fields.index('function')
        fields[idx] = '`function`'
    query = f"""
        INSERT IGNORE INTO {tableName} ({','.join(fields)})
        VALUES ({','.join(['%s' for _ in fields])})
    """
    print(query)
    cursor.executemany(query, all_data_infor)
    conn.commit()
    print("{}已经插入至对应数据库中".format(tableName))


# 遍历所有的 ID，爬取对应的网页内容
all_data = []
i = 0
for position_id_tuple in all_ids:
    position_id = position_id_tuple[0]
    position_data = fetch_enterprise_data(position_id)
    if position_data is not None:
        all_data.append(position_data)
    if i % 100 == 0:
        print('已完成{}%'.format(round((i / len(all_ids) * 100)), 2))
    print(i)
    i += 1

single_keys = [key for key in all_data[0].keys() if not isinstance(all_data[0].get(key), (list, dict))]

# 提取简历id一类信息，并插入至job_infor的表单中
resume_infor = [[resume_data.get(key) for key in single_keys] for resume_data in all_data]
insert_infor(resume_infor, single_keys, 'enterprise_infor')

cursor.close()
conn.close()
