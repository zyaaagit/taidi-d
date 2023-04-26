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
query = "SELECT id FROM ids"
cursor.execute(query)
all_ids = cursor.fetchall()
print('已请求到所有id，接下来进行数据爬取, 共有{}个id'.format(len(all_ids)))


# 定义一个函数，用于爬取指定 ID 的网页内容
def fetch_jobList_data(position_id):
    url = f'https://www.5iai.com/api/enterprise/job/public?id={position_id}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        try:
            return data['data']
        except:
            print(position_id)


# 定义一个函数，用于将爬取到的数据插入到指定的表单中
def insert_job_infor(all_data_infor, fields, tableName):
    if 'function' in fields:
        idx = fields.index('function')
        fields[idx] = '`function`'
    query = f"""
        INSERT IGNORE INTO {tableName} ({','.join(fields)})
        VALUES ({','.join(['%s' for _ in fields])})
    """
    cursor.executemany(query, all_data_infor)
    conn.commit()
    print("{}已经插入至对应数据库中".format(tableName))


# 遍历所有的 ID，爬取对应的网页内容
all_data = []
i = 0
for position_id_tuple in all_ids:
    position_id = position_id_tuple[0]
    position_data = fetch_jobList_data(position_id)
    if position_data is not None:
        all_data.append(position_data)
    if i % 100 == 0:
        print('已完成{}%'.format(round((i / len(all_ids) * 100)), 2))
    print(i)
    i += 1

single_keys = [key for key in all_data[0].keys() if not isinstance(all_data[0].get(key), (list, dict))]
list_keys = [key for key in all_data[0].keys() if isinstance(all_data[0].get(key), (list, dict))]

# 提取简历id一类信息，并插入至job_infor的表单中
resume_infor = [[resume_data.get(key) for key in single_keys] for resume_data in all_data]
insert_job_infor(resume_infor, single_keys, 'job_infor')

# 对list_keys进行遍历，对每一个元素进行操作
for list_key in list_keys:
    print(list_key)
    list_datas = [job_data.get(list_key) for job_data in all_data if len(job_data.get(list_key)) > 0]
    if list_key == 'enterpriseAddress':
        item_keys = list(list_datas[0].keys())
        datas = list_datas


    else:
        try:
            item_keys = list(list_datas[0][0].keys())
        except:
            print('list_datas is ------------------------------{}'.format(list_datas))
            print('list_key is --------------------------------{}'.format(list_key))
        datas = [item for data in list_datas for item in data]
    key_infor = [[item_data.get(key) for key in item_keys] for item_data in datas]
    insert_job_infor(key_infor, item_keys, list_key)

cursor.close()
conn.close()