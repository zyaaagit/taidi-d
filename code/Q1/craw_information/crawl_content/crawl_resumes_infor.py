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

# 查询所有的 ID
query = "SELECT id FROM ids"
cursor.execute(query)
all_ids = cursor.fetchall()
print('已请求到所有id，接下来进行数据爬取, 共有{}个id'.format(len(all_ids)))


# 定义一个函数，用于爬取指定 ID 的网页内容
def fetch_resume_data(resume_id):
    url = f'https://www.5iai.com/api/resume/baseInfo/public/{resume_id}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        try:
            return data['data']
        except:
            print(resume_id)


# 遍历所有的 ID，爬取对应的网页内容
all_data = []
i = 0
for resume_id_tuple in all_ids:
    resume_id = resume_id_tuple[0]
    resume_data = fetch_resume_data(resume_id)
    if resume_data is not None:
        all_data.append(resume_data)
    if i % 100 == 0:
        print('已完成{}%'.format(round((i / len(all_ids) * 100)), 2))
    print(i)
    i += 1


def insert_personal_infor(all_data_infor, fields, tableName):
    query = f"""
        INSERT IGNORE INTO {tableName} ({','.join(fields)})
        VALUES ({','.join(['%s' for _ in fields])})
    """
    cursor.executemany(query, all_data_infor)
    conn.commit()
    print("{}已经插入至对应数据库中".format(tableName))


single_keys = [key for key in all_data[0].keys() if not isinstance(all_data[0].get(key), (list, dict))]
list_keys = [key for key in all_data[0].keys() if isinstance(all_data[0].get(key), (list, dict))]

# 提取简历id一类信息，并插入至resume_infor的表单中
resume_infor = [[resume_data.get(key) for key in single_keys] for resume_data in all_data]
insert_personal_infor(resume_infor, single_keys, 'resume_infor')

# 对list_keys进行遍历，对每一个元素进行操作
# 其中keywordList中没有resumeId，所以需要单独处理
for list_key in list_keys:
    if list_key == 'professionalList':
        continue
    if list_key != 'keywordList':
        list_datas = [resume_data.get(list_key) for resume_data in all_data if len(resume_data.get(list_key)) > 0]
        try:
            item_keys = list(list_datas[0][0].keys())
        except:
            print('list_datas'.format(list_datas))
        datas = [item for data in list_datas for item in data]
        key_infor = [[item_data.get(key) for key in item_keys] for item_data in datas]
        insert_personal_infor(key_infor, item_keys, list_key)
    else:
        list_datas = [resume_data.get(list_key) + [resume_data.get('id')] for resume_data in all_data if
                      len(resume_data.get(list_key)) > 0]
        try:
            item_keys = list(list_datas[0][0].keys())
        except:
            print('list_data{}'.format(list_datas))
        key_infor = []
        for datas in list_datas:
            try:
                resume_id = datas[-1]
            except:
                print('datas{}'.format(datas))
            try:
                data = datas[0:-1]
            except:
                print('datas{}'.format(datas))
            for item in data:
                key_infor.append(list(item.values()) + [resume_id])
        insert_personal_infor(key_infor, item_keys + ['resumeId'], list_key)

cursor.close()
conn.close()
