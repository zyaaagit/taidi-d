import requests
import pymysql

# 数据库连接配置
db_config = {
    'host': '192.168.123.243',
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'database': 'dicttype'
}

# 建立数据库连接
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

dict_type_fileds = ['sex', 'political_outlook', 'work_nature', 'mastery', 'job_wanted_status', 'function', 'skill',
                    'education_requirement', 'industry', 'enterprise_type', 'enterprise_scale', 'region_code']

#建立一个地推函数，一直取到'children'没有数据，即取到最底层数据
def collect_data(node, params):
    params.append(node)
    if node['children']:
        for child in node['children']:
            collect_data(child, params)


def fetch_infor(dict_type):
    print('crawling {}'.format(dict_type))
    url = f'https://www.5iai.com/api/dict/data/public/list/tree?dictType={dict_type}'
    response = requests.get(url)
    if response.status_code == 200:
        try:
            data_list = response.json()['data']
            if dict_type != 'region_code':
                return data_list
            else:
                try:
                    params = []
                    for data in data_list:
                        collect_data(data, params)
                    return params
                except:
                    print('没有获取到数据')
        except:
            print(dict_type)


def insert_data(params):
    with conn.cursor() as cursor:
        sql = """
        INSERT IGNORE INTO dict_type (id, label, value, dictType, parentId, sort, remark)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(sql, params)
        conn.commit()
    print('已经插入{}条数据'.format(len(params)))


all_dictType_infor = []
for dict_type in dict_type_fileds:
    all_dictType_infor.append(fetch_infor(dict_type))

all_dictType_infor = [item for dict_type_data in all_dictType_infor for item in dict_type_data]
keys = ['id', 'label', 'value', 'dictType', 'parentId', 'sort', 'remark']
infor = []
for one_dictType in all_dictType_infor:
    try:
        infor.append([one_dictType.get(key) for key in keys])
    except:
        print(one_dictType)
try:
    insert_data(infor)
    print('数据已经完成爬取，并且成功存入数据库！')
except:
    print('爬取数据失败！')
finally:
    cursor.close()
    conn.close()
