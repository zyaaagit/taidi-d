import pandas as pd
import cpca
import pymysql

def get_workLoc(df_jobs):
    work_loc = cpca.transform(df_jobs.workplace, pos_sensitive = True)
    work_loc = work_loc[['省', '市', '区', '地址', 'adcode']]
    work_loc.columns = ['work_province', 'work_city', 'work_region', 'work_detailedAddress', 'work_adcode']
    work_loc.fillna('', inplace= True)
    return work_loc

# 检查是否为异常值的函数: NaN, None, or ''
def is_abnormal(value):
    return pd.isna(value) or value == ''

# 根据给定的条件合并df_a和df_b的行:
# 1. 如果workplace中省，市，区域都有详细登记的地址，就忽略enterprisePlace的数据，直接用workplace的数据。
# 2. 如果workplace中只有省，市有详细登记的地址，此时分几种情况：
#     a. 如果enterprisePlace的数据三列都为异常值或者只有省，市的数据或者只有省的数据，则直接用workplace的数据；
#     b. 如果enterprisePlace的数据三列都为正常值且省，市前两项数据与workplace中的值是一致的，则使用enterprisePlace的数据，否则依旧使用workplace的数据。
# 3. 如果workplace中只有省有详细等级，此时分几种情况：
#     a. 如果enterprisePlace的数据三列都为异常值或者只有省的数据，则直接用workplace的数据；
#     b. 如果enterprisePlace的数据三列都为正常值或者省，市为正常值或者省为正常值且省的数据与workplace中的值是一致的，则使用enterprisePlace的数据，否则依旧使用workplcae的数据。
def merge_rows(row_workplace, row_enterprise):
    #--------------------------------------
    #判断两个dataframe的三列值分别为正常值或者异常值
    work_a_valid = not is_abnormal(row_workplace['province'])
    work_b_valid = not is_abnormal(row_workplace['city'])
    work_c_valid = not is_abnormal(row_workplace['region'])

    entp_a_valid = not is_abnormal(row_enterprise['province'])
    entp_b_valid = not is_abnormal(row_enterprise['city'])
    entp_c_valid = not is_abnormal(row_enterprise['region'])

    #-------------------------------------
    #如果row_workplace的三列都正正常值，则直接返回row_workplace
    if work_a_valid and work_b_valid and work_c_valid:
        return row_workplace

    #如果row_workplace的省，市都正正常值，区为异常值：
    elif work_a_valid and work_b_valid and not work_c_valid:
        #同时，row_enterprise省，市，区都为异常值，则返回row_workplace
        if not (entp_a_valid or entp_b_valid or entp_c_valid):
            return row_workplace
        #如果row_workplace和row_enterprise的省，市的值是一样的，则返回row_enterprise，
        #这里包括了row_enterprise有省，市的值或者有省，市，区的值
        elif row_workplace['province'] == row_enterprise['province'] and row_workplace['city'] == row_enterprise['city']:
            return row_enterprise
        #如果row_enterprise有省的值，或者有省，市（省，市，区）的值但是与workplace的省，市值不一致，则返回row_workplace
        else:
            return row_workplace

    #如果row_workplace的省为正常值，同时市，区为异常值：
    elif work_a_valid and not work_b_valid and not work_c_valid:
        # 如果row_enterprise省，市，区都为异常值，或者只有省有数据，则返回row_workplace
        if not (entp_a_valid or entp_b_valid or entp_c_valid) or (entp_a_valid and not entp_b_valid and not entp_c_valid):
            return row_workplace
        #如果row_enterprise有省，市，区（省，市；或者省）的数据，且row_workplace与row_enterprise省的值是相等的，则返回row_enterprise
        elif row_workplace['province'] == row_enterprise['province']:
            return row_enterprise
        #如果row_enterprise有省的值，或者有省，市（省，市，区）的值但是与workplace的省值不一致，则返回row_workplace
        else:
            return row_workplace
    #如果row_workplace的三列都为异常值，则直接返回row_enterprise
    else:
        return row_enterprise

df_jobs = pd.read_excel('Q1_jobList.xlsx', dtype={'provinceCode': str, 'cityCode': str, 'regionCode': str})

# enterprise_loc = get_enterpriseLoc(df_jobs)
work_loc = get_workLoc(df_jobs)
work_place = work_loc[['work_province', 'work_city', 'work_region']]
work_place.columns = ['province', 'city', 'region']
enterprise_place = df_jobs[['province', 'city', 'region']]

#合并workplace和enterprise_place为fixed_place
df_fixedPlace = pd.DataFrame([merge_rows(work_place.iloc[i], enterprise_place.iloc[i]) for i in range(len(work_place))])
print(df_fixedPlace.head())
print(df_fixedPlace.tail())
df_fixedPlace.to_excel('fixedPlace1.xlsx')

df_fixedPlace['id'] = df_jobs['id']
df_fixedPlace.fillna('', inplace=True)

#将更新新的数据写入数据库：

# 建立数据库连接
db_config = {
    'host': '192.168.123.243',
    'user': 'remoteuser',
    'password': 'Xysan955.',
    'database': 'jobList'
}
connection = pymysql.connect(**db_config)

cursor = connection.cursor()

sql = "INSERT IGNORE INTO fixed_places (id, fixed_province, fixed_city, fixed_region) VALUES (%s, %s, %s, %s)"
values = df_fixedPlace[['id', 'province', 'city', 'region']].values.tolist()

cursor.executemany(sql, values)
connection.commit()

# 关闭数据库连接
cursor.close()
connection.close()

print('地址已经调整完毕，并且加入到数据库！')

