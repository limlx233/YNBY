# 数据处理函数封装功能
import numpy as np
import pandas as pd
from datetime import datetime

def calculate_expiry(df, date_value):
    df = df.copy()
    # 确保日期列是 datetime 类型
    df['失效日期'] = pd.to_datetime(df['失效日期'])
    df['生产日期'] = pd.to_datetime(df['生产日期'])
    date_value = pd.to_datetime(date_value)
    # 计算效期（失效日期 - 生产日期）
    df.loc[:,'效期'] = (df['失效日期'] - df['生产日期']).dt.days
    # 计算剩余效期（失效日期 - 日期值）
    df.loc[:,'剩余效期'] = (df['失效日期'] - date_value).dt.days
    # 计算效期占比（剩余效期 / 效期）
    df.loc[:,'效期占比'] = df['剩余效期'] / df['效期']
    # 将 '失效日期' 和 '生产日期' 转换为字符串格式 "YYYY-MM-DD"
    df.loc[:,'失效日期'] = df['失效日期'].dt.strftime('%Y-%m-%d')
    df.loc[:,'生产日期'] = df['生产日期'].dt.strftime('%Y-%m-%d')
    return df

def warehouse_filtering(df,wl):
    df_wl = df[df['仓库'].isin(wl)].copy()
    return  df_wl

def cp_warehouse_filtering(df,cp):
    df_cp = df[df['仓库'].str.contains('总库|研发成品库', na=False) & (~df['仓库'].isin(cp))]
    return df_cp

def expiry_classification(df):
    df = df.copy()
    # 定义效期分类的条件
    conditions = [
        (df['效期占比'] <= 0),
        (df['效期占比'] <= 1/3) & (df['效期占比'] > 0),
        (df['效期占比'] > 1/3) & (df['效期占比'] <= 2/3),
        (df['效期占比'] > 2/3)
    ]
    # 定义分类值
    choices = ["过效期", "剩余1/3效期", "剩余2/3效期", ""]
    # 使用 numpy 的 select 方法进行分类
    df.loc[:,'效期分类'] = np.select(conditions, choices, default="")
    return df

def receive_classification(df, date_value):
    df = df.copy()
    # 确保日期列是 datetime 类型
    df['最近事务处理时间'] = pd.to_datetime(df['最近事务处理时间'])
    date_value = pd.to_datetime(date_value)
    # 计算最近事务处理时间与日期值的天数差
    df.loc[:,'领用分类'] = (date_value - df['最近事务处理时间'] ).dt.days.apply(lambda x: "90天内无领用" if x >= 90 else "")
    return df

def storage_days_classification(df):
    df = df.copy()
    # 添加新列 '在库天数分类'
    df.loc[:,'在库天数分类'] = df['在库天数'].apply(lambda x: "≥180天" if x >= 180 else "")
    return df

def classify_items(df):
    # 确保 DataFrame 是副本，避免 SettingWithCopyWarning
    df = df.copy()
    # 初始化新列 '分类' 为空字符串
    df.loc[:, '分类'] = ""
    # 定义各个条件的判断函数
    def assign_classification(row):
        classification = []
        if row['效期分类'] == '过效期':
            classification.append('过期货')
        elif row['效期分类'] == '剩余1/3效期':
            classification.append('临期货')
        elif row['效期分类'] == '剩余2/3效期' and row['领用分类'] =="" and row['在库天数分类'] =="":
            classification.append('预警货')
        elif row['效期分类'] == '剩余2/3效期' or row['领用分类'] == '90天内无领用' or row['在库天数分类'] == '≥180天':
            classification.append('呆滞品')
        # 将分类结果用逗号隔开连接成字符串
        return ', '.join(classification)
    # 应用分类函数
    df.loc[:, '分类'] = df.apply(assign_classification, axis=1)
    return df

def reorder_columns(df, columns_to_front):
    # 确保 DataFrame 是副本，避免 SettingWithCopyWarning
    df = df.copy()
    # 获取所有列的名称
    all_columns = df.columns.tolist()
    # 检查指定的列是否存在于 DataFrame 中
    for col in columns_to_front:
        if col not in all_columns:
            raise ValueError(f"列 '{col}' 不在 DataFrame 中")
    # 将指定的列移到前面
    new_order = columns_to_front + [col for col in all_columns if col not in columns_to_front]
    # 重新排列列的顺序
    df = df[new_order]
    return df

def generate_description_df():
    data_list = [
                    ['库存物料说明',None],
                    ['1.库存调取时间', '2024年12月30日 上午9点'],
                    ['2.库存组织', 'JKYZ00.健康牙膏智能制造中心   JKCP:健康产品公司   JKRH00.健康日化制造中心'],
                    ['3.库存数据来源', 'EBS库存：CUX.现有量/可用量查询（XML报表）'],
                    ['4.呆滞品', '（1）物料调取维度：以当前库存在库时长≥180天物料'],
                    ['4.呆滞品', '（2）物料调取其他条件：以最后一次事物处理时间为基础，筛选出90天内无领用物料'],
                    ['4.呆滞品', '（3）外协单元存货调取维度：以当前库存在库时长≥30天物料'],
                    ['5.预警货', '以当前库存物料在库失效日期为准，剩余三分之二效期物料'],
                    ['6.临期货', '以当前库存物料在库失效日期为准，剩余三分之一效期物料'],
                    ['7.过期货', '以当前库存物料在库失效日期为准，超过失效日期物料'],
                    ['8.无系统账物料', '存放于经开区厂区仓库无系统账物料情况']
                ]
    current_month_first_day = datetime.now().replace(day=1, hour=8, minute=30, second=0, microsecond=0)
    data_list[1][1] = current_month_first_day.strftime('%Y年%m月%d日 上午%I点%M分')
    df2 = pd.DataFrame(data_list)
    return df2