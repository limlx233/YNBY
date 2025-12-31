# 数据处理函数封装功能
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
# 过滤 openpyxl 的所有 UserWarning 警告
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


def calculate_expiry(df, date_value):
    df = df.copy()
    # 确保日期列是 datetime 类型
    df['失效日期'] = pd.to_datetime(df['失效日期'])
    df['生产日期'] = pd.to_datetime(df['生产日期'])
    date_value = pd.to_datetime(date_value)
    # 计算效期（失效日期 - 生产日期）
    df.loc[:,'效期'] = (df['失效日期'] - df['生产日期']).dt.days
    # 计算剩余效期（失效日期 - 日期值）
    df.loc[:,'剩余效期天数'] = (df['失效日期'] - date_value).dt.days
    # 计算效期占比（剩余效期 / 效期）
    df.loc[:,'%(剩余效期/总效期)'] = df['剩余效期天数'] / df['效期']
    # 将 '失效日期' 和 '生产日期' 转换为字符串格式 "YYYY-MM-DD"
    df.loc[:,'失效日期'] = df['失效日期'].dt.strftime('%Y-%m-%d')
    df.loc[:,'生产日期'] = df['生产日期'].dt.strftime('%Y-%m-%d')
    return df

def warehouse_filtering(df,wl):
    df_wl = df[df['仓库代码'].isin(wl)].copy()
    return  df_wl

def cp_warehouse_filtering(df,cp,cp_filter):
    df_cp = df[df['仓库代码'].isin(cp_filter) & (~df['仓库代码'].isin(cp))]
    return df_cp

def expiry_classification(df):
    df = df.copy()
    # 定义效期类别的条件
    conditions = [
        (df['%(剩余效期/总效期)'] <= 0),
        (df['%(剩余效期/总效期)'] <= 1/3) & (df['%(剩余效期/总效期)'] > 0),
        (df['%(剩余效期/总效期)'] > 1/3) & (df['%(剩余效期/总效期)'] <= 2/3),
        (df['%(剩余效期/总效期)'] > 2/3)
    ]
    # 定义分类值
    choices = ["过效期", "剩余1/3效期", "剩余2/3效期", ""]
    # 使用 numpy 的 select 方法进行分类
    df.loc[:,'效期类别'] = np.select(conditions, choices, default="")
    return df

def receive_classification(df, date_value):
    df = df.copy()
    # 确保日期列是 datetime 类型
    df['最近事务处理时间'] = pd.to_datetime(df['最近事务处理时间'])
    date_value = pd.to_datetime(date_value)
    # 计算最近事务处理时间与日期值的天数差
    df.loc[:,'90天内无领用'] = (date_value - df['最近事务处理时间'] ).dt.days.apply(lambda x: "90天内无领用" if x >= 90 else "")
    return df

def storage_days_classification(df,cp_wx):
    df = df.copy()
    # 添加新列 '异常在库天数'
    df.loc[:, '异常在库天数'] = np.where(
    (df['仓库代码'].isin(cp_wx)) & (df['在库天数'] >= 30),
    "≥30天",
    np.where(df['在库天数'] >= 180, "≥180天", ""))
    return df

def classify_items(df):
    # 确保 DataFrame 是副本，避免 SettingWithCopyWarning
    df = df.copy()
    # 初始化新列 '分类' 为空字符串
    df.loc[:, '分类'] = ""
    # 定义各个条件的判断函数
    def assign_classification(row):
        classification = []
        if row['效期类别'] == '过效期':
            classification.append('过期货')
        elif row['90天内无领用'] == '90天内无领用':
            classification.append('呆滞品1')
        elif row['异常在库天数'] == '≥180天':
            classification.append('呆滞品2')
        elif row['异常在库天数'] == '≥30天':
            classification.append('呆滞品3')
        elif row['效期类别'] == '剩余1/3效期' and row['90天内无领用'] =="" and row['异常在库天数'] =="" :
            classification.append('临期货')
        elif row['效期类别'] == '剩余2/3效期' and row['90天内无领用'] =="" and row['异常在库天数'] =="":
            classification.append('预警货')
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
                    ['说明',None],
                    ['1.库存调取时间', '2024年12月30日 上午9点','优先级'],
                    ['2.库存组织', 'JKYZ00.健康牙膏智能制造中心   JKCP:健康产品公司   JKRH00.健康日化制造中心','--'],
                    ['3.库存数据来源', 'EBS库存：CUX.现有量/可用量查询（XML报表）','--'],
                    ['4.过期货', '以当前库存物料在库失效日期为准，超过失效日期物料',1],
                    ['5.呆滞品1', '物料调取维度：以最后一次事物处理时间为基础，筛选出90天内无领用物料',2],
                    ['6.呆滞品2', '物料调取维度：以当前库存在库时长≥180天物料',3],
                    ['7.呆滞品3', '外协单元存货调取维度：以当前库存在库时长≥30天(外协成品)',4],
                    ['8.临期货', '以当前库存物料在库失效日期为准，剩余三分之一效期物料',5],
                    ['9.预警货', '以当前库存物料在库失效日期为准，剩余三分之二效期物料',6],
                    ['10.无系统账物料', '存放于经开区厂区仓库无系统账物料情况','']
                ]
    current_month_first_day = datetime.now().replace(day=1, hour=8, minute=30, second=0, microsecond=0)
    data_list[1][1] = current_month_first_day.strftime('%Y年%m月%d日 上午%I点%M分')
    df2 = pd.DataFrame(data_list)
    return df2


def sort_and_filter(df):
    category_order = ['过期货', '呆滞品1','呆滞品2','呆滞品3', '临期货', '预警货']
    df['分类'] = pd.Categorical(df['分类'], categories=category_order, ordered=True)
    df = df.sort_values(by='分类')
    cols_to_keep = [
    "分类", "处理方案", "效期类别", "90天内无领用", "异常在库天数", 
    "%(剩余效期/总效期)", "剩余效期天数",  
    "物料说明", "所属组织", "物料编码", "仓库", "现有量(主)", "单位(主)", 
    "在库天数", "批次", "生产日期", "失效日期", "最近事务处理时间", "仓库代码"
]
    df = df[cols_to_keep]
    df = reorder_columns(df, cols_to_keep)
    return df

def add_old_solution(df_new, df_old):
    # 步骤 1: 判断 df_old 是否包含 '处理方案' 列
    if '处理方案' in df_old.columns:
        # 若存在该列，执行原有匹配逻辑
        # 创建查找映射 (物料编码, 批次, 仓库代码) -> 处理方案
        mapping_series = df_old.set_index(['物料编码', '批次', '仓库代码'])['处理方案']
        # 在 df_new 中新增 '上月处理方案' 列并完成匹配
        df_new['上月处理方案'] = df_new.set_index(['物料编码', '批次', '仓库代码']).index.map(mapping_series)
        # 步骤 2: 将新列移动到第 8 列的位置（索引7）
        cols = list(df_new.columns)
        # 移除末尾的 '上月处理方案' 列
        cols.pop()
        # 插入到索引7的位置（对应第8列）
        cols.insert(7, '上月处理方案')
        # 重新排列列顺序
        df_new = df_new[cols]
    else:
        # 若 df_old 不存在 '处理方案' 列，将 df_new 的 '处理方案' 列全部置空
        # 先判断 df_new 是否已有 '处理方案' 列，避免报错
        if '处理方案' in df_new.columns:
            # 置空（可选择空字符串''或None，按需调整）
            df_new['处理方案'] = ''  # 推荐空字符串，兼容性更强；若需None可改为 df_new['处理方案'] = None
        else:
            # 若 df_new 也没有该列，可选择创建并置空（可选，根据你的业务需求决定是否保留此段）
            df_new['处理方案'] = ''
    return df_new


# 新增半产品在库天数
def getWipInventoryDays(df_all, date_value):
    WipInventory = ['XB03', 'XB1', 'B1', 'EP', 'RNB', 'JKRHB']
    df_WipInventory = df_all[df_all['仓库代码'].isin(WipInventory)].copy()
    df_WipInventory['生产日期'] = pd.to_datetime(df_WipInventory['生产日期'])
    date_value = pd.to_datetime(date_value)
    
    # 在第二列（loc=1）插入「处理方案」列
    df_WipInventory.insert(
        loc=1,
        column='处理方案',
        value=None  # 初始值可自定义：''/None/'待处理'等
    )
    
    # 计算在库天数数值
    inventory_days = (date_value - df_WipInventory['生产日期']).dt.days
    # 插入到第三列
    df_WipInventory.insert(
        loc=2,
        column='在库天数(月初日期-生产日期)',
        value=inventory_days
    )
    
    # 按「在库天数」降序排列
    df_WipInventory.sort_values(
        by='在库天数(月初日期-生产日期)',
        ascending=False,
        inplace=True
    )
    
    return df_WipInventory