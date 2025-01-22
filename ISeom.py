import calendar
import pandas as pd
from io import BytesIO
import streamlit as st
import dataprocess as dp  # 根据实际处理需求 编写的数据处理模块
import concurrent.futures
from datetime import date
from openpyxl.styles import Font, Alignment, PatternFill,NamedStyle


# 使用缓存来存储处理后的 DataFrame
@st.cache_data
def load_excel_files(uploaded_files):
    dfs = {}
    for uploaded_file in uploaded_files:
        df = pd.read_excel(uploaded_file, header=17)
        dfs[uploaded_file.name] = df
    return dfs

# 使用缓存来存储处理后的数据
@st.cache_data
def process_data(dfs):
    df_lst = []
    for df in dfs.values():
        df = df.dropna(subset=["生产日期", "失效日期"], inplace=False)
        df_lst.append(df)
    df_all = pd.concat(df_lst, axis=0)
    return df_all

# 将 Pandas DataFrame 对象转换为 Excel 文件格式的字节流
@st.cache_resource
def to_excel(df1, df3 ,df2,sheet_name1='物料',sheet_name3='成品',sheet_name2='说明'):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='openpyxl')
    df2.to_excel(writer, index=False, header=False,sheet_name=sheet_name2)
    df1.to_excel(writer, index=False, sheet_name=sheet_name1)
    df3.to_excel(writer, index=False, sheet_name=sheet_name3)
    # 获取工作簿和工作表
    workbook = writer.book
    worksheet1 = writer.sheets[sheet_name1]
    worksheet2= writer.sheets[sheet_name2]
    worksheet3= writer.sheets[sheet_name3]
    # 调用格式设置函数
    set_description_sheet_format(worksheet2, df2)
    set_material_sheet_format(worksheet1, df1)
    set_material_sheet_format(worksheet3, df3)
    writer.close()
    processed_data = output.getvalue()
    return processed_data

def set_material_sheet_format(worksheet, df1):
    # 设置列宽
    for idx, column in enumerate(worksheet.columns, start=1):
        column_letter = column[0].column_letter
        if idx == 1:
            worksheet.column_dimensions[column_letter].width = 12
        elif idx == 5:
            worksheet.column_dimensions[column_letter].width = 24
        else:
            worksheet.column_dimensions[column_letter].width = 12
    
    # 定义百分比格式
    percentage_style = NamedStyle(name="percentage_style")
    percentage_style.number_format = '0.00%'
    
    # 应用格式到指定列
    column_name = '效期占比'
    column_index = df1.columns.get_loc(column_name) + 1  # +1 是因为 Excel 列索引从 1 开始
    for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row, min_col=column_index, max_col=column_index):
        for cell in row:
            cell.style = percentage_style
    
    # 设置标题样式
    for cell in worksheet[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="346c9c", end_color="346c9c", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center")
    
    # 设置数据居中对齐
    for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row, min_col=1, max_col=worksheet.max_column):
        for cell in row:
            cell.alignment = Alignment(horizontal="center", vertical="center")

def set_description_sheet_format(worksheet, df2):
    # 设置列宽
    worksheet.column_dimensions['A'].width = 22
    worksheet.column_dimensions['B'].width = 108
    
    # 设置行高
    for row in range(1, 12):  # 1~11行行高设置为36
        worksheet.row_dimensions[row].height = 36
    
    # 设置标题样式
    for cell in worksheet[1]:
        cell.font = Font(bold=True, size=16)
        cell.alignment = Alignment(horizontal="center", vertical="center")
    
    # 设置剩余行的标题样式
    for row in range(2, worksheet.max_row + 1):  # 从第二行开始
        for cell in worksheet[row]:
            cell.font = Font(size=14, color="000000")
            cell.alignment = Alignment(horizontal="left", vertical="center")
    
    # 合并单元格
    worksheet.merge_cells('A1:B1')  # 合并A1和B1
    worksheet.merge_cells('A5:A7')  # 合并A5到A7
    
    # 设置合并单元格的对齐方式
    worksheet['A1'].alignment = Alignment(horizontal="center", vertical="center")
    worksheet['A5'].alignment = Alignment(horizontal="center", vertical="center")
    
    # 保证样式设置不会被合并操作覆盖
    for cell in worksheet['A1:B1'][0]:
        cell.font = Font(bold=True, size=16)
        cell.alignment = Alignment(horizontal="center", vertical="center")

    for cell in worksheet['A5:A7'][0]:
        cell.font = Font(size=14, color="000000")
        cell.alignment = Alignment(horizontal="left", vertical="center")

# 页面设置
st.set_page_config(page_title="数据处理工具", page_icon=":material/home:", layout='centered')

with st.container(border=True):
    st.header('每月物料库存数据处理', divider="rainbow")
    st.subheader('月末日期选择', divider='grey')
    def get_last_day_of_previous_month():
        today = date.today()
        if today.month == 1:
            year = today.year - 1
            month = 12
        else:
            year = today.year
            month = today.month - 1
        _, last_day = calendar.monthrange(year, month)
        return date(year, month, last_day)
    
    # 默认日期为上月最后一天
    default_date = get_last_day_of_previous_month()
    # 日期输入控件
    date_value = st.date_input(label="请选择日期,(默认为上月的最后一天)", value=default_date)
    # st.write(date_value)
    st.subheader('数据文件上传', divider='grey')
    # 多文件上传
    uploaded_files = st.file_uploader(label="请选择Excel文件(.xlsx格式)上传", accept_multiple_files=True, type=["xlsx"])
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button(label="数据处理", type="primary", key="data_process"):
            if uploaded_files:
                # 读取文件并缓存
                dfs = load_excel_files(uploaded_files)
                columns_to_keep = st.secrets["warehouses"]["columns_to_keep"]
                # 并行处理数据
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    df_lst = list(executor.map(lambda df: df.dropna(subset=["生产日期", "失效日期"], inplace=False)[columns_to_keep], dfs.values()))
                df_all = pd.concat(df_lst, axis=0)
                # 数据处理--物料
                wl = st.secrets["warehouses"]["wl"]
                df_wl1 = dp.warehouse_filtering(df_all,wl) # 筛选仓库
                df_wl1 = dp.calculate_expiry(df_wl1, date_value) # 效期计算
                df_wl1 = dp.expiry_classification(df_wl1) # 效期分类
                df_wl1 = dp.receive_classification(df_wl1,date_value) # 领用时间分类
                df_wl1 = dp.storage_days_classification(df_wl1) # 在库时间分类
                df_wl1 = df_wl1[~((df_wl1['效期分类'] == '' ) & (df_wl1['领用分类'] == '' ) & (df_wl1['在库天数分类'] == ''))]
                df_wl1 = dp.classify_items(df_wl1)
                # 重新对列排序
                cloumns_to_front = ['分类','效期','剩余效期','效期占比','所属组织','效期分类','领用分类','在库天数分类']
                df_wl1 = dp.reorder_columns(df_wl1, cloumns_to_front)
                # 数据处理-成品
                cp = st.secrets["warehouses"]["cp_warehouses"]
                df_cp1 = dp.cp_warehouse_filtering(df_all,cp)
                df_cp1 = dp.calculate_expiry(df_cp1, date_value) # 效期计算
                df_cp1 = dp.expiry_classification(df_cp1) # 效期分类
                df_cp1 = dp.receive_classification(df_cp1,date_value) # 领用时间分类
                df_cp1 = dp.storage_days_classification(df_cp1) # 在库时间分类
                df_cp1 = df_cp1[~((df_cp1['效期分类'] == '' ) & (df_cp1['领用分类'] == '' ) & (df_cp1['在库天数分类'] == ''))]
                df_cp1 = dp.classify_items(df_cp1)
                # 重新对列排序
                cloumns_to_front = ['分类','效期','剩余效期','效期占比','所属组织','效期分类','领用分类','在库天数分类']
                df_cp1 = dp.reorder_columns(df_cp1, cloumns_to_front) 
                # st.write(df_all.shape)
                # st.data_editor(df_wl1)
                # 生成 Excel 文件
                df2 = dp.generate_description_df()
                excel_file = to_excel(df_wl1,df_cp1,df2)
                st.session_state.excel_file = excel_file
            else:
                st.info("请先上传数据文件!")
    with col2:
        if 'excel_file' in st.session_state:
            st.download_button(
                label="下载文件",
                data=st.session_state.excel_file,
                type="primary",
                file_name="月末库存呆滞情况.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("请先上传数据并进行数据处理。")