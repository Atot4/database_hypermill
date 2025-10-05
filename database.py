import streamlit as st
import pandas as pd
import sqlite3
from sqlite3 import Error

st.title("Tool Database Hypermill")

def create_connection(db_file):
    """
    Membuat koneksi ke database SQLite yang namanya ditentukan oleh db_file
    """
    conn = None
    try:
        # Menghilangkan 'print' agar tidak tampil di aplikasi Streamlit
        conn = sqlite3.connect(db_file) 
        return conn
    except Error as e:
        # Menampilkan error di Streamlit jika koneksi gagal
        st.error(f"Terjadi kesalahan saat koneksi database: {e}")
        return None

# Fungsi ini HANYA digunakan untuk perintah NON-SELECT (INSERT, UPDATE, DELETE)
def execute_sql(conn, sql_statement):
    """
    Menjalankan perintah SQL pada koneksi yang diberikan (NON-SELECT)
    """
    try:
        c = conn.cursor()
        c.execute(sql_statement)
        conn.commit()
        # st.success("Perintah SQL berhasil dijalankan.") # Opsional untuk konfirmasi
        return c
    except Error as e:
        st.error(f"Saat menjalankan perintah SQL terjadi kesalahan: {e}")
        return None
    

def main():
    database = 'tool_database2.db'
    conn = create_connection(database)
    
    # 1. Pastikan koneksi berhasil dibuat
    if conn is not None:
        try:
            raw_material_query = 'SELECT * FROM Materials'
            nc_tools_query = 'SELECT * FROM NCTools'
            tools_query = 'SELECT * FROM Tools'
            folders_query = 'SELECT * FROM Folders'
            holders_query = 'SELECT * FROM Holders'
            geometry_classes_query = 'SELECT * FROM GeometryClasses'
            technology_purposes_query = 'SELECT * FROM TechnologyPurposes'
            technologies_query = 'SELECT * FROM Technologies'
            tool_technologies_query = 'SELECT * FROM ToolTechnologies'

            # Menggunakan pd.read_sql_query untuk mengambil data
            df_material_data = pd.read_sql_query(raw_material_query, conn)
            df_nc_tools_data = pd.read_sql_query(nc_tools_query, conn)
            df_tools_data = pd.read_sql_query(tools_query, conn)
            df_folders_data = pd.read_sql_query(folders_query, conn)
            df_holders_data = pd.read_sql_query(holders_query, conn)
            df_geometry_classes_data = pd.read_sql_query(geometry_classes_query, conn)
            df_technology_purposes_data = pd.read_sql_query(technology_purposes_query, conn)
            df_technologies_data = pd.read_sql_query(technologies_query, conn)
            df_tool_technologies_data = pd.read_sql_query(tool_technologies_query, conn)
            
        except pd.io.sql.DatabaseError as e:
            st.error(f"Gagal mengeksekusi kueri. Pastikan tabel 'Materials' ada. Error: {e}")   
        finally:
            conn.close()

        pilihan_tabel = st.sidebar.radio('Select Database:', (
            'Folders', 'Data Material', 'Tools Data', 'Nc Tools Data', 'Holders'
        ))
        # Menggabungkan semua dataframe
        # ===============================================================================
        
        # Merge Nc Tools dan Holders
        df1 = pd.merge(              
            df_nc_tools_data,
            df_holders_data,
            left_on='holder_id',
            right_on='id',
            how='left',
            suffixes=('_of_nc_tools', '_of_holders')
        )

        # Merge df1 & Tools
        df2 = pd.merge(
            df1,
            df_tools_data,
            left_on='tool_id',
            right_on='id',
            how='left',
            suffixes=('_of_df1', '_of_tools')
        )

        # Merge df2 & Tool Technologies
        df3 = pd.merge(
            df2,
            df_tool_technologies_data,
            left_on='tool_id',
            right_on='technology_id',
            how='left',
            suffixes=('_of_df2', '_of_tool_technologies')
        )

        # Merge df3 & Geometry Class
        df4 = pd.merge(
            df3,
            df_geometry_classes_data,
            left_on='tool_type_id',
            right_on='id',
            how='left',
            suffixes=('_of_df3', '_of_geometry_classes')
        )
        df4 = df4.rename(columns={
            'name': 'name_of_geometry_classes'
        })

        # Merge df4 & Technologies
        df5 = pd.merge(
            df4,
            df_technologies_data,
            left_on='technology_id',
            right_on='technology_id',
            how='left',
            suffixes=('_of_df4', '_of_technologies_data')
        )
        

        # Merge df5 & Technology Purposes
        df6 = pd.merge(
            df5,
            df_technology_purposes_data,
            left_on='purpose_id',
            right_on='id',
            how='left',
            suffixes=('_of_df5', '_of_technology_purposes')
        )

        used_column = ['purpose', 'name_of_tools', 'name_of_geometry_classes', 'total_length', 'tool_length', 'name_of_df1']

        df_complete_data = df6.copy()
        df_complete_data_filtered = df_complete_data[used_column]
        complete_data_column = df_complete_data.columns

        st.write(complete_data_column)
        st.dataframe(df_complete_data_filtered)
        st.dataframe(df_complete_data)

        st.subheader(f"{pilihan_tabel}")
        if pilihan_tabel == "Folders":
            st.dataframe(df_folders_data)
        elif pilihan_tabel == "Data Material":
            st.dataframe(df_material_data)
        elif pilihan_tabel == "Tools Data":
            st.dataframe(df_tools_data)
        elif pilihan_tabel == "Nc Tools Data":
            st.dataframe(df_nc_tools_data)
        elif pilihan_tabel == "Holders":
            st.dataframe(df_holders_data)    
    else:
        st.warning("Tidak dapat membuat koneksi database. Periksa nama database.")

if __name__ == '__main__':
    main()