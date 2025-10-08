import streamlit as st
import pandas as pd
import sqlite3
from sqlite3 import Error
import uuid

st.title("Tool Database Hypermill")

@st.cache_resource
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
def execute_sql(conn, sql_statement, params=None): # <--- Tambahkan parameter params
    """
    Menjalankan perintah SQL pada koneksi yang diberikan (NON-SELECT)
    Menggunakan parameterisasi untuk keamanan (mencegah SQL Injection).
    """
    try:
        c = conn.cursor()
        if params:
            # Menggunakan execute dengan parameter
            c.execute(sql_statement, params) 
        else:
            # Jika tidak ada parameter (misalnya CREATE TABLE), gunakan statement biasa
            c.execute(sql_statement) 
            
        conn.commit()
        # st.success("Perintah SQL berhasil dijalankan.") # Opsional untuk konfirmasi
        return c
    except Error as e:
        # PENTING: Gunakan conn.rollback() jika ada error
        conn.rollback() 
        st.error(f"Saat menjalankan perintah SQL terjadi kesalahan: {e}")
        return None
    
def fetch_tool_database(conn):
    """Mengambil semua data dari tool_database dan mengembalikannya sebagai dictionary of DataFrames."""

    queries = {
        'UsedMaterials': 'SELECT * FROM Materials',
        'NCTools': 'SELECT * FROM NCTools',
        'Tools': 'SELECT * FROM Tools',
        'Folders': 'SELECT * FROM Folders',
        'Holders': 'SELECT * FROM Holders',
        'GeometryClasses': 'SELECT * FROM GeometryClasses',
        'TechnologyPurposes': 'SELECT * FROM TechnologyPurposes',
        'Technologies': 'SELECT * FROM Technologies',
        'ToolTechnologies': 'SELECT * FROM ToolTechnologies',
    }

    dataframes = {}
    try:
        for name, query in queries.items():
            dataframes[name] = pd.read_sql_query(query, conn)
        return dataframes

    except pd.io.sql.DatabaseError as e:
        st.error(f"Gagal mengeksekusi kueri. Pastikan semua tabel ada. Error: {e}")
        return None
    
def fetch_material_database(conn):
    """Mengambil semua data materials.db dan mengembalikannya sebagai dictionary of DataFrames."""

    queries = {
        'ChippingClasses': 'SELECT * FROM ChippingClasses',
        'MaterialGroups': 'SELECT * FROM MaterialGroups',
        'MaterialSubGroups': 'SELECT * FROM MaterialSubGroups',
        'Materials': 'SELECT * FROM Materials',
        'Qualities': 'SELECT * FROM Qualities'
    }

    dataframes = {}
    try:
        for name, query in queries.items():
            dataframes[name] = pd.read_sql_query(query, conn)
        return dataframes
    except pd.io.sql.DatabaseError as e:
        st.error(f"Gagal mengeksekusi kueri. Pastikan semua tabel ada. Error: {e}")
        return None 

def get_tool_database():
    database = 'tool_database/demo.db'
    conn = create_connection(database)
    
    # 1. Pastikan koneksi berhasil dibuat
    if conn is not None:
        try:
            dict_dataframe = fetch_tool_database(conn)
        finally:
            conn.close()

        if dict_dataframe is None:
            st.error("Gagal memuat data dari database. Silakan periksa pesan error di atas.")
            return

        # # --- Hanya Tampilkan Materials Data untuk Debugging Awal ---
        # st.subheader("Data Tabel Materials")
        # if 'Materials' in dict_dataframe:
        #     # Menggunakan .sort_values untuk menampilkan data terbaru di atas
        #     st.dataframe(dict_dataframe['Materials'].sort_values('id', ascending=False), use_container_width=True)
            
        # # ----------------------------------------------------------- 

        # Menggabungkan semua dataframe
        # ===============================================================================
        # Merge Nc Tools dan Holders
        df1 = pd.merge(              
            dict_dataframe['NCTools'],
            dict_dataframe['Holders'],
            left_on='holder_id',
            right_on='id',
            how='left',
            suffixes=('_of_nc_tools', '_of_holders')
        )

        # Merge df1 & Tools
        df2 = pd.merge(
            df1,
            dict_dataframe['Tools'],
            left_on='tool_id',
            right_on='id',
            how='left',
            suffixes=('_of_df1', '_of_tools')
        )

        # Merge df2 & Tool Technologies
        df3 = pd.merge(
            df2,
            dict_dataframe['ToolTechnologies'],
            left_on='tool_id',
            right_on='technology_id',
            how='left',
            suffixes=('_of_df2', '_of_tool_technologies')
        )

        # Merge df3 & Geometry Class
        df4 = pd.merge(
            df3,
            dict_dataframe['GeometryClasses'],
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
            dict_dataframe['Technologies'],
            left_on='technology_id',
            right_on='technology_id',
            how='left',
            suffixes=('_of_df4', '_of_technologies_data')
        )
        

        # Merge df5 & Technology Purposes
        df6 = pd.merge(
            df5,
            dict_dataframe['TechnologyPurposes'],
            left_on='purpose_id',
            right_on='id',
            how='left',
            suffixes=('_of_df5', '_of_technology_purposes')
        )

        # Menampilkan data

        # pilihan_tabel = st.sidebar.radio('Select Database:', (
        #     'Folders', 'Data Material', 'Tools Data', 'Nc Tools Data', 'Holders'
        # ))

        used_column = ['purpose', 'name_of_tools', 'name_of_geometry_classes', 'total_length', 'tool_length', 'name_of_df1']

        df_complete_data = df6.copy()
        df_complete_data_filtered = df_complete_data[used_column]
        complete_data_column = df_complete_data.columns

        # st.write(complete_data_column)
        # st.dataframe(df_complete_data_filtered)
        # st.dataframe(df_complete_data)

        # st.subheader(f"{pilihan_tabel}")
        # if pilihan_tabel == "Folders":
        #     st.dataframe(df_folders_data)
        # elif pilihan_tabel == "Data Material":
        #     st.dataframe(df_material_data)
        # elif pilihan_tabel == "Tools Data":
        #     st.dataframe(df_tools_data)
        # elif pilihan_tabel == "Nc Tools Data":
        #     st.dataframe(df_nc_tools_data)
        # elif pilihan_tabel == "Holders":
        #     st.dataframe(df_holders_data)    
    else:
        st.warning("Tidak dapat membuat koneksi database. Periksa nama database.")


# def add_material_form(conn):
#     """
#     Menampilkan form Streamlit untuk input data material baru dan menyimpannya ke database.
#     """
#     st.subheader("Input Data Material Baru")
    
#     # Menentukan kolom yang wajib diisi dan yang sering diubah
#     # 'id' akan diurus secara otomatis (PRIMARY KEY)
    
#     with st.form("new_material_form1213"):
#         # Kolom wajib/penting
#         col1, col2 = st.columns(2)
#         with col1:
#             material_name = st.text_input("Nama Material (Wajib)", max_chars=100)
#             material_type = st.selectbox("Tipe (Contoh: 1)", [1, 2, 3]) # Asumsi tipe adalah INTEGER
#         with col2:
#             chipping_class = st.number_input("Chipping Class (0-30)", min_value=0, max_value=30, value=10, step=1)
#             norm_code = st.text_input("Norm Code (Opsional)", max_chars=50)

#         # Kolom Faktor Pemotongan (Sangat penting di Hypermill)
#         st.markdown("##### Faktor Pemotongan (Biasanya 1.0 jika tidak diubah)")
#         col_milling = st.columns(4)
#         milling_vc = col_milling[0].number_input("Milling Factor Vc", value=1.0, format="%.1f")
#         milling_fz = col_milling[1].number_input("Milling Factor Fz", value=1.0, format="%.1f")
#         milling_ae = col_milling[2].number_input("Milling Factor Ae", value=1.0, format="%.1f")
#         milling_ap = col_milling[3].number_input("Milling Factor Ap", value=1.0, format="%.1f")
        
#         # Kolom lainnya (untuk kemudahan, bisa di-set nilai default)
#         comment = st.text_area("Komentar", max_chars=255)

#         submitted = st.form_submit_button("Tambah Material")

#         if submitted:
#             if not material_name:
#                 st.error("Nama Material wajib diisi!")
#             else:
#                 try:
#                     # Ambil ID terbesar saat ini dan tambahkan 1, atau mulai dari 1 jika kosong
#                     cursor = conn.cursor()
#                     cursor.execute("SELECT MAX(id) FROM Materials")
#                     max_id = cursor.fetchone()[0]
#                     new_id = (max_id if max_id is not None else 0) + 1
#                     new_guid_uuid = uuid.uuid4()
#                     new_obj_guid_blob = new_guid_uuid.bytes
#                     placeholder_guid_blob = b'\x00' * 16 
                    
#                     sql_insert = """
#                     INSERT INTO Materials (
#                         id, type, name, norm_code, comment, obj_guid, parent_id, mat_db_obj_guid, chipping_class, 
#                         milling_factor_vc, milling_factor_fz, milling_factor_ae, milling_factor_ap, 
#                         drilling_factor_vc, drilling_factor_fz, insert_factor_vc, insert_factor_fz, insert_factor_ae, insert_factor_ap
#                     ) VALUES (
#                         ?, ?, ?, ?, ?, ?, ?, ?, ?, 
#                         ?, ?, ?, ?, 
#                         ?, ?, ?, ?, ?, ?
#                     )
#                     """
#                     # Gunakan nilai default 1.0 untuk faktor-faktor drilling/insert 
#                     # agar tidak perlu banyak input di form
#                     params = (
#                     new_id, material_type, material_name, norm_code, comment, 
#                     new_obj_guid_blob,      # <--- Menggunakan data BLOB (bytes)
#                     None,                   # parent_id
#                     placeholder_guid_blob,  # <--- Menggunakan placeholder BLOB
#                     chipping_class, 
#                     milling_vc, milling_fz, milling_ae, milling_ap,
#                     1.0, 1.0, 1.0, 1.0, 1.0, 1.0 
#                 )
                    
#                     # Eksekusi perintah SQL
#                     execute_sql(conn, sql_insert, params)
#                     st.success(f"Material **'{material_name}'** berhasil ditambahkan dengan ID: {new_id}!")
#                     # Refresh aplikasi untuk melihat data baru (opsional)
#                     st.rerun() 
                    
#                 except Exception as e:
#                     st.error(f"Gagal menambahkan data: {e}")


@st.cache_resource
def get_material_db():
    database = 'tool_database/materials.db'
    conn = create_connection(database)

    if conn is not None:
        try:
            dict_dataframe = fetch_material_database(conn)
            # add_material_form(conn)
        finally:
            conn.close()
            
        if dict_dataframe is None:
            st.error("Gagal memuat data dari database. Silakan periksa pesan error di atas.")
            return
        
        df1 = pd.merge(              
            dict_dataframe['Materials'],
            dict_dataframe['ChippingClasses'],
            left_on='milling_chipping_class_id',
            right_on='chipping_class_id',
            how='left',
            suffixes=('_of_materials', '_of_chipping_classes')
        )

        df2 = pd.merge(              
            df1,
            dict_dataframe['MaterialGroups'],
            left_on='material_group_id',
            right_on='material_group_id',
            how='left',
            suffixes=('_of_df1', '_of_MaterialGroup')
        )

        df3 = pd.merge(              
            df2,
            dict_dataframe['MaterialSubGroups'],
            left_on='material_sub_group_id',
            right_on='material_sub_group_id',
            how='left',
            suffixes=('_of_df2', '_of_MaterialSubGroup')
        )

        df4 = pd.merge(              
            df3,
            dict_dataframe['Qualities'],
            left_on='quality_id',
            right_on='quality_id',
            how='left',
            suffixes=('_of_df4', '_of_Qualities')
        )

        df_material_database = df4.copy()
        # st.dataframe(df_material_database)
        return df_material_database
        
    else:
        st.warning("Tidak dapat membuat koneksi database. Periksa nama database.")

def create_new_material():
        df_material_database = get_material_db()
        database = 'tool_database/demo.db'
        conn = create_connection(database)

        if df_material_database is not None:
            selected_group = st.selectbox(
                'Select Material Group', 
                df_material_database['name_of_MaterialGroup'].unique()
            )

            df_filtered_material_group = df_material_database[
                df_material_database['name_of_MaterialGroup'] == selected_group
            ].copy()

            selected_sub_group = st.selectbox(
                'Select Material Sub Group',
                df_filtered_material_group['name_of_df4'].unique()
            )
                
            df_filtered_material_sub_group = df_filtered_material_group[
                df_filtered_material_group['name_of_df4'] == selected_sub_group
            ].copy()
        
            selected_quality = st.selectbox(
                'Select Quality',
                df_filtered_material_sub_group['name_of_Qualities'].unique()
            )

            df_filtered_quality = df_filtered_material_sub_group[
                df_filtered_material_sub_group['name_of_Qualities'] == selected_quality
            ].copy()

            df_filtered_quality['material_name'] = (
                df_filtered_quality['jis_name'] + '|' + 
                'HB Max:' + " " + df_filtered_quality['hardness_hb_max'].astype(str) + "|" +
                'HRC Max:' + " " + df_filtered_quality['hardness_hrc_max'].astype(str)
            )

            selected_material = st.selectbox(
                'Select Material',
                df_filtered_quality['material_name'].unique()
            )
            
            df_filtered_material = df_filtered_quality[
                df_filtered_quality['material_name'] == selected_material
            ].copy
            
            df_new_material = df_filtered_material.copy()
            st.dataframe(df_new_material)

            submitted = st.form_submit_button("Save Material")
                
                # if submitted:
                #     if not df_new_material:
                #         st.error("Not Found a new material")
                #     else:
                #         try:
                #             cursor = conn.cursor()
                #             cursor.execute("SELECT MAX(id) FROM Materials")
                #             max_id = cursor.fetchone()[0]
                #             new_id = (max_id if max_id is not None else 0) + 1
                #             new_guid_uuid = uuid.uuid4()
                #             new_obj_guid_blob = new_guid_uuid.bytes
                #             placeholder_guid_blob = b'\x00' * 16 

                #             sql_insert = """
                #             INSERT INTO Materials (
                #                 id, type, name, norm_code, comment, obj_guid, parent_id, mat_db_obj_guid, chipping_class, 
                #                 milling_factor_vc, milling_factor_fz, milling_factor_ae, milling_factor_ap, 
                #                 drilling_factor_vc, drilling_factor_fz, insert_factor_vc, insert_factor_fz, insert_factor_ae, insert_factor_ap
                #             ) VALUES (
                #                 ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                #                 ?, ?, ?, ?, 
                #                 ?, ?, ?, ?, ?, ?
                #             )
                #             """

                #             params = [
                #                 (new_id, 1, df_new_material['name_of_df1'], "", "", new_obj_guid_blob, None, placeholder_guid_blob, df_new_material[])
                #             ]










#         if submitted:
#             if not material_name:
#                 st.error("Nama Material wajib diisi!")
#             else:
#                 try:
#                     # Ambil ID terbesar saat ini dan tambahkan 1, atau mulai dari 1 jika kosong
#                     cursor = conn.cursor()
#                     cursor.execute("SELECT MAX(id) FROM Materials")
#                     max_id = cursor.fetchone()[0]
#                     new_id = (max_id if max_id is not None else 0) + 1
#                     new_guid_uuid = uuid.uuid4()
#                     new_obj_guid_blob = new_guid_uuid.bytes
#                     placeholder_guid_blob = b'\x00' * 16 
                    
#                     sql_insert = """
#                     INSERT INTO Materials (
#                         id, type, name, norm_code, comment, obj_guid, parent_id, mat_db_obj_guid, chipping_class, 
#                         milling_factor_vc, milling_factor_fz, milling_factor_ae, milling_factor_ap, 
#                         drilling_factor_vc, drilling_factor_fz, insert_factor_vc, insert_factor_fz, insert_factor_ae, insert_factor_ap
#                     ) VALUES (
#                         ?, ?, ?, ?, ?, ?, ?, ?, ?, 
#                         ?, ?, ?, ?, 
#                         ?, ?, ?, ?, ?, ?
#                     )
#                     """
#                     # Gunakan nilai default 1.0 untuk faktor-faktor drilling/insert 
#                     # agar tidak perlu banyak input di form
#                     params = (
#                     new_id, material_type, material_name, norm_code, comment, 
#                     new_obj_guid_blob,      # <--- Menggunakan data BLOB (bytes)
#                     None,                   # parent_id
#                     placeholder_guid_blob,  # <--- Menggunakan placeholder BLOB
#                     chipping_class, 
#                     milling_vc, milling_fz, milling_ae, milling_ap,
#                     1.0, 1.0, 1.0, 1.0, 1.0, 1.0 
#                 )
                    
#                     # Eksekusi perintah SQL
#                     execute_sql(conn, sql_insert, params)
#                     st.success(f"Material **'{material_name}'** berhasil ditambahkan dengan ID: {new_id}!")
#                     # Refresh aplikasi untuk melihat data baru (opsional)
#                     st.rerun() 
                    
#                 except Exception as e:
#                     st.error(f"Gagal menambahkan data: {e}")
        

            
            
    



def main():
    pilihan_tabel = st.sidebar.radio('Select a Menu:', (
            'Create Raw Material'
        ))
    
    if pilihan_tabel == 'Create Raw Material':
        create_new_material()


    # get_tool_database()
    

if __name__ == '__main__':
    main()