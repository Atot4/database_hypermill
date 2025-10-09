import streamlit as st
import pandas as pd
import sqlite3
from sqlite3 import Error
import uuid


USED_TOOL_DATABASE = 'tool_database/demo.db'
USED_MATERIALS_DATABASE = 'tool_database/materials.db'

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
        'Materials': 'SELECT * FROM Materials',
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
    database = USED_TOOL_DATABASE
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

        # Mengambil data material dari tabel Materials
        df_material = dict_dataframe['Materials']
        st.dataframe(df_material)


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


def get_material_db():
    database = USED_MATERIALS_DATABASE
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

        # =================================================================
        # START: PERBAIKAN TIPE DATA (MENGATASI ARROWINVALID)
        # =================================================================
        
        # Daftar kolom ID yang rentan salah dibaca sebagai binary/object
        cols_to_convert_to_int = [
            'chipping_class', # Kolom yang di-error-kan PyArrow (asumsi nama akhirnya)
            'milling_chipping_class_id',
            'chipping_class_id',
            # Tambahkan kolom lain yang seharusnya ID/INTEGER
        ]
        
        for col in cols_to_convert_to_int:
            if col in df_material_database.columns:
                try:
                    # 1. pd.to_numeric: Mengubah ke angka. errors='coerce' mengganti data non-angka (seperti bytes yang aneh) menjadi NaN.
                    # 2. fillna(0): Mengisi NaN (termasuk NULL dari DB) dengan 0.
                    # 3. astype('int64'): Mengkonversi hasil akhirnya menjadi tipe integer 64-bit yang aman.
                    df_material_database[col] = pd.to_numeric(
                        df_material_database[col], 
                        errors='coerce'
                    ).fillna(0).astype('int64')
                except Exception as e:
                    # Jika konversi integer benar-benar gagal (sangat jarang), ubah ke string sebagai fallback
                    df_material_database[col] = df_material_database[col].astype(str)

        # =================================================================
        # END: PERBAIKAN TIPE DATA
        # =================================================================
        st.markdown(f"# df_material_database")
        st.dataframe(df_material_database)
        return df_material_database
        
    else:
        st.warning("Tidak dapat membuat koneksi database. Periksa nama database.")

def create_new_material():
    df_material_database = get_material_db()
    database = USED_TOOL_DATABASE
    conn = create_connection(database)

    if not df_material_database.empty:
        selected_group = st.selectbox(
        'Select Material Group',
        df_material_database['name_of_MaterialGroup'].unique()
        )

        df_filtered_material_group = df_material_database[
        df_material_database['name_of_MaterialGroup'] == selected_group
        ]

        selected_sub_group = st.selectbox(
        'Select Material Sub Group',
        df_filtered_material_group['name_of_df4'].unique()
        )

        df_filtered_material_sub_group = df_filtered_material_group[
        df_filtered_material_group['name_of_df4'] == selected_sub_group
        ]

        selected_quality = st.selectbox(
        'Select Quality',
        df_filtered_material_sub_group['name_of_Qualities'].unique()
        )

        df_filtered_quality = df_filtered_material_sub_group[
        df_filtered_material_sub_group['name_of_Qualities'] == selected_quality
        ].copy()

        df_filtered_quality['hardness_hb_max'] = df_filtered_quality['hardness_hb_max'].astype(str)
        df_filtered_quality['hardness_hrc_max'] = df_filtered_quality['hardness_hrc_max'].astype(str)

        df_filtered_quality['material_name'] = df_filtered_quality.apply(
            lambda row: f"{row['material_no']} | {row['jis_name']} | {row['din_name']} | HB Max: {row['hardness_hb_max']} | HRC Max: {row['hardness_hrc_max']}", 
            axis=1
        )

        selected_material = st.selectbox(
        'Select Material',
        df_filtered_quality['material_name'].unique()
        )

        df_filtered_material = df_filtered_quality[
        df_filtered_quality['material_name'] == selected_material
        ]

        df_new_material = df_filtered_material.copy()
        st.dataframe(df_new_material)
        material_comment = st.text_input("Comment")
        submitted = st.button("Save Material")

    if submitted:
        if df_new_material.empty:
          st.error("Not Found a new material")
        else:
            try:
                cursor = conn.cursor()

                row_data = df_new_material.iloc[0]

                parent_name = row_data['name_of_df1']
                # child_name = row_data['jis_name']
                material_no = row_data['material_no']
                din_name = row_data['din_name']
                afnor_name = row_data['afnor_name']
                bs_name = row_data['bs_name']
                uni_name = row_data['uni_name']
                csn_name = row_data['csn_name']
                jis_name = row_data['jis_name']
                ss_name = row_data['ss_name']
                gost_name = row_data['gost_name']
                uns_name = row_data['uns_name']
                aisi_name = row_data['aisi_name']
                trademark_1 = row_data['trademark_1']
                trademark_2 = row_data['trademark_2']
                trademark_3 = row_data['trademark_3']
                chipping_class_id = row_data['chipping_class_id']
                

                list_of_names = [jis_name, din_name, aisi_name, afnor_name, bs_name, uni_name, csn_name, ss_name, gost_name, uns_name]
                child_name = next((name for name in list_of_names if name), parent_name)
                
                list_of_trademarks = [trademark_1, trademark_2, trademark_3]
                trademark = next((t_mark for t_mark in list_of_trademarks if t_mark), None) 

                # 1. PERIKSA KEBERADAAN PARENT (name_of_df1)
                cursor.execute("SELECT id FROM Materials WHERE name = ?", (parent_name,))
                existing_parent_id = cursor.fetchone() 

                # 2. PERIKSA KEBERADAAN CHILD (jis_name)
                cursor.execute("SELECT id FROM Materials WHERE name = ?", (child_name,))
                existing_child_id = cursor.fetchone()

                # =================================================================
                # Logika Pengecekan
                # ================================================================= 
                if existing_child_id:
                    st.info(f"Data material (Child) dengan nama '{child_name}' sudah ada di database. Tidak ada data yang disimpan.")
                elif existing_parent_id:
                    parent_id_to_use = existing_parent_id[0]

                    cursor.execute("SELECT MAX(id) FROM Materials")
                    max_id = cursor.fetchone()[0]
                    new_child_id = (max_id if max_id is not None else 0) + 1

                    child_guid_uuid = uuid.uuid4()
                    child_obj_guid_blob = child_guid_uuid.bytes
                    placeholder_guid_blob = b'\x00' * 16

                    sql_insert = """
                    INSERT INTO Materials (
                    id, type, name, norm_code, comment, obj_guid, parent_id, mat_db_obj_guid, chipping_class,
                    milling_factor_vc, milling_factor_fz, milling_factor_ae, milling_factor_ap,
                    drilling_factor_vc, drilling_factor_fz, insert_factor_vc, insert_factor_fz, insert_factor_ae, insert_factor_ap
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params_child = (
                    new_child_id, 2, child_name, "", material_comment, child_obj_guid_blob, parent_id_to_use, placeholder_guid_blob, row_data['chipping_class_id'],
                    row_data['milling_factor_vc'], row_data['milling_factor_fz'], row_data['milling_factor_ae'], row_data['milling_factor_ap'],
                    row_data['drilling_factor_vc'], row_data['drilling_factor_fz'], row_data['insert_factor_vc'], row_data['insert_factor_fz'],
                    row_data['insert_factor_ae'], row_data['insert_factor_ap']
                    )

                    cursor.execute(sql_insert, params_child)
                    conn.commit()
                    st.success(f"Material Child '{child_name}' berhasil disimpan. Dihubungkan ke Parent ID yang sudah ada: {parent_id_to_use}! chipping_class {chipping_class_id}")
                else:
                    cursor.execute("SELECT MAX(id) FROM Materials")
                    max_id = cursor.fetchone()[0]
                    new_id = (max_id if max_id is not None else 0) + 1
                    parent_guid_uuid = uuid.uuid4()
                    parent_obj_guid_blob = parent_guid_uuid.bytes
                    child_guid_uuid = uuid.uuid4()
                    child_obj_guid_blob = child_guid_uuid.bytes
                    placeholder_guid_blob = b'\x00' * 16

                    sql_insert = """
                    INSERT INTO Materials (
                    id, type, name, norm_code, comment, obj_guid, parent_id, mat_db_obj_guid, chipping_class,
                    milling_factor_vc, milling_factor_fz, milling_factor_ae, milling_factor_ap,
                    drilling_factor_vc, drilling_factor_fz, insert_factor_vc, insert_factor_fz, insert_factor_ae, insert_factor_ap
                    ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?
                    )
                    """
                    params = [
                    (new_id, 1, row_data['name_of_df1'], "", "Parent Class", parent_obj_guid_blob, None, placeholder_guid_blob, row_data['chipping_class_id'],
                    row_data['milling_factor_vc'], row_data['milling_factor_fz'], row_data['milling_factor_ae'], row_data['milling_factor_ap'],
                    row_data['drilling_factor_vc'], row_data['drilling_factor_fz'], row_data['insert_factor_vc'], row_data['insert_factor_fz'],
                    row_data['insert_factor_ae'], row_data['insert_factor_ap']
                    ),
                    (new_id + 1, 2, child_name, "", material_comment, child_obj_guid_blob, new_id, placeholder_guid_blob, row_data['chipping_class_id'],
                    row_data['milling_factor_vc'], row_data['milling_factor_fz'], row_data['milling_factor_ae'], row_data['milling_factor_ap'],
                    row_data['drilling_factor_vc'], row_data['drilling_factor_fz'], row_data['insert_factor_vc'], row_data['insert_factor_fz'],
                    row_data['insert_factor_ae'], row_data['insert_factor_ap']
                    )
                    ]

                    cursor.executemany(sql_insert, params)
                    conn.commit()
                    # Use material_data instead of row_data for better context
                    st.success(f"Material '{row_data['name_of_df1']}' successfully saved with ID {new_id}! chipping_class {chipping_class_id}")

            except Exception as e:
                # Translated error message
                st.error(f"Error while saving data: {e}") 
            finally:
                if conn:
                    # Translated success message
                    conn.close()



def main():
    pilihan_tabel = st.sidebar.radio('Select a Menu:', (
            'Database',
            'Create Raw Material'
        ))
    
    if pilihan_tabel == "Database":
        get_tool_database()
    elif pilihan_tabel == 'Create Raw Material':
        create_new_material()


    
    

if __name__ == '__main__':
    main()