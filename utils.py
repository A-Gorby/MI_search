import pandas as pd
# print(pd.__version__)
import numpy as np
import os, sys, glob
import humanize
import re
import xlrd

import json
import itertools
#from urllib.request import urlopen
#import requests, xmltodict
import time, datetime
import math
from pprint import pprint
import gc
from tqdm import tqdm
tqdm.pandas()
import pickle

import logging
import zipfile
import warnings
import argparse

import warnings
warnings.filterwarnings("ignore")
# import numba
# import numexpr as ne
# numba.set_num_threads(numba.get_num_threads())
import requests
from urllib.parse import urlencode

class Logger():
    def __init__(self, name = 'Fuzzy Lookup',
                 strfmt = '[%(asctime)s] [%(levelname)s] > %(message)s', # strfmt = '[%(asctime)s] [%(name)s] [%(levelname)s] > %(message)s'
                 level = logging.INFO,
                 datefmt = '%H:%M:%S', # '%Y-%m-%d %H:%M:%S'

                 ):
        self.name = name
        self.strfmt = strfmt
        self.level = level
        self.datefmt = datefmt
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.level) #logging.INFO)
        # self.logger.setLevel(logging.NOTSET) #logging.INFO)
        # create console handler and set level to debug
        self.ch = logging.StreamHandler()
        self.ch.setLevel(self.level)
        # create formatter
        self.strfmt = strfmt # '[%(asctime)s] [%(levelname)s] > %(message)s'
        strfmt = '%(asctime)s - %(levelname)s > %(message)s'
        # строка формата времени
        #datefmt = '%Y-%m-%d %H:%M:%S'
        self.datefmt = datefmt # '%H:%M:%S'
        # создаем форматтер
        self.formatter = logging.Formatter(fmt=strfmt, datefmt=datefmt)
        # add formatter to ch
        self.ch.setFormatter(self.formatter)
        # add ch to logger
        self.logger.addHandler(self.ch)
logger = Logger().logger

def save_df_to_excel(df, path_to_save, fn_main, columns = None, b=0, e=None):
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn = fn_main + '_' + str_date + '.xlsx'
    logger.info(fn + ' save - start ...')
    if e is None or (e <0):
        e = df.shape[0]
    if columns is None:
        df[b:e].to_excel(path_to_save + fn, index = False)
    else:
        df[b:e].to_excel(path_to_save + fn, index = False, columns = columns)
    logger.info(fn + ' saved to ' + path_to_save)
    hfs = get_humanize_filesize(path_to_save, fn)
    logger.info("Size: " + str(hfs))
    return fn
def get_humanize_filesize(path, fn):
    human_file_size = None
    try:
        fn_full = os.path.join(path, fn)
    except Exception as err:
        print(err)
        return human_file_size
    if os.path.exists(fn_full):
        file_size = os.path.os.path.getsize(fn_full)
        human_file_size = humanize.naturalsize(file_size)
    return human_file_size
    
def save_df_to_excel(df, path_to_save, fn_main, columns = None, b=0, e=None):
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn = fn_main + '_' + str_date + '.xlsx'
    logger.info(f"'{fn}' save - start ...")
    if e is None or (e <0):
        e = df.shape[0]
    if columns is None:
        df[b:e].to_excel(path_to_save + fn, index = False)
    else:
        df[b:e].to_excel(path_to_save + fn, index = False, columns = columns)
    logger.info(f"'{fn}' saved to '{path_to_save}'")
    hfs = get_humanize_filesize(path_to_save, fn)
    logger.info("Size: " + str(hfs))
    return fn   
def restore_df_from_pickle(path_files, fn_pickle):

    if fn_pickle is None:
        logger.error('Restore pickle from ' + path_files + ' failed!')
        sys.exit(2)
    if os.path.exists(os.path.join(path_files, fn_pickle)):
        df = pd.read_pickle(os.path.join(path_files, fn_pickle))
        # logger.info('Restore ' + re.sub(path_files, '', fn_pickle_с) + ' done!')
        logger.info('Restore ' + fn_pickle + ' done!')
        logger.info('Shape: ' + str(df.shape))
    else:
        # logger.error('Restore ' + re.sub(path_files, '', fn_pickle_с) + ' from ' + path_files + ' failed!')
        logger.error('Restore ' + fn_pickle + ' from ' + path_files + ' failed!')
    return df    

# from utils import unzip_file
def unzip_file(path_source, fn_zip, work_path):
    logger.info('Unzip ' + fn_zip + ' start...')

    try:
        with zipfile.ZipFile(path_source + fn_zip, 'r') as zip_ref:
            fn_list = zip_ref.namelist()
            zip_ref.extractall(work_path)
        logger.info('Unzip ' + fn_zip + ' done!')
        return fn_list[0]
    except Exception as err:
        logger.error('Unzip error: ' + str(err))
        sys.exit(2)

def upload_files_for_fuzzy_search(supp_dict_dir = '/content/data/supp_dict', links = {
    'df_mi_national': {'fn': 'df_mi_national_release_20230201_2023_02_06_1013.zip', 'ya_link': 'https://disk.yandex.ru/d/pfgyT_zmcYrHBw' },
    'df_mi_org_gos' : {'fn': 'df_mi_org_gos_release_20230129_2023_02_14_1759.zip', 'ya_link': 'https://disk.yandex.ru/d/xYolPYsHiSFEWA' },
    'df_mi_org_gos_prod_options': {'fn': 'df_mi_org_gos_prod_options_release_20230201_2023_02_14_1835.zip', 'ya_link': 'https://disk.yandex.ru/d/fnBfPpB8L-mJaw' },
    'dict_embedding_gos_multy' :{'fn': 'dict_embedding_gos_multy.pickle', 'ya_link': 'https://disk.yandex.ru/d/mArd7T-od6NcaQ'},
    'dict_embedding_gos_prod_options_multy': {'fn': 'dict_embedding_gos_prod_options_multy.pickle', 'ya_link': 'https://disk.yandex.ru/d/c2PdgI4JCbnWaA'},
    'dict_embedding_national_multy' : {'fn': 'dict_embedding_national_multy.pickle', 'ya_link': 'https://disk.yandex.ru/d/2qio4quws5IcUQ'},
}):
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    # public_key = link #'https://yadi.sk/d/UJ8VMK2Y6bJH7A'  # Сюда вписываете вашу ссылку

    # Получаем загрузочную ссылку
    for link in tqdm(links.values()):
        final_url = base_url + urlencode(dict(public_key=link['ya_link']))
        
        response = requests.get(final_url)
        download_url = response.json()['href']

        # Загружаем файл и сохраняем его
        download_response = requests.get(download_url)
        # with open('downloaded_file.txt', 'wb') as f:   # Здесь укажите нужный путь к файлу
        with open(os.path.join(supp_dict_dir, link['fn']), 'wb') as f:   # Здесь укажите нужный путь к файлу
            f.write(download_response.content)
            logger.info(f"File '{link['fn']}' uploaded!")
            if link['fn'].split('.')[-1] == 'zip':
                fn_unzip = unzip_file(os.path.join(supp_dict_dir, link['fn']), '', supp_dict_dir)
                logger.info(f"File '{fn_unzip}' upzipped!")    

def load_check_dictionaries_for_fuzzy_search(path_supp_dicts,
      fn_df_mi_national = 'df_mi_national_release_20230201_2023_02_06_1013.pickle',
      fn_df_mi_org_gos ='df_mi_org_gos_release_20230129_2023_02_14_1759.pickle',
      fn_df_mi_org_gos_prod_options ='df_mi_org_gos_prod_options_release_20230201_2023_02_14_1835.pickle',
      fn_dict_embedding_gos_multy = 'dict_embedding_gos_multy.pickle', 
      fn_dict_embedding_gos_prod_options_multy = 'dict_embedding_gos_prod_options_multy.pickle',
      fn_dict_embedding_national_multy = 'dict_embedding_national_multy.pickle',
    ):
    # global df_services_MGFOMS, df_services_804n, df_RM, df_MNN, df_mi_org_gos, df_mi_national, df_mi_org_gos_prod_options
    
    df_mi_org_gos, df_mi_national, df_mi_org_gos_prod_options = None, None, None
    dict_embedding_gos_multy, dict_embedding_national_multy, dict_embedding_gos_prod_options_multy = None, None, None
    
    # fn_df_mi_org_gos = 'df_mi_org_gos_release_20230129_2023_02_07_1331.pickle'
    # fn_df_mi_national = 'df_mi_national_release_20230201_2023_02_06_1013.pickle'
    # fn_df_mi_org_gos ='df_mi_org_gos_release_20230129_2023_02_14_1759.pickle'
    # fn_df_mi_org_gos_prod_options ='df_mi_org_gos_prod_options_release_20230201_2023_02_14_1835.pickle'
    df_mi_org_gos = restore_df_from_pickle(path_supp_dicts, fn_df_mi_org_gos)
    df_mi_national = restore_df_from_pickle(path_supp_dicts, fn_df_mi_national)
    df_mi_org_gos_prod_options = restore_df_from_pickle(path_supp_dicts, fn_df_mi_org_gos_prod_options)
    dict_embedding_gos_multy = restore_df_from_pickle(path_supp_dicts, fn_dict_embedding_gos_multy )
    dict_embedding_national_multy = restore_df_from_pickle(path_supp_dicts, fn_dict_embedding_national_multy )
    dict_embedding_gos_prod_options_multy = restore_df_from_pickle(path_supp_dicts, fn_dict_embedding_gos_prod_options_multy )
    
    
    return df_mi_org_gos, df_mi_national, df_mi_org_gos_prod_options,\
          dict_embedding_gos_multy, dict_embedding_national_multy, dict_embedding_gos_prod_options_multy

def save_to_excel(df_total, total_sheet_names, save_path, fn):
    # fn = model + '.xlsx'
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn_date = fn.replace('.xlsx','')  + '_' + str_date + '.xlsx'
    
    # with pd.ExcelWriter(os.path.join(path_tkbd_processed, fn_date )) as writer:  
    with pd.ExcelWriter(os.path.join(save_path, fn_date )) as writer:  
        
        for i, df in enumerate(df_total):
            df.to_excel(writer, sheet_name = total_sheet_names[i], index=False)
    return fn_date    

def get_humanize_filesize(path, fn):
    human_file_size = None
    try:
        fn_full = os.path.join(path, fn)
    except Exception as err:
        print(err)
        return human_file_size
    if os.path.exists(fn_full):
        file_size = os.path.os.path.getsize(fn_full)
        human_file_size = humanize.naturalsize(file_size)
    return human_file_size
    
def save_df_to_excel(df, path_to_save, fn_main, columns = None, b=0, e=None):
    offset = datetime.timezone(datetime.timedelta(hours=3))
    dt = datetime.datetime.now(offset)
    str_date = dt.strftime("%Y_%m_%d_%H%M")
    fn = fn_main + '_' + str_date + '.xlsx'
    logger.info(f"'{fn}' save - start ...")
    if e is None or (e <0):
        e = df.shape[0]
    if columns is None:
        df[b:e].to_excel(path_to_save + fn, index = False)
    else:
        df[b:e].to_excel(path_to_save + fn, index = False, columns = columns)
    logger.info(f"'{fn}' saved to '{path_to_save}'")
    hfs = get_humanize_filesize(path_to_save, fn)
    logger.info("Size: " + str(hfs))
    return fn         

def np_unique_nan(lst: np.array, debug = False)->np.array: # a la version 2.4
    lst_unique = None
    if lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and np.isnan(lst)):
        # if debug: print('np_unique_nan:','lst is None or (((type(lst)==float) or (type(lst)==np.float64)) and math.isnan(lst))')
        lst_unique = lst
    else:
        data_types_set = list(set([type(i) for i in lst]))
        if debug: print('np_unique_nan:', 'lst:', lst, 'data_types_set:', data_types_set)
        if ((type(lst)==list) or (type(lst)==np.ndarray)):
            if debug: print('np_unique_nan:','if ((type(lst)==list) or (type(lst)==np.ndarray)):')
            if len(data_types_set) > 1: # несколько типов данных
                if list not in data_types_set and dict not in data_types_set and tuple not in data_types_set and type(None) not in data_types_set:
                    lst_unique = np.array(list(set(lst)), dtype=object)
                else:
                    lst_unique = lst
            elif len(data_types_set) == 1:
                if debug: print("np_unique_nan: elif len(data_types_set) == 1:")
                if list in data_types_set:
                    lst_unique = np.unique(np.array(lst, dtype=object))
                elif  np.ndarray in data_types_set:
                    # print('elif  np.ndarray in data_types_set :')
                    lst_unique = np.unique(lst.astype(object))
                    # lst_unique = np_unique_nan(lst_unique)
                    lst_unique = np.asarray(lst, dtype = object)
                    # lst_unique = np.unique(lst_unique)
                elif type(None) in data_types_set:
                    # lst_unique = np.array(list(set(lst)))
                    lst_unique = np.array(list(set(list(lst))))
                elif dict in  data_types_set:
                    lst_unique = lst
                    # np.unique(lst)
                elif type(lst) == np.ndarray:
                    if debug: print("np_unique_nan: type(lst) == np.ndarray")
                    if (lst.dtype.kind == 'f') or  (lst.dtype == np.float64) or  (float in data_types_set):
                        if debug: print("np_unique_nan: (lst.dtype.kind == 'f')")
                        lst_unique = np.unique(lst.astype(float))
                        # if debug: print("np_unique_nan: lst_unique predfinal:", lst_unique)
                        # lst_unique = np.array(list(set(list(lst))))
                        # if debug: print("np_unique_nan: lst_unique predfinal v2:", lst_unique)
                        # if np.isnan(lst).all():
                        #     lst_unique = np.nan
                        #     if debug: print("np_unique_nan: lst_unique predfinal v3:", lst_unique)
                    elif (lst.dtype.kind == 'S') :
                        if debug: print("np_unique_nan: lst.dtype == string")
                        lst_unique = np.array(list(set(list(lst))))
                        if debug: print(f"np_unique_nan: lst_unique 0: {lst_unique}")
                    elif lst.dtype == object:
                        if debug: print("np_unique_nan: lst.dtype == object")
                        if (type(lst[0])==str) or (type(lst[0])==np.str_) :
                            try:
                                lst_unique = np.unique(lst)
                            except Exception as err:
                                lst_unique = np.array(list(set(list(lst))))
                        else:
                            lst_unique = np.array(list(set(list(lst))))
                        if debug: print(f"np_unique_nan: lst_unique 0: {lst_unique}")
                    else:
                        if debug: print("np_unique_nan: else 0")
                        lst_unique = np.unique(lst)
                else:
                    if debug: print('np_unique_nan:','else i...')
                    lst_unique = np.array(list(set(lst)))
                    
            elif len(data_types_set) == 0:
                lst_unique = None
            else:
                # print('else')
                lst_unique = np.array(list(set(lst)))
        else: # другой тип данных
            if debug: print('np_unique_nan:','другой тип данных')
            # lst_unique = np.unique(np.array(list(set(lst)),dtype=object))
            # lst_unique = np.unique(np.array(list(set(lst)))) # Исходим из того что все елеменыт спсика одного типа
            lst_unique = lst
    if type(lst_unique) == np.ndarray:
        if debug: print('np_unique_nan: final: ', "if type(lst_unique) == np.ndarray")
        if lst_unique.shape[0]==1: 
            if debug: print('np_unique_nan: final: ', "lst_unique.shape[0]==1")
            lst_unique = lst_unique[0]
            if debug: print(f"np_unique_nan: final after: lst_unique: {lst_unique}")
            if (type(lst_unique) == np.ndarray) and (lst_unique.shape[0]==1):  # двойная вложенность
                if debug: print('np_unique_nan: final: ', 'one more', "lst_unique.shape[0]==1")
                lst_unique = lst_unique[0]
        elif lst_unique.shape[0]==0: lst_unique = None
    if debug: print(f"np_unique_nan: return: lst_unique: {lst_unique}")
    if debug: print(f"np_unique_nan: return: type(lst_unique): {type(lst_unique)}")
    return lst_unique

import re, regex
def search_product_options_by_str_list(s_lst):
    '''
    sorce: ["Проволка ортопедическая"] or ["Проволка", "ортопедическая"] or ["Спица Киршнера", "1,8", "мм"]
    target: "Проволка костная ортопедическая" or в описании продукта (варианты исполнения) 
         --- Спица Киршнера с троакарным концом. ...., - диаметр 1,6 мм, длина 150-310 мм, - диаметр 1,8 мм, длина 150-310 мм,
    '''
    global df_mi_org_gos
    code_gos, name_gos, product_options_gos, code_national, name_national = None, None, None, None, None
    if (type(s_lst)==list) or (type(s_lst)==np.ndarray) and (len(s_lst)>0) and ((type(s_lst[0])==str) or (type(s_lst[0])==np.str_)):
        query_str_01, query_str_02 = '', ''
        # if len(s_lst)>1:
        for i, el in enumerate(s_lst):
            if len(query_str_01) > 0:
                # query_str_01 += f" & product_options.str.contains('{s_lst[i]}', case=False)" 
                query_str_01 += f" & product_options.str.contains('{s_lst[i]}', regex=True, flags=re.I)" 
            else: 
                # query_str_01 += f"product_options.notnull() & product_options.str.contains('{s_lst[i]}', case=False)"
                # query_str_01 += f"product_options.notnull() & product_options.str.contains('{s_lst[i]}', regex=True, flags=re.I)"
                query_str_01 += f"product_options.notnull() & product_options.str.contains('{s_lst[i]}', regex=True, flags=@re.I)"
        # else:
        #     query_str_01 = "product_options == @s_lst[0]"
        lst_01 = df_mi_org_gos.query(query_str_01, engine='python')[['kind', 'name_clean', 'product_options']].values # , 'product_options'
        if len(lst_01) >0:
            code_gos = np_unique_nan(lst_01[:,0])
            name_gos = np_unique_nan(lst_01[:,1])
            product_options_gos = lst_01[:2]
        return code_gos, name_gos, product_options_gos
        
        
    else:
        return code_gos, name_gos, product_options_gos    

def upload_check_dictionaries(supp_dict_dir, data_links):
    upload_files_for_fuzzy_search(supp_dict_dir, links = data_links)
    df_mi_org_gos, df_mi_national, df_mi_org_gos_prod_options, dict_embedding_gos_multy, dict_embedding_gos_prod_options_multy, dict_embedding_national_multy = load_check_dictionaries_for_fuzzy_search( supp_dict_dir,
      fn_df_mi_national = data_links['df_mi_national']['fn'],
      fn_df_mi_org_gos = data_links['df_mi_org_gos']['fn'],
      fn_df_mi_org_gos_prod_options = data_links['df_mi_org_gos_prod_options']['fn'],
      fn_dict_embedding_gos_multy = data_links['dict_embedding_gos_multy']['fn'],
      fn_dict_embedding_gos_prod_options_multy = data_links['dict_embedding_gos_prod_options_multy']['fn'],
      fn_dict_embedding_national_multy = data_links['dict_embedding_national_multy']['fn'],
    )
    return df_mi_org_gos, df_mi_national, df_mi_org_gos_prod_options,\
    dict_embedding_gos_multy, dict_embedding_gos_prod_options_multy, dict_embedding_national_multy        
