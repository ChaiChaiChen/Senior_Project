import os
import sqlite3
import pandas as pd

from cmath import nan
from numpy import int64

from glob import glob
from collections import OrderedDict


def get_stocknumber():

    res = [] # 存放切割文字結果

    try:
        with open('Stock_Number.csv', encoding="utf-8-sig") as F: # 開啟csv檔案
            
            slist = F.readlines()

            for lst in slist: # 走訪每一個股票組合
            
                s = lst.replace("\u3000", ",")
                s = s.split(',')
                
                # s = lst.splitlines(',') # 以逗點切割為陣列
                res.append([s[0].strip(),str(s[1].strip()),str(s[2].strip())]) # .strip去除空白，存到res陣列
    except:
        print('讀不到')

    return res

###################################################

def get_data(s, path, number, group):

    File_Path = (path+"\\html\\"+s+"\\"+number+".html")
    
    result = os.path.isfile(File_Path)
    
    if result == True:
        df = pd.read_html(path+"\\html\\"+s+"\\"+number+".html")#取得放置html的資料夾
        for index_num in range(0,3): #取得陣列中的資料
            file_num = str(index_num + 1)
            # 查詢是否已有資料夾，無資料夾則新增一個
            folder = path + group + "\\" + s + "\\" + number
            if not os.path.isdir(folder):
                os.makedirs(folder)
            df[index_num].to_csv(folder+"\\"+number+"_"+file_num+".csv", index = False, encoding='utf_8-sig')
    else:
        pass

###################################################

def combine_all_data(s, path, number, name, group):

    Path_File = (path + group + "\\" + s + "\\" + number+"\\"+number+"_1.csv")
    
    result = os.path.isfile(Path_File)

    if result == True:

        for i in range(1,4):
            
            #取得csv檔案位置
            file_num  = str(i)

            folder = path + group + "\\" + s + "\\" + number
            if not os.path.isdir(folder):
                os.makedirs(folder)

            df = pd.read_csv(folder+"\\"+number+"_"+file_num+".csv", encoding= "utf_8_sig")
            #取得csv表格列數
            num = (len(df.columns))+1
            #整個轉為list
            df = df.values.tolist()
            #設定一個空陣列，放置列數
            num_list = []
            for i in range(1, num):
                num_list += [i]

            #將陣列從int轉str
            num_list = [str(i) for i in num_list]
            df = pd.DataFrame(columns = num_list, data = df)
            
            #刪除num_list陣列的3、4
            del num_list[1]
            del num_list[1]
            
            #刪除不需要的資料
            df = df.drop(num_list,axis=1)

            #把 Dataframe 轉成 2D numpy array
            data = df.values
            #找到數據的 key
            index1 = list(df.keys())
            #行列互換，再利用map函數將zip內的元組轉列表
            data = list(map(list, zip(*data)))
            data = pd.DataFrame(data, index=index1)

            #存檔
            data.to_csv(folder+"\\"+number+"_getfile_"+file_num+".csv",header=0, encoding= "utf_8_sig")

            ###################################################

            if s == '2019Q4' or s == '2020Q4' or s == '2021Q4':
                
                df = pd.read_csv(folder+"\\"+number+"_1.csv", encoding= "utf_8_sig")
                #取得csv表格列數
                num = (len(df.columns))+1
                #整個轉為list
                df = df.values.tolist()
                #設定一個空陣列，放置列數
                num_list = []
                for i in range(1, num):
                    num_list += [i]

                #將陣列從int轉str
                num_list = [str(i) for i in num_list]
                df = pd.DataFrame(columns = num_list, data = df)

                #刪除num_list陣列的3、4
                del num_list[1:2]
                del num_list[2:4]
                
                #刪除不需要的資料
                df = df.drop(num_list,axis=1)

                #把 Dataframe 轉成 2D numpy array
                data = df.values
                #找到數據的 key
                index1 = list(df.keys())
                #行列互換，再利用map函數將zip內的元組轉列表
                data = list(map(list, zip(*data)))
                data = pd.DataFrame(data, index=index1)

                #存檔
                data.to_csv(folder+"\\"+number+"_getCACL_1.csv",header=0, encoding= "utf_8_sig")

            else:

                df = pd.read_csv(folder+"\\"+number+"_1.csv", encoding= "utf_8_sig")
                #取得csv表格列數
                num = (len(df.columns))+1
                #整個轉為list
                df = df.values.tolist()
                #設定一個空陣列，放置列數
                num_list = []
                for i in range(1, num):
                    num_list += [i]

                #將陣列從int轉str
                num_list = [str(i) for i in num_list]
                df = pd.DataFrame(columns = num_list, data = df)

                #刪除num_list陣列的3、4
                del num_list[1:2]
                del num_list[3:5]
                
                #刪除不需要的資料
                df = df.drop(num_list,axis=1)

                #把 Dataframe 轉成 2D numpy array
                data = df.values
                #找到數據的 key
                index1 = list(df.keys())
                #行列互換，再利用map函數將zip內的元組轉列表
                data = list(map(list, zip(*data)))
                data = pd.DataFrame(data, index=index1)

                #存檔
                data.to_csv(folder+"\\"+number+"_getCACL_1.csv",header=0, encoding= "utf_8_sig")

                ###################################################

            if s == '2019Q2' or s == '2020Q2' or s == '2021Q2' or s == '2019Q3' or s == '2020Q3' or s == '2021Q3':

                df = pd.read_csv(folder+"\\"+number+"_2.csv", encoding= "utf_8_sig")
                #取得csv表格列數
                num = (len(df.columns))+1
                #整個轉為list
                df = df.values.tolist()
                #設定一個空陣列，放置列數
                num_list = []
                for i in range(1, num):
                    num_list += [i]

                #將陣列從int轉str
                num_list = [str(i) for i in num_list]
                df = pd.DataFrame(columns = num_list, data = df)

                #刪除num_list陣列的3、4
                del num_list[1]
                del num_list[3]
                
                #刪除不需要的資料
                df = df.drop(num_list,axis=1)

                #把 Dataframe 轉成 2D numpy array
                data = df.values
                #找到數據的 key
                index1 = list(df.keys())
                #行列互換，再利用map函數將zip內的元組轉列表
                data = list(map(list, zip(*data)))
                data = pd.DataFrame(data, index=index1)

                #存檔
                data.to_csv(folder+"\\"+number+"_getCACL_2.csv",header=0, encoding= "utf_8_sig")

            else:

                df = pd.read_csv(folder+"\\"+number+"_2.csv", encoding= "utf_8_sig")
                #取得csv表格列數
                num = (len(df.columns))+1
                #整個轉為list
                df = df.values.tolist()
                #設定一個空陣列，放置列數
                num_list = []
                for i in range(1, num):
                    num_list += [i]

                #將陣列從int轉str
                num_list = [str(i) for i in num_list]
                df = pd.DataFrame(columns = num_list, data = df)

                #刪除num_list陣列的3、4
                del num_list[1]
                del num_list[2]
                
                #刪除不需要的資料
                df = df.drop(num_list,axis=1)

                #把 Dataframe 轉成 2D numpy array
                data = df.values
                #找到數據的 key
                index1 = list(df.keys())
                #行列互換，再利用map函數將zip內的元組轉列表
                data = list(map(list, zip(*data)))
                data = pd.DataFrame(data, index=index1)

                #存檔
                data.to_csv(folder+"\\"+number+"_getCACL_2.csv",header=0, encoding= "utf_8_sig")
          
        ####################################################    

        files = glob(folder+"\\"+number+"_getCACL_1.csv")

        datalist = ["2", "資產總計　Total assets", "權益總計 Total stockholders' equity", "權益總計 Equity", "權益總計 Total equity", "權益總額 Total equity"]

        for i in range(len(datalist)):

            names = list(pd.read_csv(folder+"\\"+number+"_getCACL_1.csv", nrows=0)) + datalist
            final_list = list(OrderedDict.fromkeys(names))


            file_new = pd.read_csv(folder+"\\"+number+"_getCACL_1.csv", names=final_list, skiprows=1, na_values=['?'])
            file_new.to_csv(folder+"\\"+number+"_getCACL_1.csv", encoding= "utf_8_sig", index = False)
        
        ####################################################

        file2 = glob(folder+"\\"+number+"_getCACL_1.csv")

        df = pd.concat(
            (pd.read_csv(file,header=0, names=final_list, usecols=datalist
                        ,dtype={'name': str, 'tweet':str}) for file in file2), sort=False, ignore_index=True)
        
        df.to_csv(folder+"\\"+number+"_getCACL_1.csv", encoding= "utf_8_sig", index = False)

        ####################################################

        files = glob(folder+"\\"+number+"_getCACL_1.csv")

        df = pd.concat(
                (pd.read_csv(file,header=0, usecols=["資產總計　Total assets", "權益總計 Total stockholders' equity", "權益總計 Equity", "權益總計 Total equity", "權益總額 Total equity"]
                                ,dtype={'name': str, 'tweet': str}) for file in files), sort=False, ignore_index=True).replace( '[(]','-',  regex=True).replace( '[,]','',  regex=True).replace( '[)]','',  regex=True)

        df.columns = ['上季資產總計', '上季權益總計', '上季權益總計', '上季權益總計', '上季權益總計']
        
        df = df.dropna(axis=1)
        df.to_csv(folder+"\\"+number+"_getCACL_1.csv", encoding= "utf_8_sig", index = False)
     
        ####################################################

        files = glob(folder+"\\"+number+"_getCACL_2.csv")

        datalist = ['2', '本期淨利（淨損）Profit (loss)', '本期稅後淨利（淨損）Profit (loss)']

        for i in range(len(datalist)):
        
            names = list(pd.read_csv(folder+"\\"+number+"_getCACL_2.csv", nrows=0)) + datalist
            final_list = list(OrderedDict.fromkeys(names))

            file_new = pd.read_csv(folder+"\\"+number+"_getCACL_2.csv", names=final_list, skiprows=1, na_values=['?'])
            file_new.to_csv(folder+"\\"+number+"_getCACL_2.csv", encoding= "utf_8_sig", index = False)
        
        ####################################################

        file2 = glob(folder+"\\"+number+"_getCACL_2.csv")

        df = pd.concat(
            (pd.read_csv(file,header=0, names=final_list, usecols=datalist
                        ,dtype={'name': str, 'tweet':str}) for file in file2), sort=False, ignore_index=True)
        
        df.to_csv(folder+"\\"+number+"_getCACL_2.csv", encoding= "utf_8_sig", index = False)

        #####################################################

        files = glob(folder+"\\"+number+"_getCACL_2.csv")

        df = pd.concat(
                (pd.read_csv(file,header=0, usecols=['本期淨利（淨損）Profit (loss)', '本期稅後淨利（淨損）Profit (loss)']
                                ,dtype={'name': str, 'tweet': str}) for file in files), sort=False, ignore_index=True).replace( '[(]','-',  regex=True).replace( '[,]','',  regex=True).replace( '[)]','',  regex=True)

        df.columns = ['累季本期淨利', '累季本期淨利']

        df = df.dropna(axis=1)
        df.to_csv(folder+"\\"+number+"_getCACL_2.csv", encoding= "utf_8_sig", index = False)
        
        ####################################################
  
        files = glob(folder+"\\"+number+"_getfile_1.csv")
    

        datalist = ['2', "資產總計　Total assets", "權益總計 Total stockholders' equity", "權益總計 Equity", "權益總計 Total equity", "權益總額 Total equity"]
        
        for i in range(len(datalist)):
        
            names = list(pd.read_csv(folder+"\\"+number+"_getfile_1.csv", nrows=0)) + datalist
            final_list = list(OrderedDict.fromkeys(names))

            file_new = pd.read_csv(folder+"\\"+number+"_getfile_1.csv", names=final_list, skiprows=1, na_values=['?'])
            file_new.to_csv(folder+"\\"+number+"_getfile_1.csv", encoding= "utf_8_sig", index = False)

        ####################################################

        file2 = glob(folder+"\\"+number+"_getfile_1.csv")

        df = pd.concat(
            (pd.read_csv(file,header=0, names=final_list, usecols=datalist
                        ,dtype={'name': str, 'tweet':str}) for file in file2), sort=False, ignore_index=True)
        
        df.columns = ['2', '資產總計　Total assets', "權益總計", "權益總計", "權益總計", "權益總計"]

        df = df.dropna(axis=1)
        df.to_csv(folder+"\\"+number+"_getfile_1.csv", encoding= "utf_8_sig", index = False)
        
        ####################################################

        File1 = pd.read_csv(folder+"\\"+number+"_getCACL_1.csv")
        File2 = pd.read_csv(folder+"\\"+number+"_getfile_1.csv")
        Files = File2.join(File1)
        Files.to_csv(folder+"\\"+number+"_getfile_1.csv", encoding= "utf_8_sig", index = False)

        ####################################################

        files = glob(folder+"\\"+number+"_getfile_2.csv")
        
        datalist = ['2', '本期淨利（淨損）Profit (loss)', '本期稅後淨利（淨損）Profit (loss)']
        
        for i in range(len(datalist)):

            names = list(pd.read_csv(folder+"\\"+number+"_getfile_2.csv", nrows=0)) + datalist
            final_list = list(OrderedDict.fromkeys(names))

            file_new = pd.read_csv(folder+"\\"+number+"_getfile_2.csv", names=final_list, skiprows=1, na_values=['?'])
            file_new.to_csv(folder+"\\"+number+"_getfile_2.csv", encoding= "utf_8_sig", index = False)

        ####################################################

        file2 = glob(folder+"\\"+number+"_getfile_2.csv")

        df = pd.concat(
            (pd.read_csv(file,header=0, names=final_list, usecols=datalist
                        ,dtype={'name': str, 'tweet':str}) for file in file2), sort=False, ignore_index=True)

        df.columns = ['2', '本期淨利', '本期淨利']

        df = df.dropna(axis=1)
        df.to_csv(folder+"\\"+number+"_getfile_2.csv", encoding= "utf_8_sig", index = False)

        ####################################################
   
        File1 = pd.read_csv(folder+"\\"+number+"_getCACL_2.csv")
        File2 = pd.read_csv(folder+"\\"+number+"_getfile_2.csv")
        Files = File2.join(File1)
        Files.to_csv(folder+"\\"+number+"_getfile_2.csv", encoding= "utf_8_sig", index = False)
        
        ####################################################

        files = glob(folder+"\\"+number+"_getfile_3.csv")

        datalist = ['2', '利息費用 Interest expense']

        for i in range(len(datalist)):
                        
            names = list(pd.read_csv(folder+"\\"+number+"_getfile_3.csv", nrows=0)) + datalist
            final_list = list(OrderedDict.fromkeys(names))

            file_new = pd.read_csv(folder+"\\"+number+"_getfile_3.csv", names=final_list, skiprows=1, na_values=['?'])
            file_new.to_csv(folder+"\\"+number+"_getfile_3.csv", encoding= "utf_8_sig", index = False)
        
        ####################################################

        file2 = glob(folder+"\\"+number+"_getfile_3.csv")

        df = pd.concat(
            (pd.read_csv(file,header=0, names=final_list, usecols=datalist
                        ,dtype={'name': str, 'tweet':str}) for file in file2), sort=False, ignore_index=True)
        
        df.to_csv(folder+"\\"+number+"_getfile_3.csv", encoding= "utf_8_sig", index = False)

        ####################################################

        #取得資料夾內所有資料
        files = glob(folder+"\\"+number+"_getfile_*.csv")

        #串列中包含兩個Pandas DataFrame
        df_list = [pd.read_csv(file) for file in files]
        result = pd.merge(df_list[0],df_list[1], on='2')
        result = pd.merge(result,df_list[2],on='2')

        #新增股票代碼
        result["股票代碼"] = number 
        #新增股票名稱
        result["股票名稱"] = name   
        #新增股票分類
        result["類股"] = group
        #新增股票季別
        result["季別"] = s 
        result["判斷季別"] = s 

        # 查詢是否已有資料夾，無資料夾則新增一個
        folder = path + group + "\\" + s + "\\" + number + "\\整合\\"
        if not os.path.isdir(folder):
            os.makedirs(folder)

        result = result.fillna(0)
        result.to_csv(folder+"stock_"+number+".csv", encoding='utf-8-sig', index = False)

    else:
        pass

    ####################################################

def csv_data(s, num, season, path, number, group):

    Path_File = path + group + "\\" + s + "\\" + number
    
    result = os.path.isdir(Path_File)
   
    if result == True:

        # 查詢是否已有資料夾，無資料夾則新增一個
        folder = path + group + "\\" + s + "\\" + number + "\\整合\\"
        if not os.path.isdir(folder):
            os.makedirs(folder)
        
        files = glob(folder+"stock_"+number+".csv")

        df = pd.concat(
                (pd.read_csv(file,header=0, usecols=['資產總計　Total assets'
                                                    ,'權益總計'
                                                    ,'上季資產總計'
                                                    ,'上季權益總計'
                                                    ,'本期淨利'
                                                    ,'累季本期淨利'
                                                    ,'利息費用 Interest expense'
                                                    ,'股票代碼'
                                                    ,'股票名稱'
                                                    ,'類股'
                                                    ,'季別'
                                                    ,'判斷季別'
                                                    ]
                                ,dtype={'name': str, 'tweet': str}) for file in files), sort=False, ignore_index=True).replace( '[(]','-',  regex=True).replace( '[,]','',  regex=True).replace( '[)]','',  regex=True)

        df.columns = ['資產總計', '權益總計', '上季資產總計', '上季權益總計', '本期淨利', '累季本期淨利', '利息費用', '股票代碼', '股票名稱', '類股', '季別', '判斷季別'] 

        # df["年度"] = nan    
        df["年度"] = s[0:4]

        df["ROE"] = nan
        df["ROA"] = nan

        df["近四季ROE"] = nan
        df["近四季ROA"] = nan

        df["年度ROE"] = nan
        df["年度ROA"] = nan

        df.to_csv(folder+"stock_"+number+".csv", encoding="utf_8-sig", index = False)
    
    else:
        pass

    ####################################################

def get_calculate(s, path, number, group):

    # 查詢是否已有資料夾，無資料夾則新增一個
    Path = path + group + "\\" 

    Path_File = Path + s + "\\" + number
    print(Path_File)

    result = os.path.isdir(Path_File)
    print(result)
    if result == True:

        for i in range(0,len(os.listdir(Path))):

            Folder = (os.listdir(Path)[i]) 
            Path_New = Path + Folder + "\\" + number
            Data_File = pd.read_csv(Path_New + "\\整合\\stock_"+number+".csv")

            if Folder == '2020Q1' or Folder == '2021Q1':

                Folder_1 = (os.listdir(Path)[i-1]) 
                Path_New_1 = Path + Folder_1 + "\\" + number
                Data_File_1 = pd.read_csv(Path_New_1 + "\\整合\\stock_"+number+".csv")


                # 第一季
                Folder1 = (os.listdir(Path)[i]) 
                Path_New1 = Path + Folder1 + "\\" + number
                Data_File1 = pd.read_csv(Path_New1 + "\\整合\\stock_"+number+".csv")
    

                # 資料區
                Income_Net1 = Data_File1.本期淨利.astype(int64)

                Total_Assets1 = Data_File1.資產總計.astype(int64) 
                Total_Assets_1 = Data_File_1.資產總計.astype(int64) 

                Total_Equity1 = Data_File1.權益總計.astype(int64) 
                Total_Equity_1 = Data_File_1.權益總計.astype(int64) 

                # 計算區
                Total_Equity = ((Total_Equity1 + Total_Equity_1) / 2)
                Assets_Result = ((Total_Assets1 + Total_Assets_1) / 2)
                
                Result_ROE = Income_Net1 / Total_Equity
                Result_ROA = Income_Net1 / Assets_Result

                # 儲存區
                Data_File['ROE'] = Result_ROE
                Data_File["ROE"] = round(Data_File["ROE"]*100,2)

                Data_File['ROA'] = Result_ROA
                Data_File["ROA"] = round(Data_File["ROA"]*100,2)

                Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)

                #################################################### 

            if Folder == '2019Q2' or Folder == '2020Q2' or Folder == '2021Q2':

                # 第一季
                Folder1 = (os.listdir(Path)[i-1]) 
                Path_New1 = Path + Folder1 + "\\" + number
                Data_File1 = pd.read_csv(Path_New1 + "\\整合\\stock_"+number+".csv")
    

                # 第二季
                Folder2 = (os.listdir(Path)[i]) 
                Path_New2 = Path + Folder2 + "\\" + number
                Data_File2 = pd.read_csv(Path_New2 + "\\整合\\stock_"+number+".csv")


                # 資料區
                Income_Net2 = Data_File2.本期淨利.astype(int64)

                Total_Assets1 = Data_File1.資產總計.astype(int64) 
                Total_Assets2 = Data_File2.資產總計.astype(int64) 

                Total_Equity1 = Data_File1.權益總計.astype(int64) 
                Total_Equity2 = Data_File2.權益總計.astype(int64) 

                Total_Equity = ((Total_Equity1 + Total_Equity2) / 2)
                Assets_Result = ((Total_Assets1 + Total_Assets2) / 2)

                # 計算區
                Result_ROE = Income_Net2 / Total_Equity
                Result_ROA = Income_Net2 / Assets_Result


                # 儲存區
                Data_File['ROE'] = Result_ROE
                Data_File["ROE"] = round(Data_File["ROE"]*100,2)

                Data_File['ROA'] = Result_ROA
                Data_File["ROA"] = round(Data_File["ROA"]*100,2)

                Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)

                #################################################### 

            if Folder == '2019Q3' or Folder == '2020Q3' or Folder == '2021Q3':

                # 第二季
                Folder2 = (os.listdir(Path)[i-1]) 
                Path_New2 = Path + Folder2 + "\\" + number
                Data_File2 = pd.read_csv(Path_New2 + "\\整合\\stock_"+number+".csv")

                # 第三季
                Folder3 = (os.listdir(Path)[i]) 
                Path_New3 = Path + Folder3 + "\\" + number
                Data_File3 = pd.read_csv(Path_New3 + "\\整合\\stock_"+number+".csv")

    
                # 資料區
                Income_Net3 = Data_File3.本期淨利.astype(int64)

                Total_Assets2 = Data_File2.資產總計.astype(int64)
                Total_Assets3 = Data_File3.資產總計.astype(int64)
                
                Total_Equity2 = Data_File2.權益總計.astype(int64) 
                Total_Equity3 = Data_File3.權益總計.astype(int64) 


                Total_Equity = ((Total_Equity2 + Total_Equity3) / 2)
                Assets_Result = ((Total_Assets2 + Total_Assets3) / 2)

                # 計算區
                Result_ROE = Income_Net3 / Total_Equity
                Result_ROA = Income_Net3 / Assets_Result

                # 儲存區
                Data_File['ROE'] = Result_ROE
                Data_File["ROE"] = round(Data_File["ROE"]*100,2)

                Data_File['ROA'] = Result_ROA
                Data_File["ROA"] = round(Data_File["ROA"]*100,2)

                Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)

                #################################################### 

            if Folder == '2019Q4' or Folder == '2020Q4' or Folder == '2021Q4':
                
                # 第一季
                Folder1 = (os.listdir(Path)[i-3]) 
                Path_New1 = Path + Folder1 + "\\" + number
                Data_File1 = pd.read_csv(Path_New1 + "\\整合\\stock_"+number+".csv")

                Income_Net1 = Data_File1.本期淨利.astype(int64)


                # 第二季
                Folder2 = (os.listdir(Path)[i-2]) 
                Path_New2 = Path + Folder2 + "\\" + number
                Data_File2 = pd.read_csv(Path_New2 + "\\整合\\stock_"+number+".csv")

                Income_Net2 = Data_File2.本期淨利.astype(int64)
                

                # 第三季
                Folder3 = (os.listdir(Path)[i-1]) 
                Path_New3 = Path + Folder3 + "\\" + number
                Data_File3 = pd.read_csv(Path_New3 + "\\整合\\stock_"+number+".csv")

                Income_Net3 = Data_File3.本期淨利.astype(int64)
                Total_Assets3 = Data_File3.資產總計.astype(int64)

                # 資料區
                Income_Net4 = Data_File.本期淨利.astype(int64)
                Total_Assets4 = Data_File.資產總計.astype(int64)
                Total_Equity3 = Data_File3.權益總計.astype(int64) 
                Total_Equity4 = Data_File.權益總計.astype(int64) 

                Total_Equity = ((Total_Equity3 + Total_Equity4) / 2)
                Assets_Result = ((Total_Assets4 + Total_Assets3) / 2)

                # 總計算
                Income_Result = (Income_Net4 - Income_Net1 - Income_Net2 - Income_Net3)

                # 計算區
                Result_ROE = Income_Result / Total_Equity
                Result_ROA = Income_Result / Assets_Result
                
                # 儲存區
                Data_File['ROE'] = Result_ROE
                Data_File["ROE"] = round(Data_File["ROE"]*100,2)

                Data_File['ROA'] = Result_ROA
                Data_File["ROA"] = round(Data_File["ROA"]*100,2)

                Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)

    else:
        Data_File['ROE'] = nan
        Data_File['ROA'] = nan
        Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)
    # else:
    #     pass       
    ####################################################
def calculate_4(s, path, number, group):
    
    Path = path + group + "\\" 

    Path_File = Path + s + "\\" + number
    
    result = os.path.isdir(Path_File)

    if result == True:

        for i in range(0,len(os.listdir(Path))):

            Folder = (os.listdir(Path)[i]) 
            Path_New = Path + Folder + "\\" + number
            Data_File = pd.read_csv(Path_New + "\\整合\\stock_"+number+".csv")


            if Folder == '2020Q1' or Folder == '2021Q1':

                Folder2 = (os.listdir(Path)[i-1]) 
                Path_New2 = Path + Folder2 + "\\" + number
                Data_File2 = pd.read_csv(Path_New2 + "\\整合\\stock_"+number+".csv")

                Income_Net2 = Data_File2.本期淨利.astype(int64)
                

                Folder4 = (os.listdir(Path)[i-4]) 
                Path_New4 = Path + Folder4 + "\\" + number
                Data_File4 = pd.read_csv(Path_New4 + "\\整合\\stock_"+number+".csv")

                Income_Net4 = Data_File4.本期淨利.astype(int64)

                Income_New = Income_Net2 - Income_Net4

                # 資料區
                Income_Net = Data_File.本期淨利.astype(int64)

                Total_Assets4 = Data_File.資產總計.astype(int64)
                LST_Assets4 = Data_File.上季資產總計.astype(int64)

                Total_Equity4 = Data_File.權益總計.astype(int64) 
                LST_Equity4 = Data_File.上季權益總計.astype(int64) 

                Income_Result = Income_Net + Income_New
                
                LSTE = ((Total_Equity4 + LST_Equity4) / 2)
                LSTA = ((Total_Assets4 + LST_Assets4) / 2)

                ROE_4 = Income_Result / LSTE
                ROA_4 = Income_Result / LSTA

                Data_File['近四季ROE'] = ROE_4
                Data_File["近四季ROE"] = round(Data_File["近四季ROE"]*100,2)

                Data_File['近四季ROA'] = ROA_4
                Data_File["近四季ROA"] = round(Data_File["近四季ROA"]*100,2)

            Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)

            ####################################################

            if Folder == '2020Q2' or Folder == '2021Q2':

                Folder2 = (os.listdir(Path)[i-2]) 
                Path_New2 = Path + Folder2 + "\\" + number
                Data_File2 = pd.read_csv(Path_New2 + "\\整合\\stock_"+number+".csv")

                Income_Net2 = Data_File2.本期淨利.astype(int64)
                

                Folder4 = (os.listdir(Path)[i-4]) 
                Path_New4 = Path + Folder4 + "\\" + number
                Data_File4 = pd.read_csv(Path_New4 + "\\整合\\stock_"+number+".csv")

                Income_Net4 = Data_File4.累季本期淨利.astype(int64)

                Income_New = Income_Net2 - Income_Net4

                
                # 資料區
                Income_Net = Data_File.累季本期淨利.astype(int64)

                Total_Assets4 = Data_File.資產總計.astype(int64)
                LST_Assets4 = Data_File.上季資產總計.astype(int64)  

                Total_Equity4 = Data_File.權益總計.astype(int64) 
                LST_Equity4 = Data_File.上季權益總計.astype(int64) 

                Income_Result = Income_Net + Income_New
                
                LSTE = ((Total_Equity4 + LST_Equity4) / 2)
                LSTA = ((Total_Assets4 + LST_Assets4) / 2)

                ROE_4 = Income_Result / LSTE
                ROA_4 = Income_Result / LSTA

                Data_File['近四季ROE'] = ROE_4
                Data_File["近四季ROE"] = round(Data_File["近四季ROE"]*100,2)

                Data_File['近四季ROA'] = ROA_4
                Data_File["近四季ROA"] = round(Data_File["近四季ROA"]*100,2)

            Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)

            ####################################################

            if Folder == '2020Q3' or Folder == '2021Q3':

                Folder3 = (os.listdir(Path)[i-3]) 
                Path_New3 = Path + Folder3 + "\\" + number
                Data_File3 = pd.read_csv(Path_New3 + "\\整合\\stock_"+number+".csv")

                Income_Net3 = Data_File3.本期淨利.astype(int64)


                Folder4 = (os.listdir(Path)[i-4]) 
                Path_New4 = Path + Folder4 + "\\" + number
                Data_File4 = pd.read_csv(Path_New4 + "\\整合\\stock_"+number+".csv")

                Income_Net4 = Data_File4.累季本期淨利.astype(int64)

                Income_New = Income_Net3 - Income_Net4
                
                # 資料區
                Income_Net = Data_File.累季本期淨利.astype(int64)

                Total_Assets4 = Data_File.資產總計.astype(int64)
                LST_Assets4 = Data_File.上季資產總計.astype(int64)  

                Total_Equity4 = Data_File.權益總計.astype(int64) 
                LST_Equity4 = Data_File.上季權益總計.astype(int64) 

                Income_Result = Income_Net + Income_New

                LSTE = ((Total_Equity4 + LST_Equity4) / 2)
                LSTA = ((Total_Assets4 + LST_Assets4) / 2)

                ROE_4 = Income_Result / LSTE
                ROA_4 = Income_Result / LSTA

                Data_File['近四季ROE'] = ROE_4
                Data_File["近四季ROE"] = round(Data_File["近四季ROE"]*100,2)

                Data_File['近四季ROA'] = ROA_4
                Data_File["近四季ROA"] = round(Data_File["近四季ROA"]*100,2)

            Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)
                
            ####################################################

            if Folder == '2019Q4' or Folder == '2020Q4' or Folder == '2021Q4':

                # 資料區
                Income_Net4 = Data_File.本期淨利.astype(int64)
                
                Total_Assets4 = Data_File.資產總計.astype(int64)
                LST_Assets4 = Data_File.上季資產總計.astype(int64)

                Total_Equity4 = Data_File.權益總計.astype(int64) 
                LST_Equity4 = Data_File.上季權益總計.astype(int64) 

                LSTE = ((Total_Equity4 + LST_Equity4) / 2)
                LSTA = ((Total_Assets4 + LST_Assets4) / 2)

                ROE_4 = Income_Net4 / LSTE
                ROA_4 = Income_Net4 / LSTA

                Data_File['近四季ROE'] = ROE_4
                Data_File["近四季ROE"] = round(Data_File["近四季ROE"]*100,2)

                Data_File['年度ROE'] = ROE_4
                Data_File["年度ROE"] = round(Data_File["年度ROE"]*100,2)

                Data_File['近四季ROA'] = ROA_4
                Data_File["近四季ROA"] = round(Data_File["近四季ROA"]*100,2)

                Data_File['年度ROA'] = ROA_4
                Data_File["年度ROA"] = round(Data_File["年度ROA"]*100,2)

            Data_File.to_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf_8-sig", index = False)
        
        else:
            pass    
    ####################################################

def csv_db(s, path, number, group): 

    # 查詢是否已有資料夾，無資料夾則新增一個
    Path = path + group 

    for i in range(0,len(os.listdir(Path))):

        Folder = (os.listdir(Path)[i]) 
        Path_New = Path  + "\\" + Folder + "\\" + number

        File_Path = (Path_New + "\\整合\\stock_"+number+".csv")

        result = os.path.isfile(File_Path)

        if result == True:

            Path_DB = path + "DataBase\\"
            if not os.path.isdir(Path_DB):
                os.makedirs(Path_DB)

            # Path_DB = path + group  + "\\DataBase\\"
            # if not os.path.isdir(Path_DB):
            #     os.makedirs(Path_DB)
        
        
            conn = sqlite3.connect(path + "DataBase\\Stock.db")
            # conn = sqlite3.connect(Path_DB + "stock.db")

            df = pd.read_csv(Path_New + "\\整合\\stock_"+number+".csv", encoding="utf-8-sig") 
            df.to_sql("stock", conn, if_exists="append", index=False)
            
        else:
            pass

    ####################################################
