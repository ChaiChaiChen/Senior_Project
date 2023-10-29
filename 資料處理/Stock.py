import Get_Data as gd


path = 'D:\\資料庫\\台股\\'

# season = ['2019Q1', '2019Q2', '2019Q3', '2019Q4'
#         , '2020Q1', '2020Q2', '2020Q3', '2020Q4']

season = ['2019Q1', '2019Q2', '2019Q3', '2019Q4'
        , '2020Q1', '2020Q2', '2020Q3', '2020Q4'
        , '2021Q1', '2021Q2', '2021Q3', '2021Q4']

group_name =  "造紙工業"     
# group_name = "金融保險業"  
# 6790
lenum = len(season)

print(group_name + "\n")

for s in season:
    for num in range(lenum):
        if s == season[num]:

            print(s)
            print("-----------------")

            slist = gd.get_stocknumber()

            cnt = len(slist)
            
            for i in range(cnt):

                number,name,group = slist[i]
                
                if group == group_name:
                    try:
                        gd.get_data(s, path, number, group)
                        gd.combine_all_data(s, path, number, name, group)
                        gd.csv_data(s, num, season, path, number, group)
                        gd.get_calculate(s, path, number, group)
                        # print(number + "\t Success")

                    except: 
                        pass
                    # print(number + "\t Fail")
            print("======================")


for i in range(cnt):

    number,name,group = slist[i]

    if group == group_name:
        try:
            # gd.calculate(s, num, season, path, number, group)
            gd.get_calculate(s, path, number, group)
            # gd.calculate_4(s, path, number, group)

            # print(number + "\t Success")

        except:
            pass
            # print(number + "\t Fail")
print("======================")

for i in range(cnt):

    number,name,group = slist[i]

    if group == group_name:
        try:
            gd.csv_db(s, path, number, group) # 有問題

            print(number + "\t Success")

        except:
            pass
print("======================")
