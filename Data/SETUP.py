import datetime
import json


def main():
    lista1 = ["1001", "1002", "1003", "1004", "1005", "1006", "1007", "1008", "1009", "1010", "1011", "1012", "1013",
              "1014", "1015", "1016",
              "1017", "1018", "1019", "1020", "1021", "1022", "1023", "1024", "1025", "1026", "1027", "1028", "1029",
              "1030", "1031", "1032",
              "1033", "1034", "1035", "1036", "1037", "1038", "1039", "1040", "1041", "1042", "1043", "1044", "1045",
              "1046", "1047", "1048",
              "1049", "1050", "1051", "1052", "1053", "1054", "1055", "1056", "1057", "1058", "1059", "1060", "1061",
              "1062", "1063", "1064",
              "1065", "1066", "1067", "1068", "1069", "1070"]

    lista2 = ['ivajok', 'maremil', 'majaman', 'maremil', 'maremil', 'maremil', 'majaman', 'ivajok',
              'maremil', 'majaman', 'maremil', 'ivajok', 'maremil', 'majaman', 'maremil', 'majaman',
              'majaman', 'maremil', 'ivajok', 'maremil', 'ivajok', 'majaman', 'maremil', 'maremil', 'ivajok',
              'maremil', 'majaman', 'maremil', 'ivajok', 'ivajok', 'maremil', 'maremil', 'majaman', 'majaman',
              'majaman', 'majaman', 'majaman', 'majaman', 'maremil', 'ivajok', 'majaman', 'maremil', 'maremil',
              'ivajok', 'majaman', 'ivajok', 'ivajok', 'maremil', 'maremil', 'majaman', 'majaman', 'ivajok',
              'ivajok', 'ivajok', 'maremil', 'ivajok', 'ivajok', 'majaman', 'majaman', 'ivajok', 'maremil',
              'maremil', 'ivajok', 'ivajok', 'maremil', 'maremil', 'ivajok', 'maremil', 'ivajok', 'majaman']

    lista3 = ['MAN01', 'TRL01', 'MAN01', 'TRL02', 'MAS01', 'MAS02', 'PED01', 'MAN02',
              'MAS01', 'TRL02', 'TRL01', 'MAN02', 'MAS01', 'PED02', 'TRL01', 'PED01',
              'MAN02', 'MAS01', 'MAN01', 'TRL01', 'MAN02', 'PED01', 'MAS02', 'MAS02', 'MAN01',
              'TRL01', 'PED01', 'TRL01', 'MAN02', 'MAN01', 'MAS01', 'TRL01', 'TRL01', 'TRL02',
              'MAN01', 'TRL01', 'PED01', 'TRL02', 'MAS01', 'MAN02', 'PED01', 'MAS02', 'MAS02',
              'MAN02', 'PED01', 'MAN01', 'MAN02', 'MAS01', 'TRL02', 'PED01', 'MAN02', 'MAN01',
              'MAN01', 'MAN01', 'MAS01', 'MAN02', 'MAN01', 'PED02', 'PED01', 'MAN02', 'TRL02',
              'TRL01', 'MAN01', 'MAN02', 'MAS02', 'TRL01', 'MAN02', 'TRL02', 'MAN02', 'PED02']

    lista4 = ['jagbis', 'viksav', 'jagbis', 'maremar', 'viksav', 'jagbis', 'sandratom', 'jagbis', 'ivanmart',
              'bobamic', 'jokastan', 'viksav', 'bobamic', 'dragotor', 'viksav', 'maremar', 'maremar', 'sandratom',
              'bobamic', 'djorcvar', 'ivanmart', 'sandratom', 'maremar', 'viksav', 'andramil', 'viksav', 'djorcvar',
              'jagbis', 'andramil', 'jokastan', 'andramil', 'djorcvar', 'dragotor', 'djorcvar', 'jokastan', 'sandratom',
              'andramil', 'maremar', 'djorcvar', 'djorcvar', 'jagbis', 'djorcvar', 'djorcvar', 'djorcvar', 'jokastan',
              'ivanmart', 'dragotor', 'maremar', 'ivanmart', 'dragotor', 'viksav', 'jagbis', 'jokastan', 'viksav',
              'dragotor', 'sandratom', 'dragotor', 'djorcvar', 'ivanmart', 'sandratom', 'djorcvar', 'jokastan',
              'sandratom', 'viksav', 'jokastan', 'dragotor', 'dragotor', 'djorcvar', 'djorcvar', 'maremar']
    lista5 = ['9:00', '10:00', '12:00', '15:00', '16:00', '13:00', '10:00', '10:00',
              '12:00', '13:00', '16:00', '14:00', '9:00', '10:00', '10:00', '9:00',
              '9:00', '9:00', '9:00', '10:00', '12:00', '12:00', '13:00', '14:00', '16:00',
              '15:00', '10:00', '9:00', '10:00', '12:00', '12:00', '13:00', '12:00', '13:00',
              '14:00', '15:00', '10:00', '9:00', '10:00', '10:00', '12:00', '12:00', '13:00',
              '14:00', '15:00', '16:00', '9:00', '9:00', '10:00', '10:00', '16:00', '12:00',
              '13:00', '14:00', '15:00', '15:00', '10:00', '9:00', '12:00', '13:00', '13:00',
              '9:00', '10:00', '10:00', '9:00', '12:00', '15:00', '9:00', '9:00', '9:00']
    lista6 = ['9:40', '10:45', '12:40', '15:30', '17:00', '13:45', '10:30', '11:00',
              '13:00', '13:30', '16:45', '15:00', '10:00', '10:45', '10:45', '9:40',
              '10:00', '10:00', '9:40', '10:45', '13:00', '12:30', '13:45', '14:45', '16:40',
              '15:45', '10:30', '9:45', '11:00', '12:40', '13:00', '13:45', '12:45', '13:30',
              '14:40', '15:45', '10:30', '9:30', '11:00', '11:00', '12:30', '12:45', '13:45',
              '15:00', '15:30', '16:40', '10:00', '10:00', '10:30', '10:30', '17:00', '12:40',
              '13:40', '14:40', '16:00', '16:00', '10:40', '9:45', '12:30', '14:00', '13:30',
              '9:45', '10:40', '11:00', '9:45', '12:45', '16:00', '9:30', '10:00', '9:45']

    datum = datetime.date.today() - datetime.timedelta(5)
    dm5 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() - datetime.timedelta(4)
    dm4 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() - datetime.timedelta(3)
    dm3 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() - datetime.timedelta(2)
    dm2 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() - datetime.timedelta(1)
    dm1 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() + datetime.timedelta(0)
    dm0 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() + datetime.timedelta(1)
    dp1 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() + datetime.timedelta(2)
    dp2 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() + datetime.timedelta(3)
    dp3 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() + datetime.timedelta(4)
    dp4 = datum.strftime('%Y-%m-%d')
    datum = datetime.date.today() + datetime.timedelta(5)
    dp5 = datum.strftime('%Y-%m-%d')
    lista7 = [dm5, dm5, dm5, dm5, dm5, dm5, dm4, dm4,
              dm4, dm4, dm4, dm4, dm4, dm4, dm4, dm4,
              dm3, dm3, dm3, dm3, dm3, dm3, dm3, dm3, dm3,
              dm3, dm2, dm2, dm2, dm2, dm2, dm2, dm2, dm2,
              dm2, dm2, dm1, dm1, dm1, dm1, dm1, dm1, dm1,
              dm1, dm1, dm1, dm0, dm0, dm0, dm0, dm0, dm0,
              dm0, dm0, dm0, dm0, dp1, dp1, dp1, dp1, dp1,
              dp2, dp2, dp3, dp3, dp4, dp4, dp4, dp5, dp5, ]
    lista8 = [['Jagoda', 'Biscic'], ['Viktor', 'Savic'], ['Jagoda', 'Biscic'], ['Marko', 'Maric'], ['Viktor', 'Savic'],
              ['Jagoda', 'Biscic'], ['Aleksandra', 'Tomic'], ['Jagoda', 'Biscic'], ['Ivan', 'Martinovic'],
              ['Sloboda', 'Micalovic'], ['Jovana', 'Stanivukovic'], ['Viktor', 'Savic'], ['Sloboda', 'Micalovic'],
              ['Dragan', 'Torbica'], ['Viktor', 'Savic'], ['Marko', 'Maric'], ['Marko', 'Maric'],
              ['Aleksandra', 'Tomic'],
              ['Sloboda', 'Micalovic'], ['Djordje', 'Cvarkov'], ['Ivan', 'Martinovic'], ['Aleksandra', 'Tomic'],
              ['Marko', 'Maric'],
              ['Viktor', 'Savic'], ['Andrija', 'Milosevic'], ['Viktor', 'Savic'], ['Djordje', 'Cvarkov'],
              ['Jagoda', 'Biscic'],
              ['Andrija', 'Milosevic'], ['Jovana', 'Stanivukovic'], ['Andrija', 'Milosevic'], ['Djordje', 'Cvarkov'],
              ['Dragan', 'Torbica'],
              ['Djordje', 'Cvarkov'], ['Jovana', 'Stanivukovic'], ['Aleksandra', 'Tomic'], ['Andrija', 'Milosevic'],
              ['Marko', 'Maric'],
              ['Djordje', 'Cvarkov'], ['Djordje', 'Cvarkov'], ['Jagoda', 'Biscic'], ['Djordje', 'Cvarkov'],
              ['Djordje', 'Cvarkov'],
              ['Djordje', 'Cvarkov'], ['Jovana', 'Stanivukovic'], ['Ivan', 'Martinovic'], ['Dragan', 'Torbica'],
              ['Marko', 'Maric'],
              ['Ivan', 'Martinovic'], ['Dragan', 'Torbica'], ['Viktor', 'Savic'], ['Jagoda', 'Biscic'],
              ['Jovana', 'Stanivukovic'],
              ['Viktor', 'Savic'], ['Dragan', 'Torbica'], ['Aleksandra', 'Tomic'], ['Dragan', 'Torbica'],
              ['Djordje', 'Cvarkov'],
              ['Ivan', 'Martinovic'], ['Aleksandra', 'Tomic'], ['Djordje', 'Cvarkov'], ['Jovana', 'Stanivukovic'],
              ['Aleksandra', 'Tomic'], ['Viktor', 'Savic'], ['Jovana', 'Stanivukovic'], ['Dragan', 'Torbica'],
              ['Dragan', 'Torbica'], ['Djordje', 'Cvarkov'], ['Djordje', 'Cvarkov'], ['Marko', 'Maric']]
    lista9 = ['suzajov', 'online', 'jovadal', 'online', 'suzajov', 'suzajov', 'suzajov', 'suzajov', 'suzajov', 'online',
              'jovadal', 'jovadal', 'suzajov', 'suzajov', 'jovadal', 'online', 'jovadal', 'suzajov', 'online',
              'jovadal', 'online', 'online', 'online', 'jovadal', 'jovadal', 'jovadal', 'suzajov', 'jovadal', 'suzajov',
              'suzajov', 'suzajov', 'jovadal', 'online', 'suzajov', 'online', 'jovadal', 'suzajov', 'online', 'online',
              'jovadal', 'jovadal', 'suzajov', 'suzajov', 'jovadal', 'online', 'online', 'suzajov', 'suzajov',
              'suzajov', 'jovadal', 'suzajov', 'jovadal', 'jovadal', 'suzajov', 'online', 'online', 'online', 'online',
              'online', 'online', 'online', 'online', 'suzajov', 'online', 'jovadal', 'jovadal', 'jovadal', 'online',
              'online', 'online']
    file = open('testData.txt', 'DefWal')
    for i in range(70):
        file.write(lista1[i]+"|"+lista8[i][0]+" "+
                             lista8[i][1]+ "\n")
    file.close()



if __name__ == "__main__":
    main()
