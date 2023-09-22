from datetime import date
current_year = date.today().year

file_configurations = [
    {
        "file_name": "Atviri_JTP_parko_duomenys.csv",
        "table_name": "Atviri_JTP_parko_duomenys",
        "delimiter": ",",
        "url": "https://www.regitra.lt/atvduom/Atviri_JTP_parko_duomenys.zip",
        "table_schema": [
            {"name": 'KODAS', "data_type": 'STRING'},
            {"name": 'MARKE', "data_type": 'STRING'},
            {"name": 'KOMERCINIS_PAV', "data_type": 'STRING'},
            {"name": 'KATEGORIJA_KLASE', "data_type": 'STRING'},
            {"name": 'NUOSAVA_MASE', "data_type": 'FLOAT'},
            {"name": 'GALIA', "data_type": 'FLOAT'},
            {"name": 'GALIA_ELEKTR', "data_type": 'FLOAT'},
            {"name": 'DEGALAI', "data_type": 'STRING'},
            {"name": 'CO2_KIEKIS', "data_type": 'INTEGER'},
            {"name": 'CO2_KIEKIS__WLTP', "data_type": 'INTEGER'},
            {"name": 'TERSALU_LYGIS', "data_type": 'STRING'},
            {"name": 'GALIOS_MASES_SANT', "data_type": 'FLOAT'},
            {"name": 'PIRM_REG_DATA', "data_type": 'DATE'},
            {"name": 'PIRM_REG_DATA_LT', "data_type": 'DATE'},
            {"name": 'PAVADINIMAS', "data_type": 'STRING'},
            {"name": 'SAVIVALDYBE', "data_type": 'STRING'},
            {"name": 'APSKRITIS', "data_type": 'STRING'}
        ]
    },
    {
        "file_name": "employees_salaries_raw.csv",
        "table_name": "employees_salaries_raw",
        "delimiter": ";",
        "url": "https://atvira.sodra.lt/imones/downloads/2023/monthly-2023.csv.zip",        
        "table_schema": [
            {"name": 'kodas', "data_type": 'STRING'},
            {"name": 'pavadinimas', "data_type": 'STRING'},
            {"name": 'savivaldybe', "data_type": 'STRING'},
            {"name": 'veiklos_kodas', "data_type": 'STRING'},
            {"name": 'veiklos_pavadinimas', "data_type": 'STRING'},
            {"name": 'vidutinis_darbo_uzmokestis', "data_type": 'FLOAT'},
            {"name": 'apdraustuju_skaicius', "data_type": 'FLOAT'},
            {"name": 'periodas', "data_type": 'DATE'}
        ]
    }
]