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
        "file_name": "Atviri_TP_parko_duomenys.csv",
        "table_name": "Atviri_TP_parko_duomenys",
        "delimiter": ",",
        "url": "https://www.regitra.lt/atvduom/Atviri_TP_parko_duomenys.zip",
        "table_schema": [
            {"name": 'MARKE', "data_type": 'STRING'},
            {"name": 'KOMERCINIS_PAV', "data_type": 'STRING'},
            {"name": 'KATEGORIJA_KLASE', "data_type": 'STRING'},
            {"name": 'NUOSAVA_MASE', "data_type": 'FLOAT'},
            {"name": 'DARBINIS_TURIS', "data_type": 'FLOAT'},
            {"name": 'GALIA', "data_type": 'FLOAT'},
            {"name": 'GALIA_ELEKTR', "data_type": 'FLOAT'},
            {"name": 'DEGALAI', "data_type": 'STRING'},
            {"name": 'CO2_KIEKIS', "data_type": 'INTEGER'},
            {"name": 'CO2_KIEKIS_WLTP', "data_type": 'INTEGER'},
            {"name": 'RIDA', "data_type": 'INTEGER'},
            {"name": 'PIRM_REG_DATA', "data_type": 'DATE'},
            {"name": 'PIRM_REG_DATA_LT', "data_type": 'DATE'},
            {"name": 'VALD_TIPAS', "data_type": 'STRING'},
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
    },
    {
        "file_name": "VidausVandenuLaivas.csv",
        "table_name": "inland_vessels_raw",
        "delimiter": ",",
        "url": "https://get.data.gov.lt/datasets/gov/ltsa/vidaus_vandenu_laivai/VidausVandenuLaivas/:format/csv",        
        "table_schema": [
            {"name": 'own_le', "data_type": 'STRING'},
            {"name": 'shipyard_location', "data_type": 'STRING'},
            {"name": 'ship_constructed', "data_type": 'TIMESTAMP'},
            {"name": 'ship_freeboard', "data_type": 'FLOAT'},
            {"name": 'ship_height', "data_type": 'FLOAT'},
            {"name": 'ship_hull_material', "data_type": 'STRING'},
            {"name": 'ship_length', "data_type": 'FLOAT'},
            {"name": 'ship_max_drght', "data_type": 'FLOAT'},
            {"name": 'ship_net_tg', "data_type": 'FLOAT'},
            {"name": 'ship_max_psg', "data_type": 'STRING'},
            {"name": 'ship_brand', "data_type": 'STRING'},
            {"name": 'ship_type', "data_type": 'STRING'},
            {"name": 'ship_use', "data_type": 'STRING'},
            {"name": 'shipyard_title', "data_type": 'STRING'},
            {"name": 'ship_id', "data_type": 'STRING'},
            {"name": 'ship_width', "data_type": 'FLOAT'},
        ]
    },
        {
        "file_name": "JuruLaivas.csv",
        "table_name": "marine_vessels_raw",
        "delimiter": ",",
        "url": "https://get.data.gov.lt/datasets/gov/ltsa/juru_laivai/JuruLaivas/:format/csv",        
        "table_schema": [
            {"_id": 'STRING'},
            {"name": 'own_le', "data_type": 'STRING'},
            {"name": 'ship_constructed', "data_type": 'TIMESTAMP'},
            {"name": 'ship_dw', "data_type": 'FLOAT'},
            {"name": 'ship_gross_tg', "data_type": 'FLOAT'},
            {"name": 'ship_length', "data_type": 'FLOAT'},
            {"name": 'ship_max_drght', "data_type": 'FLOAT'},
            {"name": 'ship_name', "data_type": 'STRING'},
            {"name": 'ship_net_tg', "data_type": 'FLOAT'},
            {"name": 'ship_side_hght', "data_type": 'FLOAT'},
            {"name": 'ship_width', "data_type": 'FLOAT'},
            {"name": 'ship_type', "data_type": 'STRING'}
        ]
    }
]